% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.


% A generic cache optimized for append mode only, that is, it assumes that
% the value for each unique key is never replaced.

-module(couch_cache).
-behaviour(gen_server).

% public API
-export([start_link/1, stop/1]).
-export([get/2, put/3]).
-export([get_stats/1]).

% gen_server callbacks
-export([init/1, handle_call/3, handle_info/2, handle_cast/2]).
-export([code_change/3, terminate/2]).

-include("couch_db.hrl").

-define(DEFAULT_POLICY, lru).
-define(DEFAULT_SIZE, "128Kb"). % bytes

-record(cache, {
    ets,
    pid
}).

-record(state, {
    cache_size,
    free,
    policy,
    items,
    atimes,
    take_fun,
    hits = 0,
    misses = 0
}).

%% @type cache() = #cache{}
%% @type key() = term()
%% @type item() = term()


%% @spec get(#cache{}, key()) -> {ok, item()} | not_found
get(#cache{ets = Ets, pid = Pid}, Key) ->
    case ets:lookup(Ets, Key) of
    [] ->
        ok = gen_server:cast(Pid, cache_miss),
        not_found;
    [{Key, {{bin, Bin}, _}}] ->
        ok = gen_server:cast(Pid, {cache_hit, Key, erlang:now()}),
        {ok, binary_to_term(Bin)};
    [{Key, {Bin, _}}] ->
        ok = gen_server:cast(Pid, {cache_hit, Key, erlang:now()}),
        {ok, Bin}
    end.


%% @spec put(cache(), key(), item()) -> ok
put(#cache{pid = Pid}, Key, Item) when is_binary(Item) ->
    ok = gen_server:cast(Pid, {put, Key, Item});
put(#cache{pid = Pid}, Key, Item) ->
    ok = gen_server:cast(Pid, {put, Key, {bin, term_to_binary(Item)}}).


%% @spec get_stats(cache()) -> {ok, [ stat() ]}
%% @type stat() = {hits, int()} | {misses, int()} | {size, int()} | {free, int()}
get_stats(#cache{pid = Pid}) ->
    gen_server:call(Pid, get_stats, infinity).


%% @spec start_link(options()) -> {ok, #cache{}}
%% @type options() = [ option() ]
%% @type option() = {policy, policy()} | {size, size()}
%% @type size() = int() | string() | binary()
%% @type policy() = lru | mru
start_link(Options) ->
    {ok, Pid} = gen_server:start_link(?MODULE, Options, []),
    {ok, Ets} = gen_server:call(Pid, get_ets, infinity),
    {ok, #cache{ets = Ets, pid = Pid}}.


%% @spec stop(cache()) -> ok
stop(#cache{pid = Pid}) ->
    catch gen_server:call(Pid, stop),
    ok.


init(Options) ->
    Size = parse_size(couch_util:get_value(size, Options, ?DEFAULT_SIZE)),
    Policy= couch_util:get_value(policy, Options, ?DEFAULT_POLICY),
    State = #state{
        policy = Policy,
        cache_size = Size,
        free = Size,
        items = ets:new(cache_ets, [set, protected]),
        atimes = gb_trees:empty(),
        take_fun = case Policy of
            lru ->
                fun gb_trees:take_smallest/1;
            mru ->
                fun gb_trees:take_largest/1
            end
    },
    {ok, State}.


handle_cast({put, _Key, {bin, Bin}}, #state{cache_size = CacheSize} = State)
    when byte_size(Bin) > CacheSize ->
    {noreply, State};

handle_cast({put, _Key, Bin}, #state{cache_size = CacheSize} = State)
    when is_binary(Bin) andalso byte_size(Bin) > CacheSize ->
    {noreply, State};

handle_cast({put, Key, Item}, #state{items = Items} = State) ->
    case ets:lookup(Items, Key) of
    [{Key, _}] ->
        {noreply, State};
    [] ->
        ItemSize = bin_size(Item),
        #state{
            atimes = ATimes,
            free = Free
        } = State2 = free_until(State, ItemSize),
        Now = erlang:now(),
        ATimes2 = gb_trees:insert(Now, Key, ATimes),
        true = ets:insert(Items, {Key, {Item, Now}}),
        {noreply, State2#state{atimes = ATimes2, free = Free - ItemSize}}
    end;

handle_cast({cache_hit, Key, NewATime},
    #state{items = Items, atimes = ATimes, hits = Hits} = State) ->
    case ets:lookup(Items, Key) of
    [] ->
        {noreply, State#state{hits = Hits + 1}};
    [{Key, {Item, OldATime}}] ->
        case NewATime =< OldATime of
        true ->
            {noreply, State#state{hits = Hits + 1}};
        false ->
            ATimes2 = gb_trees:insert(
                NewATime, Key, gb_trees:delete(OldATime, ATimes)),
            true = ets:insert(Items, {Key, {Item, NewATime}}),
            {noreply, State#state{atimes = ATimes2, hits = Hits + 1}}
        end
    end;

handle_cast(cache_miss, #state{misses = Misses} = State) ->
    {noreply, State#state{misses = Misses + 1}}.


handle_call(get_ets, _From, #state{items = Items} = State) ->
   {reply, {ok, Items}, State};

handle_call(get_stats, _From, #state{cache_size = Size, free = Free,
   hits = Hits, misses = Misses} = State) ->
   Stats = [{size, Size}, {free, Free}, {hits, Hits}, {misses, Misses}],
   {reply, {ok, Stats}, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.


handle_info(Msg, State) ->
    {stop, {unexpected_msg, Msg}, State}.


terminate(_Reason, #state{items = Items}) ->
    true = ets:delete(Items).


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


free_until(#state{free = Free} = State, MinFreeSize) when Free >= MinFreeSize ->
    State;
free_until(State, MinFreeSize) ->
    State2 = free_cache_entry(State),
    free_until(State2, MinFreeSize).


free_cache_entry(#state{take_fun = TakeFun, atimes = ATimes,
    items = Items, free = Free} = State) ->
    {ATime, Key, ATimes2} = TakeFun(ATimes),
    [{Key, {Item, ATime}}] = ets:lookup(Items, Key),
    true = ets:delete(Items, Key),
    State#state{atimes = ATimes2, free = Free + bin_size(Item)}.



% helper functions

bin_size({bin, Bin}) ->
    byte_size(Bin);
bin_size(Bin) ->
    byte_size(Bin).


parse_size(Size) when is_integer(Size) ->
    Size;
parse_size(Size) ->
    {match, [Value1, Suffix]} = re:run(
        Size,
        [$^, "\\s*", "(\\d+)", "\\s*", "(\\w*)", "\\s*", $$],
        [{capture, [1, 2], list}]),
    Value = list_to_integer(Value1),
    case string:to_lower(Suffix) of
    [] ->
        Value;
    "b" ->
        Value;
    "kb" ->
        Value * 1024;
    "mb" ->
        Value * 1024 * 1024;
    "gb" ->
        Value * 1024 * 1024 * 1024
    end.
