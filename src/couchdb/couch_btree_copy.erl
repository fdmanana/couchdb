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

-module(couch_btree_copy).

-export([copy/3]).

-include("couch_db.hrl").

-define(CHUNK_THRESHOLD, 16#4ff).

-record(acc, {
    btree,
    fd,
    before_kv_write = fun(Item) -> Item end,
    compression = ?DEFAULT_COMPRESSION,
    nodes = dict:from_list([{1, []}]),
    cur_level = 1,
    max_level = 1
}).


copy(Btree, Fd, Options) ->
    case lists:member(override, Options) of
    true ->
        ok = couch_file:truncate(Fd, 0);
    false ->
        ok
    end,
    Acc0 = apply_options(Options, #acc{btree = Btree, fd = Fd}),
    {ok, _, #acc{cur_level = 1} = FinalAcc} = couch_btree:fold(
        Btree, fun fold_copy/3, Acc0, []),
    {ok, CopyRootState} = finish_copy(FinalAcc),
    ok = couch_file:sync(Fd),
    {ok, CopyRootState}.


apply_options([], Acc) ->
    Acc;
apply_options([{before_kv_write, Fun} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{before_kv_write = Fun});
apply_options([override | Rest], Acc) ->
    apply_options(Rest, Acc);
apply_options([{compression, Comp} | Rest], Acc) ->
    apply_options(Rest, Acc#acc{compression = Comp}).


extract(#acc{btree = #btree{extract_kv = Extract}}, Value) ->
    Extract(Value).


assemble(#acc{btree = #btree{assemble_kv = Assemble}}, Key, Value) ->
    Assemble(Key, Value).


before_leaf_write(#acc{before_kv_write = Fun} = Acc, KVs) ->
    lists:map(
        fun({K, V}) ->
            Item = assemble(Acc, K, V),
            NewItem = Fun(Item),
            {K, _NewValue} = extract(Acc, NewItem)
        end,
        KVs).


write_leaf(#acc{fd = Fd, compression = Comp}, Node, Red) ->
    {ok, Pos, Size} = couch_file:append_term(Fd, Node, [{compression, Comp}]),
    {ok, {Pos, Red, Size}}.


write_kp_node(#acc{fd = Fd, btree = Bt, compression = Comp}, NodeList) ->
    {ChildrenReds, ChildrenSize} = lists:foldr(
        fun({_Key, {_P, Red, Sz}}, {AccR, AccSz}) ->
            {[Red | AccR], Sz + AccSz}
        end,
        {[], 0}, NodeList),
    Red = case Bt#btree.reduce of
    nil -> [];
    _ ->
        couch_btree:final_reduce(Bt, {[], ChildrenReds})
    end,
    {ok, Pos, Size} = couch_file:append_term(
        Fd, {kp_node, NodeList}, [{compression, Comp}]),
    {ok, {Pos, Red, ChildrenSize + Size}}.


fold_copy(Item, _Reds, #acc{nodes = Nodes, cur_level = 1} = Acc) ->
    {K, V} = extract(Acc, Item),
    LevelNode = dict:fetch(1, Nodes),
    LevelNodes2 = [{K, V} | LevelNode],
    NextAcc = case ?term_size(LevelNodes2) >= ?CHUNK_THRESHOLD of
    true ->
        LeafState = flush_leaf(LevelNodes2, Acc),
        bubble_up({K, LeafState}, Acc);
    false ->
        Acc#acc{nodes = dict:store(1, LevelNodes2, Nodes)}
    end,
    {ok, NextAcc}.


bubble_up({Key, NodeState}, #acc{cur_level = Level} = Acc) ->
    bubble_up({Key, NodeState}, Level, Acc).

bubble_up({Key, NodeState}, Level, #acc{max_level = MaxLevel,
                                        nodes = Nodes} = Acc) ->
    Acc2 = Acc#acc{nodes = dict:store(Level, [], Nodes)},
    case Level of
    MaxLevel ->
        Acc2#acc{
            nodes = dict:store(Level + 1, [{Key, NodeState}], Acc2#acc.nodes),
            max_level = Level + 1
        };
    _ when Level < MaxLevel ->
        NextLevelNodes = dict:fetch(Level + 1, Acc2#acc.nodes),
        NextLevelNodes2 = [{Key, NodeState} | NextLevelNodes],
        case ?term_size(NextLevelNodes2) >= ?CHUNK_THRESHOLD of
        true ->
            {ok, NewNodeState} = write_kp_node(
                Acc2, lists:reverse(NextLevelNodes2)),
            bubble_up({Key, NewNodeState}, Level + 1, Acc2);
        false ->
            Acc2#acc{
                nodes = dict:store(Level + 1, NextLevelNodes2, Acc2#acc.nodes)
            }
        end
    end.


finish_copy(#acc{cur_level = 1, max_level = 1, nodes = Nodes} = Acc) ->
    case dict:fetch(1, Nodes) of
    [] ->
        {ok, nil};
    [{_Key, _Value} | _] = KvList ->
        {ok, flush_leaf(KvList, Acc)}
    end;

finish_copy(#acc{cur_level = Level, max_level = Level, nodes = Nodes} = Acc) ->
    case dict:fetch(Level, Nodes) of
    [{_Key, {Pos, Red, Size}}] ->
        {ok, {Pos, Red, Size}};
    NodeList ->
        write_kp_node(Acc, lists:reverse(NodeList))
    end;

finish_copy(#acc{cur_level = Level, nodes = Nodes} = Acc) ->
    case dict:fetch(Level, Nodes) of
    [{LastKey, _} | _] = NodeList ->
        UpperNodeState= case Level of
        1 ->
            flush_leaf(NodeList, Acc);
        _ when Level > 1 ->
            {ok, KpNodeState} = write_kp_node(Acc, lists:reverse(NodeList)),
            KpNodeState
        end,
        ParentNode = dict:fetch(Level + 1, Nodes),
        Acc2 = Acc#acc{
            nodes = dict:store(Level + 1, [{LastKey, UpperNodeState} | ParentNode], Nodes),
            cur_level = Level + 1
        },
        finish_copy(Acc2);
    [] when Level =:= 1 ->
        finish_copy(Acc#acc{cur_level = Level + 1})
    end.


flush_leaf(KVs, #acc{btree = Btree} = Acc) ->
    NewKVs = before_leaf_write(Acc, lists:reverse(KVs)),
    Red = case Btree#btree.reduce of
    nil -> [];
    _ ->
        Items = lists:map(
            fun({K, V}) -> assemble(Acc, K, V) end,
            NewKVs),
        couch_btree:final_reduce(Btree, {Items, []})
    end,
    {ok, LeafState} = write_leaf(Acc, {kv_node, NewKVs}, Red),
    LeafState.
