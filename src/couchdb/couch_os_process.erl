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

-module(couch_os_process).
-behaviour(gen_server).

-export([start_link/1, start_link/2, start_link/3, stop/1]).
-export([set_timeout/2, prompt/2]).
-export([writeline/2, readline/1, writejson/2, readjson/1]).
-export([send/2, nb_send/2, set_receiver/2]).
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-include("couch_db.hrl").

-define(PORT_OPTIONS, [stream, {line, 4096}, binary, exit_status, hide]).

-record(os_proc,
    {command,
     port,
     writer,
     reader,
     timeout=5000,
     receiver=nil,
     data_acc=[]
    }).

start_link(Command) ->
    start_link(Command, []).
start_link(Command, Options) ->
    start_link(Command, Options, ?PORT_OPTIONS).
start_link(Command, Options, PortOptions) ->
    gen_server:start_link(couch_os_process, [Command, Options, PortOptions], []).

stop(Pid) ->
    gen_server:cast(Pid, stop).

% Read/Write API
set_timeout(Pid, TimeOut) when is_integer(TimeOut) ->
    ok = gen_server:call(Pid, {set_timeout, TimeOut}, infinity).

% Non-blocking send. Used by couch_db_update_notifier.erl
nb_send(Pid, Data) ->
    gen_server:cast(Pid, {send, Data}).

% To be used together with set_receiver/2. The OS process replies to the
% requests sent by the caller of this function are delivered to the process
% defined by set_receiver/2.
send(Pid, Data) ->
    case get({os_proc, Pid}) of
    undefined ->
        #os_proc{writer = Writer} = OsProc =
            gen_server:call(Pid, get_os_proc, infinity),
        put({os_proc, Pid}, OsProc);
    #os_proc{writer = Writer} = OsProc ->
        ok
    end,
    Writer(OsProc, Data).

% To be used together with send/2. Sets the receiver process for the
% responses to the requests sent by the process which invokes send/2.
set_receiver(Pid, ReceiverPid) ->
    ok = gen_server:cast(Pid, {set_receiver, ReceiverPid}).

prompt(Pid, Data) ->
    case gen_server:call(Pid, {prompt, Data}, infinity) of
        {ok, Result} ->
            Result;
        Error ->
            ?LOG_ERROR("OS Process Error ~p :: ~p",[Pid,Error]),
            throw(Error)
    end.

% Utility functions for reading and writing
% in custom functions
writeline(OsProc, Data) when is_record(OsProc, os_proc) ->
    port_command(OsProc#os_proc.port, [Data, $\n]).

readline(#os_proc{} = OsProc) ->
    readline(OsProc, []).
readline(#os_proc{port = Port} = OsProc, Acc) ->
    receive
    {Port, {data, {noeol, Data}}} when is_binary(Acc) ->
        readline(OsProc, <<Acc/binary,Data/binary>>);
    {Port, {data, {noeol, Data}}} when is_binary(Data) ->
        readline(OsProc, Data);
    {Port, {data, {noeol, Data}}} ->
        readline(OsProc, [Data|Acc]);
    {Port, {data, {eol, <<Data/binary>>}}} when is_binary(Acc) ->
        [<<Acc/binary,Data/binary>>];
    {Port, {data, {eol, Data}}} when is_binary(Data) ->
        [Data];
    {Port, {data, {eol, Data}}} ->
        lists:reverse(Acc, Data);
    {Port, Err} ->
        catch port_close(Port),
        throw({os_process_error, Err})
    after OsProc#os_proc.timeout ->
        catch port_close(Port),
        throw({os_process_error, "OS process timed out."})
    end.

% Standard JSON functions
writejson(OsProc, Data) when is_record(OsProc, os_proc) ->
    JsonData = ?JSON_ENCODE(Data),
    ?LOG_DEBUG("OS Process ~p Input  :: ~s", [OsProc#os_proc.port, JsonData]),
    true = writeline(OsProc, JsonData).

readjson(OsProc) when is_record(OsProc, os_proc) ->
    Line = iolist_to_binary(readline(OsProc)),
    ?LOG_DEBUG("OS Process ~p Output :: ~s", [OsProc#os_proc.port, Line]),
    try
        % Don't actually parse the whole JSON. Just try to see if it's
        % a command or a doc map/reduce/filter/show/list/update output.
        % If it's a command then parse the whole JSON and execute the
        % command, otherwise return the raw JSON line to the caller.
        pick_command(Line)
    catch
    throw:abort ->
        {json, Line};
    throw:{cmd, _Cmd} ->
        exec_command(Line, OsProc),
        readjson(OsProc)
    end.

exec_command(Line, OsProc) ->
    case ?JSON_DECODE(Line) of
    [<<"log">>, Msg] when is_binary(Msg) ->
        % we got a message to log. Log it and continue
        ?LOG_INFO("OS Process ~p Log :: ~s", [OsProc#os_proc.port, Msg]);
    [<<"error">>, Id, Reason] ->
        throw({error, {couch_util:to_existing_atom(Id),Reason}});
    [<<"fatal">>, Id, Reason] ->
        ?LOG_INFO("OS Process ~p Fatal Error :: ~s ~p",
            [OsProc#os_proc.port, Id, Reason]),
        throw({couch_util:to_existing_atom(Id),Reason})
    end.

pick_command(Line) ->
    json_stream_parse:events(Line, fun pick_command0/1).

pick_command0(array_start) ->
    fun pick_command1/1;
pick_command0(_) ->
    throw(abort).

pick_command1(<<"log">> = Cmd) ->
    throw({cmd, Cmd});
pick_command1(<<"error">> = Cmd) ->
    throw({cmd, Cmd});
pick_command1(<<"fatal">> = Cmd) ->
    throw({cmd, Cmd});
pick_command1(_) ->
    throw(abort).


% gen_server API
init([Command, Options, PortOptions]) ->
    PrivDir = couch_util:priv_dir(),
    Spawnkiller = filename:join(PrivDir, "couchspawnkillable"),
    BaseProc = #os_proc{
        command=Command,
        port=open_port({spawn, Spawnkiller ++ " " ++ Command}, PortOptions),
        writer=fun writejson/2,
        reader=fun readjson/1
    },
    KillCmd = readline(BaseProc),
    Pid = self(),
    ?LOG_DEBUG("OS Process Start :: ~p", [BaseProc#os_proc.port]),
    spawn(fun() ->
            % this ensure the real os process is killed when this process dies.
            erlang:monitor(process, Pid),
            receive _ -> ok end,
            os:cmd(?b2l(iolist_to_binary(KillCmd)))
        end),
    OsProc =
    lists:foldl(fun(Opt, Proc) ->
        case Opt of
        {writer, Writer} when is_function(Writer) ->
            Proc#os_proc{writer=Writer};
        {reader, Reader} when is_function(Reader) ->
            Proc#os_proc{reader=Reader};
        {timeout, TimeOut} when is_integer(TimeOut) ->
            Proc#os_proc{timeout=TimeOut}
        end
    end, BaseProc, Options),
    {ok, OsProc}.

terminate(_Reason, #os_proc{port=Port}) ->
    catch port_close(Port),
    ok.

handle_call(get_os_proc, _From, OsProc) ->
    {reply, OsProc, OsProc};
handle_call({set_timeout, TimeOut}, _From, OsProc) ->
    {reply, ok, OsProc#os_proc{timeout=TimeOut}};
handle_call({prompt, Data}, _From, OsProc) ->
    #os_proc{writer=Writer, reader=Reader} = OsProc,
    try
        Writer(OsProc, Data),
        {reply, {ok, Reader(OsProc)}, OsProc}
    catch
        throw:{error, OsError} ->
            {reply, OsError, OsProc};
        throw:OtherError ->
            {stop, normal, OtherError, OsProc}
    end.

handle_cast({set_receiver, ReceiverPid}, OsProc) ->
    {noreply, OsProc#os_proc{receiver=ReceiverPid}};
handle_cast({send, Data}, #os_proc{writer=Writer}=OsProc) ->
    try
        Writer(OsProc, Data),
        {noreply, OsProc}
    catch
        throw:OsError ->
            ?LOG_ERROR("Failed sending data: ~p -> ~p", [Data, OsError]),
            {stop, normal, OsProc}
    end;
handle_cast(stop, OsProc) ->
    {stop, normal, OsProc};
handle_cast(Msg, OsProc) ->
    ?LOG_DEBUG("OS Proc: Unknown cast: ~p", [Msg]),
    {noreply, OsProc}.

handle_info({Port, {exit_status, 0}}, #os_proc{port=Port}=OsProc) ->
    ?LOG_INFO("OS Process terminated normally", []),
    {stop, normal, OsProc};
handle_info({Port, {exit_status, Status}}, #os_proc{port=Port}=OsProc) ->
    ?LOG_ERROR("OS Process died with status: ~p", [Status]),
    case is_pid(OsProc#os_proc.receiver) of
    true ->
        OsProc#os_proc.receiver ! {error, {os_proc_exit_status, Status}};
    false ->
        ok
    end,
    {stop, {exit_status, Status}, OsProc};
handle_info({Port, {data, {noeol, Data}}}, #os_proc{port = Port} = OsProc) ->
    case is_pid(OsProc#os_proc.receiver) of
    true ->
        DataAcc2 = [Data | OsProc#os_proc.data_acc],
        {noreply, OsProc#os_proc{data_acc = DataAcc2}};
    false ->
        {stop, {unexpected_data, Data}, OsProc}
    end;
handle_info({Port, {data, {eol, Data}}}, #os_proc{port = Port} = OsProc) ->
    case is_pid(OsProc#os_proc.receiver) of
    true ->
        #os_proc{receiver = Receiver, data_acc = DataAcc} = OsProc,
        Line = lists:reverse(DataAcc, [Data]),
        try
            pick_command(Line)
        catch
        throw:abort ->
            Receiver ! {result, self(), {json, Line}},
            {noreply, OsProc#os_proc{data_acc = []}};
        throw:{cmd, _Cmd} ->
            try
                exec_command(Line, OsProc),
                {noreply, OsProc#os_proc{data_acc = []}}
            catch
            throw:{error, OsError} ->
                Receiver ! {result, self(), {error, OsError}},
                {noreply, OsProc#os_proc{data_acc = []}};
            throw:OtherError ->
                Receiver ! {result, self(), {error, OtherError}},
                {stop, normal, OsProc}
            end
        end;
    false ->
        {stop, {unexpected_data, Data}, OsProc}
    end;
handle_info({Port, Error}, #os_proc{port = Port, receiver = Receiver} = OsProc)
        when is_pid(Receiver) ->
    Receiver ! {result, self(), {error, Error}},
    {stop, Error, OsProc}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

