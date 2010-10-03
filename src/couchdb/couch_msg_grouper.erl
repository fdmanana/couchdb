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

-module(couch_msg_grouper).

-export([start_link/2, start_link/3]).

-include("couch_db.hrl").



start_link(Target, MaxGroupSize) ->
    start_link(Target, MaxGroupSize, 0).

start_link(Target, MaxGroupSize, After) ->
    spawn_link(fun() -> receive_loop(Target, MaxGroupSize, After) end).


receive_loop(Target, MaxGroupSize, T) ->
    receive
    stop ->
        ok;
    {MsgType, {Tag, Value}} ->
        MsgGroup = collect_identical(MsgType, Tag, MaxGroupSize, T, [Value]),
        send_msg(Target, MsgType, MsgGroup),
        receive_loop(Target, MaxGroupSize, T)
    end.


collect_identical(_MsgType, Tag, Max, _T, Acc) when length(Acc) >= Max ->
    {Tag, lists:reverse(Acc)};
collect_identical(MsgType, Tag, Max, T, Acc) ->
    receive
    {MsgType, {Tag, Value}} ->
        collect_identical(MsgType, Tag, Max, T, [Value | Acc])
    after T ->
        {Tag, lists:reverse(Acc)}
    end.


send_msg(Target, info, Msg) ->
    Target ! Msg;
send_msg(Target, cast, Msg) ->
    ok = gen_server:cast(Target, Msg);
send_msg(Target, call, Msg) ->
    gen_server:call(Target, Msg).

