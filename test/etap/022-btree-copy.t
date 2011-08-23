#!/usr/bin/env escript
%% -*- erlang -*-
%%! -pa ./src/couchdb -sasl errlog_type error -boot start_sasl -noshell

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

-record(btree, {
    fd,
    root,
    extract_kv,
    assemble_kv,
    less,
    reduce,
    compression
}).

path(NumItems) ->
    test_util:build_file("test_btree_" ++ integer_to_list(NumItems) ++ ".dat").

path(NumItems, copy) ->
    test_util:build_file("test_btree_" ++ integer_to_list(NumItems) ++ "_copy.dat").

main(_) ->
    test_util:init_code_path(),
    etap:plan(54),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

test() ->
    ReduceCount = fun(reduce, KVs) ->
            length(KVs);
        (rereduce, Reds) ->
            lists:sum(Reds)
    end,
    SplitFun = fun(I) -> {I, I} end,
    JoinFun = fun(I, I) -> I end,

    test_copy(10, ReduceCount, SplitFun, JoinFun),
    test_copy(20, ReduceCount, SplitFun, JoinFun),
    test_copy(50, ReduceCount, SplitFun, JoinFun),
    test_copy(100, ReduceCount, SplitFun, JoinFun),
    test_copy(300, ReduceCount, SplitFun, JoinFun),
    test_copy(500, ReduceCount, SplitFun, JoinFun),
    test_copy(700, ReduceCount, SplitFun, JoinFun),
    test_copy(811, ReduceCount, SplitFun, JoinFun),
    test_copy(2333, ReduceCount, SplitFun, JoinFun),
    test_copy(6666, ReduceCount, SplitFun, JoinFun),
    test_copy(9999, ReduceCount, SplitFun, JoinFun),
    test_copy(15003, ReduceCount, SplitFun, JoinFun),
    test_copy(21477, ReduceCount, SplitFun, JoinFun),
    test_copy(38888, ReduceCount, SplitFun, JoinFun),
    test_copy(66069, ReduceCount, SplitFun, JoinFun),
    test_copy(150123, ReduceCount, SplitFun, JoinFun),
    test_copy(420789, ReduceCount, SplitFun, JoinFun),
    test_copy(711321, ReduceCount, SplitFun, JoinFun),
    ok.


test_copy(NumItems, ReduceFun, SplitFun, JoinFun) ->
    KVs = [{I, I} || I <- lists:seq(1, NumItems)],
    {ok, #btree{fd = Fd} = Btree} = make_btree(KVs, ReduceFun, SplitFun, JoinFun),
    {_, Red, _} = couch_btree:get_state(Btree),
    {ok, FdCopy} = couch_file:open(path(NumItems, copy), [create, overwrite]),
    {ok, RootCopy} = couch_btree_copy:copy(Btree, FdCopy, []),
    {ok, BtreeCopy} = couch_btree:open(
        RootCopy, FdCopy, [{compression, none}, {reduce, ReduceFun},
            {join, JoinFun}, {split, SplitFun}]),
    check_btree_copy(BtreeCopy, Red, KVs),
    ok = couch_file:close(Fd),
    ok = couch_file:close(FdCopy),
    ok = file:delete(path(NumItems)),
    ok = file:delete(path(NumItems, copy)).


check_btree_copy(Btree, Red, KVs) ->
    {_, RedCopy, _} = couch_btree:get_state(Btree),
    etap:is(Red, RedCopy, "btree copy has same reduce value"),
    {ok, _, CopyKVs} = couch_btree:fold(
        Btree,
        fun(KV, _, Acc) -> {ok, [KV | Acc]} end,
        [], []),
    etap:is(KVs, lists:reverse(CopyKVs), "btree copy has same keys").


make_btree(KVs, ReduceFun, SplitFun, JoinFun) ->
    {ok, Fd} = couch_file:open(path(length(KVs)), [create, overwrite]),
    {ok, Btree} = couch_btree:open(
        nil, Fd, [{compression, none}, {reduce, ReduceFun},
           {join, JoinFun}, {split, SplitFun}]),
    {ok, Btree2} = couch_btree:add_remove(Btree, KVs, []),
    {_, Red, _} = couch_btree:get_state(Btree2),
    etap:is(length(KVs), Red,
        "Inserted " ++ integer_to_list(length(KVs)) ++ " items into a new btree"),
    ok = couch_file:sync(Fd),
    {ok, Btree2}.
