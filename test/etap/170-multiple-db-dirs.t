#!/usr/bin/env escript
%% -*- erlang -*-

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

-record(user_ctx, {
    name = null,
    roles = [],
    handler
}).

default_config() ->
    test_util:build_file("etc/couchdb/default_dev.ini").

num_dbs() ->
    4.

db_dir_1() ->
    test_util:build_file("tmp/lib/dir_1").
db_dir_2() ->
    test_util:build_file("tmp/lib/dir_2").

main(_) ->
    test_util:init_code_path(),

    etap:plan(20),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail(Other)
    end,
    ok.

test() ->
    couch_server_sup:start_link([default_config()]),
    ok = empty_db_dirs(),
    couch_config:set(
        "couchdb", "database_dirs", string:join([db_dir_1(), db_dir_2()], ", ")
    ),
    timer:sleep(1000),

    ok = create_test_dbs(),

    ok = populate_test_dbs(),
    ok = dbs_have_doc_ids([<<"doc1">>]),
    ok = test_all_dbs(),
    ok = dbs_per_dir(),

    ok = compact_test_dbs(),
    ok = dbs_have_doc_ids([<<"doc1">>]),
    ok = test_all_dbs(),
    ok = dbs_per_dir(),

    ok = delete_test_dbs(),

    couch_server_sup:stop(),
    timer:sleep(1000),
    ok = empty_db_dirs().

admin_user_ctx() ->
    {user_ctx, #user_ctx{roles=[<<"_admin">>]}}.

create_db(Name) ->
    {ok, _} = couch_db:create(Name, [admin_user_ctx()]),
    ok.

delete_db(Name) ->
    couch_server:delete(Name, [admin_user_ctx()]),
    ok.

db_name(N) ->
    iolist_to_binary(["test_db_", integer_to_list(N)]).

b2l(B) ->
    binary_to_list(B).

create_test_dbs() ->
    lists:foreach(
        fun(I) ->
            DbName = db_name(I),
            ok = delete_db(DbName),
            ok = create_db(DbName)
        end,
        lists:seq(1, num_dbs())
    ),
    ok.

dbs_per_dir() ->
    DirDbCount = lists:foldl(
        fun(I, Acc) ->
            DbFile = b2l(db_name(I)) ++ ".couch",
            case couch_util:file_exists(DbFile, [db_dir_1(), db_dir_2()]) of
            {ok, FullPath} ->
                Dir = filename:dirname(FullPath),
                Count = couch_util:get_value(Dir, Acc, 0),
                lists:keystore(Dir, 1, Acc, {Dir, Count + 1});
            not_found ->
                Acc
            end
        end,
        [],
        lists:seq(1, num_dbs())
    ),
    NumDbsPerDir = num_dbs() div 2,
    Dir1Count = couch_util:get_value(db_dir_1(), DirDbCount, 0),
    Dir2Count = couch_util:get_value(db_dir_2(), DirDbCount, 0),
    etap:is(
        Dir1Count,
        NumDbsPerDir,
        "DB dir 1 has " ++ integer_to_list(NumDbsPerDir) ++ " DBs"
    ),
    etap:is(
        Dir2Count,
        NumDbsPerDir,
        "DB dir 2 has " ++ integer_to_list(NumDbsPerDir) ++ " DBs"
    ),
    ok.

test_all_dbs() ->
    {ok, AllDbs} = couch_server:all_databases(),
    lists:foreach(
        fun(I) ->
            DbName = db_name(I),
            DbListed = lists:member(DbName, AllDbs),
            etap:is(
                DbListed, true, "DB " ++ b2l(DbName) ++ " in all_databases()"
            )
        end,
        lists:seq(1, num_dbs())
    ),
    ok.

populate_test_dbs() ->
    Docs = [
        {[
            {<<"_id">>, <<"doc1">>},
            {<<"value">>, 111}
        ]}
    ],
    lists:foreach(
        fun(I) ->
            ok = create_docs(db_name(I), Docs)
        end,
        lists:seq(1, num_dbs())
    ),
    ok.

create_docs(DbName, JsonDocList) ->
    {ok, Db} = couch_db:open(DbName, [admin_user_ctx()]),
    Docs = lists:map(fun couch_doc:from_json_obj/1, JsonDocList),
    {ok, _} = couch_db:update_docs(Db, Docs),
    couch_db:ensure_full_commit(Db),
    couch_db:close(Db),
    ok.

dbs_have_doc_ids(DocIdList) ->
    lists:foreach(
        fun(I) ->
            DbName = db_name(I),
            {ok, Db} = couch_db:open(DbName, [admin_user_ctx()]),
            lists:foreach(
                fun(DocId) ->
                    HasDoc = case couch_db:open_doc(Db, DocId, []) of
                    {ok, _Doc} ->
                        true;
                    _ ->
                        false
                    end,
                    etap:is(
                        HasDoc,
                        true,
                        "DB " ++ b2l(DbName) ++ " has doc " ++ b2l(DocId)
                    )
                end,
                DocIdList
            ),
            couch_db:close(Db)
        end,
        lists:seq(1, num_dbs())
    ),
    ok.

delete_test_dbs() ->
    lists:foreach(
        fun(I) ->
            ok = delete_db(db_name(I))
        end,
        lists:seq(1, num_dbs())
    ),
    ok.

compact_test_dbs() ->
    lists:foreach(
        fun(I) ->
            {ok, Db} = couch_db:open(db_name(I), [admin_user_ctx()]),
            couch_db:start_compact(Db),
            couch_db:close(Db)
        end,
        lists:seq(1, num_dbs())
    ),
    timer:sleep(800 * num_dbs()),
    ok.

empty_db_dirs() ->
    filelib:fold_files(
        test_util:build_file("tmp/lib/"),
        "\.couch$",
        true,
        fun(F, _) -> ok = file:delete(F) end,
        []
    ),
    ok.
