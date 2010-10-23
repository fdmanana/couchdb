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

default_config() ->
    test_util:build_file("etc/couchdb/default_dev.ini").

filename() -> test_util:build_file("test/etap/temp.180").

main(_) ->
    test_util:init_code_path(),
    couch_config:start_link([default_config()]),
    etap:plan(22),
    case (catch test()) of
        ok ->
            etap:end_tests();
        Other ->
            etap:diag(io_lib:format("Test died abnormally: ~p", [Other])),
            etap:bail()
    end,
    ok.

test() ->
    etap:is(fd:open("q353t25", []),
            {error, enoent},
            "Opening a file that does not exist should return error"),

    {ok, Fd} = fd:open(filename(), [create, overwrite]),

    etap:is_greater(Fd, 
                    0,
                    "File descriptor should be positive"),

    etap:is(fd:position(Fd),
            {ok, 0}, 
            "Should be at the beginning of the file after open"),

    etap:is(fd:truncate(Fd, 0),
            ok,
            "Truncating to 0"),

    etap:is(fd:bytes(Fd),
            {ok, 0}, 
            "File should be 0 bytes after truncate"),

    etap:is(fd:truncate(Fd, 10),
            ok, 
            "Truncate to 10 should return ok"),
    
    etap:is(fd:bytes(Fd),
            {ok, 10}, 
            "File should be 10 bytes after truncate"),
    
    etap:is(fd:sync(Fd),
            ok,
            "File sync works"),

    etap:is(fd:pread(Fd, 0, 10),
            {ok, <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>},
            "Reading 10 bytes from start of file works ater resize"),
    
    Bin1 = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 0>>,
    etap:is(fd:write(Fd, Bin1),
            ok, 
            "Writing 10 bytes to the end of the file"),

    etap:is(fd:position(Fd),
            {ok, 20}, 
            "Current position moved after write"),
    
    etap:is(fd:pread(Fd, 10, 10),
            {ok, Bin1},
            "Reading 10 bytes we just wrote to the end of the file"),

    etap:is(fd:position(Fd),
            {ok, 20}, 
            "Current position did not move after pread"),
    
    etap:is(fd:position(Fd, 0),
            {ok, 0}, 
            "Moving to the beginning of the file"),

    etap:is(fd:position(Fd),
            {ok, 0}, 
            "Current position is at the beginning of the file"),

    etap:is(fd:write(Fd, Bin1),
            ok,
            "Writing 10 bytes to the beginning of the file"),

    etap:is(fd:pread(Fd, 0, 10),
            {ok, Bin1},
            "Reading 10 bytes we just wrote to the beginning of the file"),

    Bin2 = <<"beefbabe">>,
    etap:is(fd:pwrite(Fd, 0, Bin2),
            {ok, byte_size(Bin2)},
            "Writing different bytes to the beginning of the file"),

    etap:is(fd:position(Fd),
            {ok, 10}, 
            "Current position did not move after pwrite"),
    
    etap:is(fd:pread(Fd, 0, byte_size(Bin2)),
            {ok, Bin2},
            "Reading bytes we just p-wrote beginning of the file"),

    etap:is(fd:pread(Fd, [{10, byte_size(Bin1)}, {0, byte_size(Bin2)}]),
            {ok, [Bin1, Bin2]},
            "Reading multiple chunks"),

    etap:is(fd:close(Fd),
            ok, 
            "File closes properly"),

    ok.
