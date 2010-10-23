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

-module(fd).

-export([open/2, close/1, sync/1, 
         bytes/1, write/2, pwrite/3, pread/2, pread/3,
         position/1, position/2, truncate/2]).

-on_load(init/0).

init() ->
    LibDir = case couch_config:get("couchdb", "util_driver_dir", null) of
                 null ->
                     filename:join(couch_util:priv_dir(), "lib");
                 LibDir0 -> LibDir0
             end,
    ok = erlang:load_nif(LibDir ++ "/fd", 0).

bytes(Fd) ->
    position(Fd, eof).

pread(Fd, Locs) ->
    case pread2(Fd, Locs) of
        {ok, L} ->
            {ok, lists:reverse(L)};
        Error ->
            Error
    end.

pread(Fd, Pos, Size) ->
    pread3(Fd, Pos, Size).

open(_Filename, _Opts) ->
    exit(nif_library_not_loaded).

close(_Fd) ->    
    exit(nif_library_not_loaded).

sync(_Fd) ->
    exit(nif_library_not_loaded).

truncate(_Fd) ->
    exit(nif_library_not_loaded).

truncate(_Fd, _Pos) ->
    exit(nif_library_not_loaded).

position(_Fd) ->
    exit(nif_library_not_loaded).

position(_Fd, _Pos) ->
    exit(nif_library_not_loaded).

write(_Fd, _Bin) ->
    exit(nif_library_not_loaded).

pwrite(_Fd, _Pos, _Bin) ->
    exit(nif_library_not_loaded).

pread2(_Fd, _Locs) ->
    exit(nif_library_not_loaded).

pread3(_Fd, _Pos, _Size) ->
    exit(nif_library_not_loaded).


