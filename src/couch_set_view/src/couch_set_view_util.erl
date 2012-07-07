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

-module(couch_set_view_util).

-export([expand_dups/2, expand_dups/3, partitions_map/2]).
-export([build_bitmask/1, decode_bitmask/1]).
-export([make_btree_purge_fun/1]).
-export([make_key_options/1]).
-export([design_doc_to_set_view_group/2, get_ddoc_ids_with_sig/2]).
-export([open_raw_read_fd/1, close_raw_read_fd/1]).
-export([make_disk_header/1]).
-export([compute_indexed_bitmap/1, cleanup_group/1]).
-export([missing_changes_count/2]).
-export([is_group_empty/1]).
-export([new_sort_file_path/1, delete_sort_files/1]).

-include("couch_db.hrl").
-include_lib("couch_set_view/include/couch_set_view.hrl").


expand_dups([], Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {PartId, {dups, Vals}}} | Rest], Acc) ->
    Expanded = lists:map(fun(Val) -> {Key, {PartId, Val}} end, Vals),
    expand_dups(Rest, Expanded ++ Acc);
expand_dups([{_Key, {_PartId, _Val}} = Kv | Rest], Acc) ->
    expand_dups(Rest, [Kv | Acc]).


expand_dups([], _Abitmask, Acc) ->
    lists:reverse(Acc);
expand_dups([{Key, {PartId, {dups, Vals}}} | Rest], Abitmask, Acc) ->
    case (1 bsl PartId) band Abitmask of
    0 ->
        expand_dups(Rest, Abitmask, Acc);
    _ ->
        Expanded = lists:map(fun(V) -> {Key, {PartId, V}} end, Vals),
        expand_dups(Rest, Abitmask, Expanded ++ Acc)
    end;
expand_dups([{_Key, {PartId, _Val}} = Kv | Rest], Abitmask, Acc) ->
    case (1 bsl PartId) band Abitmask of
    0 ->
        expand_dups(Rest, Abitmask, Acc);
    _ ->
        expand_dups(Rest, Abitmask, [Kv | Acc])
    end.


-spec partitions_map([{term(), {partition_id(), term()}}], bitmask()) -> bitmask().
partitions_map([], BitMap) ->
    BitMap;
partitions_map([{_Key, {PartitionId, _Val}} | RestKvs], BitMap) ->
    partitions_map(RestKvs, BitMap bor (1 bsl PartitionId)).


-spec build_bitmask([partition_id()]) -> bitmask().
build_bitmask(ActiveList) ->
    build_bitmask(ActiveList, 0).

-spec build_bitmask([partition_id()], bitmask()) -> bitmask().
build_bitmask([], Acc) ->
    Acc;
build_bitmask([PartId | Rest], Acc) when is_integer(PartId), PartId >= 0 ->
    build_bitmask(Rest, (1 bsl PartId) bor Acc).


-spec decode_bitmask(bitmask()) -> ordsets:ordset(partition_id()).
decode_bitmask(Bitmask) ->
    decode_bitmask(Bitmask, 0).

-spec decode_bitmask(bitmask(), partition_id()) -> [partition_id()].
decode_bitmask(0, _) ->
    [];
decode_bitmask(Bitmask, PartId) ->
    case Bitmask band 1 of
    1 ->
        [PartId | decode_bitmask(Bitmask bsr 1, PartId + 1)];
    0 ->
        decode_bitmask(Bitmask bsr 1, PartId + 1)
    end.


-spec make_btree_purge_fun(#set_view_group{}) -> set_view_btree_purge_fun().
make_btree_purge_fun(Group) when ?set_cbitmask(Group) =/= 0 ->
    fun(branch, Value, {go, Acc}) ->
            receive
            stop ->
                % Being stopped by main updater process or by the view group.
                {stop, {stop, Acc}};
            stop_immediately ->
                % Updater performing full cleanup (current cleanup bitmask +
                % pending transition cleanup bitmask) in order to apply
                % pending transition asap.
                {stop, {stop, Acc}}
            after 0 ->
                btree_purge_fun(branch, Value, {go, Acc}, ?set_cbitmask(Group))
            end;
        (value, Value, {go, Acc}) ->
            btree_purge_fun(value, Value, {go, Acc}, ?set_cbitmask(Group))
    end.

btree_purge_fun(value, {_K, {PartId, _}}, {go, Acc}, Cbitmask) ->
    Mask = 1 bsl PartId,
    case (Cbitmask band Mask) of
    Mask ->
        {purge, {go, Acc + 1}};
    0 ->
        {keep, {go, Acc}}
    end;
btree_purge_fun(branch, Red, {go, Acc}, Cbitmask) ->
    Bitmap = element(tuple_size(Red), Red),
    case Bitmap band Cbitmask of
    0 ->
        {keep, {go, Acc}};
    Bitmap ->
        {purge, {go, Acc + element(1, Red)}};
    _ ->
        {partial_purge, {go, Acc}}
    end.


-spec make_key_options(#view_query_args{}) -> [{atom(), term()}].
make_key_options(QueryArgs) ->
    couch_httpd_view:make_key_options(QueryArgs).


-spec get_ddoc_ids_with_sig(binary(), binary()) -> [binary()].
get_ddoc_ids_with_sig(SetName, ViewGroupSig) ->
    {ok, Db} = couch_db:open_int(?master_dbname(SetName), []),
    {ok, DDocList} = couch_db:get_design_docs(Db, no_deletes),
    ok = couch_db:close(Db),
    lists:foldl(
        fun(#doc{id = Id} = DDoc, Acc) ->
            case design_doc_to_set_view_group(SetName, DDoc) of
            #set_view_group{sig = ViewGroupSig} ->
                [Id | Acc];
            #set_view_group{sig = _OtherSig} ->
                Acc
            end
        end,
        [], DDocList).


-spec design_doc_to_set_view_group(binary(), #doc{}) -> #set_view_group{}.
design_doc_to_set_view_group(SetName, #doc{id = Id, body = {Fields}}) ->
    {DesignOptions} = couch_util:get_value(<<"options">>, Fields, {[]}),
    {RawViews} = couch_util:get_value(<<"views">>, Fields, {[]}),
    % add the views to a dictionary object, with the map source as the key
    DictBySrc =
    lists:foldl(
        fun({Name, {MRFuns}}, DictBySrcAcc) ->
            case couch_util:get_value(<<"map">>, MRFuns) of
            undefined -> DictBySrcAcc;
            MapSrc ->
                RedSrc = couch_util:get_value(<<"reduce">>, MRFuns, null),
                {ViewOptions} = couch_util:get_value(<<"options">>, MRFuns, {[]}),
                View =
                case dict:find({MapSrc, ViewOptions}, DictBySrcAcc) of
                    {ok, View0} -> View0;
                    error -> #set_view{def = MapSrc, options = ViewOptions}
                end,
                View2 =
                if RedSrc == null ->
                    View#set_view{map_names = [Name | View#set_view.map_names]};
                true ->
                    View#set_view{reduce_funs = [{Name, RedSrc} | View#set_view.reduce_funs]}
                end,
                dict:store({MapSrc, ViewOptions}, View2, DictBySrcAcc)
            end
        end, dict:new(), RawViews),
    % number the views
    {Views, _N} = lists:mapfoldl(
        fun({_Src, View}, N) ->
            {View#set_view{id_num = N}, N + 1}
        end,
        0, lists:sort(dict:to_list(DictBySrc))),
    SetViewGroup = #set_view_group{
        set_name = SetName,
        name = Id,
        views = Views,
        design_options = DesignOptions
    },
    set_view_sig(SetViewGroup).


-spec set_view_sig(#set_view_group{}) -> #set_view_group{}.
set_view_sig(#set_view_group{
            views = Views,
            design_options = DesignOptions} = G) ->
    Sig = couch_util:md5(term_to_binary({Views, DesignOptions})),
    G#set_view_group{sig = Sig}.


-spec open_raw_read_fd(#set_view_group{}) -> 'ok'.
open_raw_read_fd(Group) ->
    #set_view_group{
        fd = FilePid,
        filepath = FileName,
        set_name = SetName,
        type = Type,
        name = DDocId
    } = Group,
    case file:open(FileName, [read, raw, binary]) of
    {ok, RawReadFd} ->
        erlang:put({FilePid, fast_fd_read}, RawReadFd),
        ok;
    {error, Reason} ->
        ?LOG_INFO("Warning, could not open raw fd for fast reads for "
            "~s view group `~s`, set `~s`: ~s",
            [Type, DDocId, SetName, file:format_error(Reason)]),
        ok
    end.


-spec close_raw_read_fd(#set_view_group{}) -> 'ok'.
close_raw_read_fd(#set_view_group{fd = FilePid}) ->
    case erlang:erase({FilePid, fast_fd_read}) of
    undefined ->
        ok;
    Fd ->
        ok = file:close(Fd)
    end.


-spec make_disk_header(#set_view_group{}) ->
                              {Signature::binary(), #set_view_index_header{}}.
make_disk_header(Group) ->
    #set_view_group{
        sig = Sig,
        id_btree = IdBtree,
        views = Views,
        index_header = Header
    } = Group,
    ViewStates = [couch_btree:get_state(V#set_view.btree) || V <- Views],
    Header2 = Header#set_view_index_header{
        id_btree_state = couch_btree:get_state(IdBtree),
        view_states = ViewStates
    },
    {Sig, Header2}.


-spec compute_indexed_bitmap(#set_view_group{}) -> bitmap().
compute_indexed_bitmap(#set_view_group{id_btree = IdBtree, views = Views}) ->
    compute_indexed_bitmap(IdBtree, Views).

compute_indexed_bitmap(IdBtree, Views) ->
    {ok, {_, IdBitmap}} = couch_btree:full_reduce(IdBtree),
    lists:foldl(fun(#set_view{btree = Bt}, AccMap) ->
        {ok, {_, _, Bm}} = couch_btree:full_reduce(Bt),
        AccMap bor Bm
    end,
    IdBitmap, Views).


-spec cleanup_group(#set_view_group{}) -> {'ok', #set_view_group{}, non_neg_integer()}.
cleanup_group(Group) when ?set_cbitmask(Group) == 0 ->
    {ok, Group, 0};
cleanup_group(Group) ->
    #set_view_group{
        index_header = Header,
        id_btree = IdBtree,
        views = Views
    } = Group,
    PurgeFun = make_btree_purge_fun(Group),
    ok = couch_set_view_util:open_raw_read_fd(Group),
    {ok, NewIdBtree, {Go, IdPurgedCount}} =
        couch_btree:guided_purge(IdBtree, PurgeFun, {go, 0}),
    {TotalPurgedCount, NewViews} =
        clean_views(Go, PurgeFun, Views, IdPurgedCount, []),
    ok = couch_set_view_util:close_raw_read_fd(Group),
    IndexedBitmap = compute_indexed_bitmap(NewIdBtree, NewViews),
    Group2 = Group#set_view_group{
        id_btree = NewIdBtree,
        views = NewViews,
        index_header = Header#set_view_index_header{
            cbitmask = ?set_cbitmask(Group) band IndexedBitmap,
            id_btree_state = couch_btree:get_state(NewIdBtree),
            view_states = [couch_btree:get_state(V#set_view.btree) || V <- NewViews]
        }
    },
    ok = couch_file:flush(Group#set_view_group.fd),
    {ok, Group2, TotalPurgedCount}.

clean_views(_, _, [], Count, Acc) ->
    {Count, lists:reverse(Acc)};
clean_views(stop, _, Rest, Count, Acc) ->
    {Count, lists:reverse(Acc, Rest)};
clean_views(go, PurgeFun, [#set_view{btree = Btree} = View | Rest], Count, Acc) ->
    couch_set_view_mapreduce:start_reduce_context(View),
    {ok, NewBtree, {Go, PurgedCount}} =
        couch_btree:guided_purge(Btree, PurgeFun, {go, Count}),
    couch_set_view_mapreduce:end_reduce_context(View),
    NewAcc = [View#set_view{btree = NewBtree} | Acc],
    clean_views(Go, PurgeFun, Rest, PurgedCount, NewAcc).


-spec missing_changes_count(partition_seqs(), partition_seqs()) -> non_neg_integer().
missing_changes_count(CurSeqs, NewSeqs) ->
    missing_changes_count(CurSeqs, NewSeqs, 0).

missing_changes_count([], [], MissingCount) ->
    MissingCount;
missing_changes_count([{Part, CurSeq} | RestCur], [{Part, NewSeq} | RestNew], Acc) ->
    Diff = CurSeq - NewSeq,
    case Diff > 0 of
    true ->
        missing_changes_count(RestCur, RestNew, Acc + Diff);
    false ->
        missing_changes_count(RestCur, RestNew, Acc)
    end.


-spec is_group_empty(#set_view_group{}) -> boolean().
is_group_empty(Group) ->
    Predicate = fun({_PartId, Seq}) -> Seq == 0 end,
    lists:all(Predicate, ?set_seqs(Group)) andalso
        lists:all(Predicate, ?set_unindexable_seqs(Group)).


-spec new_sort_file_path(string()) -> string().
new_sort_file_path(RootDir) ->
    Base = ?b2l(couch_uuids:new()) ++ ".sort",
    Path = filename:join([RootDir, Base]),
    ok = filelib:ensure_dir(Path),
    Path.


-spec delete_sort_files(string()) -> 'ok'.
delete_sort_files(RootDir) ->
    WildCard = filename:join([RootDir, "*"]),
    lists:foreach(
        fun(F) -> _ = file:delete(F) end,
        filelib:wildcard(WildCard)).
