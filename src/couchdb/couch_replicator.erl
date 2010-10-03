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

-module(couch_replicator).
-behaviour(gen_server).

% public API
-export([replicate/1]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).

% Can't be greater than the maximum number of child restarts specified
% in couch_rep_sup.erl.
-define(MAX_RESTARTS, 3).


-record(rep_state, {
    rep_details,
    source_name,
    target_name,
    source,
    target,
    history,
    checkpoint_history,
    start_seq,
    current_through_seq,
    next_through_seqs = [],
    committed_seq,
    source_log,
    target_log,
    rep_starttime,
    src_starttime,
    tgt_starttime,
    timer, % checkpoint timer
    missing_revs_queue,
    changes_queue,
    changes_reader,
    missing_rev_finders,
    doc_copiers,
    seqs_in_progress = gb_trees:empty(),
    stats = #rep_stats{},
    msg_proxies = []
    }).


replicate(#rep{id = RepId, options = Options} = Rep) ->
    case get_value(cancel, Options, false) of
    true ->
        end_replication(RepId);
    false ->
        {ok, Listener} = rep_result_listener(RepId),
        Result = do_replication_loop(Rep),
        couch_replication_notifier:stop(Listener),
        Result
    end.


do_replication_loop(#rep{options = Options, source = Src} = Rep) ->
    DocIds = get_value(doc_ids, Options),
    Continuous = get_value(continuous, Options, false),
    Seq = case {DocIds, Continuous} of
    {undefined, false} ->
        last_seq(Src, Rep#rep.user_ctx);
    _ ->
        undefined
    end,
    do_replication_loop(Rep, Seq).

do_replication_loop(#rep{id = {BaseId,_} = Id, options = Options} = Rep, Seq) ->
    case start_replication(Rep) of
    {ok, _Pid} ->
        case get_value(continuous, Options, false) of
        true ->
            {ok, {continuous, ?l2b(BaseId)}};
        false ->
            Result = wait_for_result(Id),
            maybe_retry(Result, Rep, Seq)
        end;
    Error ->
        Error
    end.


maybe_retry(RepResult, _Rep, undefined) ->
    RepResult;
maybe_retry({ok, {Props}} = Result, Rep, Seq) ->
    case get_value(source_last_seq, Props) >= Seq of
    true ->
        Result;
    false ->
        do_replication_loop(Rep, Seq)
    end;
maybe_retry(RepResult, _Rep, _Seq) ->
    RepResult.


last_seq(DbName, UserCtx) ->
    case (catch couch_api_wrap:db_open(DbName, [{user_ctx, UserCtx}])) of
    {ok, Db} ->
        {ok, DbInfo} = couch_api_wrap:get_db_info(Db),
        couch_api_wrap:db_close(Db),
        get_value(<<"update_seq">>, DbInfo);
    _ ->
        undefined
    end.


start_replication(#rep{id = {BaseId, Extension}} = Rep) ->
    RepChildId = BaseId ++ Extension,
    ChildSpec = {
        RepChildId,
        {gen_server, start_link, [?MODULE, Rep, []]},
        transient,
        1,
        worker,
        [?MODULE]
    },
    case supervisor:start_child(couch_rep_sup, ChildSpec) of
    {ok, Pid} ->
        ?LOG_INFO("starting new replication ~p at ~p", [RepChildId, Pid]),
        {ok, Pid};
    {error, already_present} ->
        case supervisor:restart_child(couch_rep_sup, RepChildId) of
        {ok, Pid} ->
            ?LOG_INFO("starting replication ~p at ~p", [RepChildId, Pid]),
            {ok, Pid};
        {error, running} ->
            %% this error occurs if multiple replicators are racing
            %% each other to start and somebody else won.  Just grab
            %% the Pid by calling start_child again.
            {error, {already_started, Pid}} =
                supervisor:start_child(couch_rep_sup, ChildSpec),
            ?LOG_DEBUG("replication ~p already running at ~p",
                [RepChildId, Pid]),
            {ok, Pid};
        {error, _} = Err ->
            Err
        end;
    {error, {already_started, Pid}} ->
        ?LOG_DEBUG("replication ~p already running at ~p", [RepChildId, Pid]),
        {ok, Pid};
    {error, {Error, _}} ->
        {error, Error}
    end.


rep_result_listener(RepId) ->
    ReplyTo = self(),
    {ok, _Listener} = couch_replication_notifier:start_link(
        fun({_, RepId2, _} = Ev) when RepId2 =:= RepId ->
                ReplyTo ! Ev;
            (_) ->
                ok
        end).


wait_for_result(RepId) ->
    wait_for_result(RepId, ?MAX_RESTARTS).

wait_for_result(RepId, RetriesLeft) ->
    receive
    {finished, RepId, RepResult} ->
        {ok, RepResult};
    {error, RepId, Reason} ->
        case RetriesLeft > 0 of
        true ->
            wait_for_result(RepId, RetriesLeft - 1);
        false ->
            {error, Reason}
        end
    end.


end_replication({BaseId, Extension}) ->
    FullRepId = BaseId ++ Extension,
    case supervisor:terminate_child(couch_rep_sup, FullRepId) of
    {error, not_found} = R ->
        R;
    ok ->
        ok = supervisor:delete_child(couch_rep_sup, FullRepId),
        {ok, {cancelled, ?l2b(BaseId)}}
    end.


init(InitArgs) ->
    try
        do_init(InitArgs)
    catch
    throw:Error ->
        {stop, Error}
    end.

do_init(#rep{options = Options} = Rep) ->
    process_flag(trap_exit, true),

    #rep_state{
        source = Source,
        target = Target,
        start_seq = StartSeq
    } = State = init_state(Rep),

    {ok, MissingRevsQueue} = couch_work_queue:new(
        [{max_size, 100000}, {max_items, 2000}, {multi_workers, true}]),

    {RevFindersCount, CopiersCount} = case ?l2i(
        couch_config:get("replicator", "worker_processes", "10")) of
    Small when Small < 2 ->
        ?LOG_ERROR("The number of worker processes for the replicator "
            "should be at least 2", []),
        {1, 1};
    N ->
        {N div 2, (N div 2) + (N rem 2)}
    end,

    Proxies = case get_value(doc_ids, Options) of
    undefined ->
        {ok, ChangesQueue} = couch_work_queue:new(
            [{max_size, 100000}, {max_items, 500}, {multi_workers, true}]),

        ChangesProxy = couch_msg_grouper:start_link(self(), 300),
        % This starts the _changes reader process. It adds the changes from
        % the source db to the ChangesQueue.
        ChangesReader = spawn_changes_reader(ChangesProxy, StartSeq, Source,
            ChangesQueue, Options),

        RevsProxy = couch_msg_grouper:start_link(self(), 100),
        % This starts the missing rev finders. They check the target for changes
        % in the ChangesQueue to see if they exist on the target or not. If not,
        % adds them to MissingRevsQueue.
        MissingRevFinders =
            couch_replicator_rev_finders:spawn_missing_rev_finders(RevsProxy,
                Target, ChangesQueue, MissingRevsQueue, RevFindersCount),
        [ChangesProxy, RevsProxy];
    DocIds ->
        ChangesQueue = nil,
        ChangesReader = nil,
        MissingRevFinders =
            couch_replicator_rev_finders:spawn_missing_rev_finders(self(),
                Target, DocIds, MissingRevsQueue, RevFindersCount),
        []
    end,

    CopiersProxy = couch_msg_grouper:start_link(self(), 50),
    % This starts the doc copy processes. They fetch documents from the
    % MissingRevsQueue and copy them from the source to the target database.
    DocCopiers = couch_replicator_doc_copiers:spawn_doc_copiers(
        CopiersProxy, Source, Target, MissingRevsQueue, CopiersCount),

    {ok, State#rep_state{
            missing_revs_queue = MissingRevsQueue,
            changes_queue = ChangesQueue,
            changes_reader = ChangesReader,
            missing_rev_finders = MissingRevFinders,
            doc_copiers = DocCopiers,
            msg_proxies = [CopiersProxy | Proxies]
        }
    }.


handle_info({'EXIT', Pid, normal}, #rep_state{changes_reader=Pid} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #rep_state{changes_reader=Pid} = State) ->
    ?LOG_ERROR("ChangesReader process died with reason: ~p", [Reason]),
    {stop, changes_reader_died, cancel_timer(State)};

handle_info({'EXIT', Pid, normal}, #rep_state{missing_revs_queue=Pid} = St) ->
    {noreply, St};

handle_info({'EXIT', Pid, Reason}, #rep_state{missing_revs_queue=Pid} = St) ->
    ?LOG_ERROR("MissingRevsQueue process died with reason: ~p", [Reason]),
    {stop, missing_revs_queue_died, cancel_timer(St)};

handle_info({'EXIT', Pid, normal}, #rep_state{changes_queue=Pid} = State) ->
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, #rep_state{changes_queue=Pid} = State) ->
    ?LOG_ERROR("ChangesQueue process died with reason: ~p", [Reason]),
    {stop, changes_queue_died, cancel_timer(State)};

handle_info({'EXIT', Pid, normal}, State) ->
    #rep_state{
        doc_copiers = DocCopiers,
        missing_rev_finders = RevFinders,
        missing_revs_queue = RevsQueue,
        msg_proxies = Proxies
    } = State,
    case get_value(Pid, RevFinders) of
    undefined ->
        case get_value(Pid, DocCopiers) of
        undefined ->
            case lists:member(Pid, Proxies) of
            true ->
                {noreply, State#rep_state{msg_proxies = Proxies -- [Pid]}};
            false ->
                {stop, {unknown_process_died, Pid, normal}, State}
            end;
        _CopierId ->
            case lists:keydelete(Pid, 1, DocCopiers) of
            [] ->
                {stop, normal, do_last_checkpoint(State)};
            DocCopiers2 ->
                {noreply, State#rep_state{doc_copiers = DocCopiers2}}
            end
        end;
    _FinderId ->
        case lists:keydelete(Pid, 1, RevFinders) of
        [] ->
            couch_work_queue:close(RevsQueue),
            {noreply, State#rep_state{missing_rev_finders = []}};
        RevFinders2 ->
            {noreply, State#rep_state{missing_rev_finders = RevFinders2}}
        end
    end;

handle_info({'EXIT', Pid, Reason}, State) ->
    #rep_state{
        doc_copiers = DocCopiers,
        missing_rev_finders = RevFinders,
        msg_proxies = Proxies
    } = State,
    State2 = cancel_timer(State),
    case get_value(Pid, DocCopiers) of
    undefined ->
        case get_value(Pid, RevFinders) of
        undefined ->
            case lists:member(Pid, Proxies) of
            true ->
                ?LOG_ERROR("MsgProxy process died with reason: ~p", [Reason]),
                {stop, {msg_proxy_died, Pid, Reason}, State2};
            false ->
                {stop, {unknown_process_died, Pid, Reason}, State2}
            end;
        FinderId ->
            ?LOG_ERROR("RevsFinder process ~p died with reason: ~p",
                [FinderId, Reason]),
            {stop, {revs_finder_died, Pid, Reason}, State2}
        end;
    CopierId ->
        ?LOG_ERROR("DocCopier process ~p died with reason: ~p",
            [CopierId, Reason]),
        {stop, {doc_copier_died, Pid, Reason}, State2}
    end.


handle_call(Msg, _From, State) ->
    ?LOG_ERROR("Replicator received an unexpected synchronous call: ~p", [Msg]),
    {stop, unexpected_sync_message, State}.


handle_cast(checkpoint, State) ->
    State2 = do_checkpoint(State),
    {noreply, State2#rep_state{timer = start_timer(State)}};

handle_cast({seq_start, SeqRevs}, State) ->
    #rep_state{
        seqs_in_progress = SeqsInProgress,
        stats = #rep_stats{missing_checked = Mc} = Stats
    } = State,
    {SeqsInProgress2, Mc2} = lists:foldl(
        fun({Seq, NumRevs}, {InProgress, Missing}) ->
            {gb_trees:insert(Seq, NumRevs, InProgress), Missing + NumRevs}
        end,
        {SeqsInProgress, Mc}, SeqRevs
    ),
    NewState = State#rep_state{
        seqs_in_progress = SeqsInProgress2,
        stats = Stats#rep_stats{missing_checked = Mc2}
    },
    {noreply, NewState};

handle_cast({seq_changes_done, Changes}, State) ->
    {noreply, process_seq_changes_done(lists:flatten(Changes), State)};

handle_cast({add_stat, StatUpdates}, #rep_state{stats = Stats1} = State) ->
    NewStats = lists:foldl(
        fun({StatPos, Val}, Stats) ->
            Stat = element(StatPos, Stats),
            setelement(StatPos, Stats, Stat + Val)
        end,
        Stats1, StatUpdates
    ),
    {noreply, State#rep_state{stats = NewStats}};

handle_cast(Msg, State) ->
    ?LOG_ERROR("Replicator received an unexpected asynchronous call: ~p", [Msg]),
    {stop, unexpected_async_message, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(normal, #rep_state{rep_details = #rep{id = RepId}} = State) ->
    terminate_cleanup(State),
    couch_replication_notifier:notify({finished, RepId, get_result(State)});

terminate(shutdown, State) ->
    % cancelled replication throught ?MODULE:end_replication/1
    terminate_cleanup(State);

terminate(Reason, #rep_state{rep_details = #rep{id = RepId}} = State) ->
    terminate_cleanup(State),
    couch_replication_notifier:notify({error, RepId, Reason}).


terminate_cleanup(#rep_state{source = Source, target = Target}) ->
    couch_api_wrap:db_close(Source),
    couch_api_wrap:db_close(Target).


do_last_checkpoint(State) ->
    #rep_state{
        next_through_seqs = DoneSeqs,
        current_through_seq = Seq,
        seqs_in_progress = InProgress
    } = State,
    0 = gb_trees:size(InProgress),
    LastSeq = case DoneSeqs of
    [] ->
        Seq;
    _ ->
        lists:max([Seq, lists:last(DoneSeqs)])
    end,
    State2 = do_checkpoint(State#rep_state{
        current_through_seq = LastSeq,
        next_through_seqs = []}),
    cancel_timer(State2).


start_timer(#rep_state{rep_details = #rep{options = Options}} = State) ->
    case get_value(doc_ids, Options) of
    undefined ->
        After = checkpoint_interval(State),
        case timer:apply_after(After, gen_server, cast, [self(), checkpoint]) of
        {ok, Ref} ->
            Ref;
        Error ->
            ?LOG_ERROR("Replicator, error scheduling checkpoint:  ~p", [Error]),
            nil
        end;
    _DocIdList ->
        nil
    end.


cancel_timer(#rep_state{timer = nil} = State) ->
    State;
cancel_timer(#rep_state{timer = Timer} = State) ->
    {ok, cancel} = timer:cancel(Timer),
    State#rep_state{timer = nil}.


get_result(#rep_state{stats = Stats, rep_details = Rep} = State) ->
    case get_value(doc_ids, Rep#rep.options) of
    undefined ->
        State#rep_state.checkpoint_history;
    _DocIdList ->
        {[
            {<<"start_time">>, ?l2b(State#rep_state.rep_starttime)},
            {<<"end_time">>, ?l2b(httpd_util:rfc1123_date())},
            {<<"docs_read">>, Stats#rep_stats.docs_read},
            {<<"docs_written">>, Stats#rep_stats.docs_written},
            {<<"doc_write_failures">>, Stats#rep_stats.doc_write_failures}
        ]}
    end.


init_state(Rep) ->
    #rep{
        id = {BaseId, _Ext},
        source = Src, target = Tgt,
        options = Options, user_ctx = UserCtx
    } = Rep,
    {ok, Source} = couch_api_wrap:db_open(Src, [{user_ctx, UserCtx}]),
    {ok, Target} = couch_api_wrap:db_open(Tgt, [{user_ctx, UserCtx}],
        get_value(create_target, Options, false)),

    {ok, SourceInfo} = couch_api_wrap:get_db_info(Source),
    {ok, TargetInfo} = couch_api_wrap:get_db_info(Target),

    DocId = ?l2b(?LOCAL_DOC_PREFIX ++ BaseId),
    case couch_api_wrap:open_doc(Source, DocId, []) of
    {ok, SourceLog} ->  SourceLog;
    _ ->                SourceLog = #doc{id=DocId}
    end,
    case couch_api_wrap:open_doc(Target, DocId, []) of
    {ok, TargetLog} ->  TargetLog;
    _ ->                TargetLog = #doc{id=DocId}
    end,
    {StartSeq, History} = compare_replication_logs(SourceLog, TargetLog),
    #doc{body={CheckpointHistory}} = SourceLog,
    State = #rep_state{
        rep_details = Rep,
        source_name = couch_api_wrap:db_uri(Source),
        target_name = couch_api_wrap:db_uri(Target),
        source = Source,
        target = Target,
        history = History,
        checkpoint_history = {[{<<"no_changes">>, true}| CheckpointHistory]},
        start_seq = StartSeq,
        current_through_seq = StartSeq,
        committed_seq = StartSeq,
        source_log = SourceLog,
        target_log = TargetLog,
        rep_starttime = httpd_util:rfc1123_date(),
        src_starttime = get_value(<<"instance_start_time">>, SourceInfo),
        tgt_starttime = get_value(<<"instance_start_time">>, TargetInfo)
    },
    State#rep_state{timer = start_timer(State)}.


spawn_changes_reader(Cp, StartSeq, Source, ChangesQueue, Options) ->
    spawn_link(
        fun()->
            couch_api_wrap:changes_since(Source, all_docs, StartSeq,
                fun(#doc_info{high_seq=Seq, revs=Revs} = DocInfo) ->
                    Cp ! {cast, {seq_start, {Seq, length(Revs)}}},
                    ok = couch_work_queue:queue(ChangesQueue, DocInfo)
                end, Options),
            couch_work_queue:close(ChangesQueue)
        end).


checkpoint_interval(_State) ->
    5000.

do_checkpoint(#rep_state{current_through_seq=Seq, committed_seq=Seq} = State) ->
    State;
do_checkpoint(State) ->
    #rep_state{
        source_name=SourceName,
        target_name=TargetName,
        source = Source,
        target = Target,
        history = OldHistory,
        start_seq = StartSeq,
        current_through_seq = NewSeq,
        source_log = SourceLog,
        target_log = TargetLog,
        rep_starttime = ReplicationStartTime,
        src_starttime = SrcInstanceStartTime,
        tgt_starttime = TgtInstanceStartTime,
        stats = Stats
    } = State,
    case commit_to_both(Source, Target) of
    {SrcInstanceStartTime, TgtInstanceStartTime} ->
        ?LOG_INFO("recording a checkpoint for ~p -> ~p at source update_seq ~p",
            [SourceName, TargetName, NewSeq]),
        SessionId = couch_uuids:random(),
        NewHistoryEntry = {[
            {<<"session_id">>, SessionId},
            {<<"start_time">>, list_to_binary(ReplicationStartTime)},
            {<<"end_time">>, list_to_binary(httpd_util:rfc1123_date())},
            {<<"start_last_seq">>, StartSeq},
            {<<"end_last_seq">>, NewSeq},
            {<<"recorded_seq">>, NewSeq},
            {<<"missing_checked">>, Stats#rep_stats.missing_checked},
            {<<"missing_found">>, Stats#rep_stats.missing_found},
            {<<"docs_read">>, Stats#rep_stats.docs_read},
            {<<"docs_written">>, Stats#rep_stats.docs_written},
            {<<"doc_write_failures">>, Stats#rep_stats.doc_write_failures}
        ]},
        % limit history to 50 entries
        NewRepHistory = {[
            {<<"session_id">>, SessionId},
            {<<"source_last_seq">>, NewSeq},
            {<<"history">>, lists:sublist([NewHistoryEntry | OldHistory], 50)}
        ]},

        try
            {ok, {SrcRevPos,SrcRevId}} = couch_api_wrap:update_doc(Source,
                SourceLog#doc{body=NewRepHistory}, [delay_commit]),
            {ok, {TgtRevPos,TgtRevId}} = couch_api_wrap:update_doc(Target,
                TargetLog#doc{body=NewRepHistory}, [delay_commit]),
            State#rep_state{
                checkpoint_history = NewRepHistory,
                committed_seq = NewSeq,
                source_log = SourceLog#doc{revs={SrcRevPos, [SrcRevId]}},
                target_log = TargetLog#doc{revs={TgtRevPos, [TgtRevId]}}
            }
        catch throw:conflict ->
            ?LOG_ERROR("checkpoint failure: conflict (are you replicating to "
                "yourself?)", []),
            State
        end;
    _Else ->
        ?LOG_INFO("rebooting ~p -> ~p from last known replication checkpoint",
            [SourceName, TargetName]),
        throw(restart)
    end.


commit_to_both(Source, Target) ->
    % commit the src async
    ParentPid = self(),
    SrcCommitPid = spawn_link(
        fun() ->
            ParentPid ! {self(), couch_api_wrap:ensure_full_commit(Source)}
        end),

    % commit tgt sync
    {ok, TargetStartTime} = couch_api_wrap:ensure_full_commit(Target),

    SourceStartTime =
    receive
    {SrcCommitPid, {ok, Timestamp}} ->
        receive
        {'EXIT', SrcCommitPid, normal} ->
            ok
        end,
        Timestamp;
    {'EXIT', SrcCommitPid, _} ->
        exit(replication_link_failure)
    end,
    {SourceStartTime, TargetStartTime}.


compare_replication_logs(SrcDoc, TgtDoc) ->
    #doc{body={RepRecProps}} = SrcDoc,
    #doc{body={RepRecPropsTgt}} = TgtDoc,
    case get_value(<<"session_id">>, RepRecProps) ==
            get_value(<<"session_id">>, RepRecPropsTgt) of
    true ->
        % if the records have the same session id,
        % then we have a valid replication history
        OldSeqNum = get_value(<<"source_last_seq">>, RepRecProps, 0),
        OldHistory = get_value(<<"history">>, RepRecProps, []),
        {OldSeqNum, OldHistory};
    false ->
        SourceHistory = get_value(<<"history">>, RepRecProps, []),
        TargetHistory = get_value(<<"history">>, RepRecPropsTgt, []),
        ?LOG_INFO("Replication records differ. "
                "Scanning histories to find a common ancestor.", []),
        ?LOG_DEBUG("Record on source:~p~nRecord on target:~p~n",
                [RepRecProps, RepRecPropsTgt]),
        compare_rep_history(SourceHistory, TargetHistory)
    end.

compare_rep_history(S, T) when S =:= [] orelse T =:= [] ->
    ?LOG_INFO("no common ancestry -- performing full replication", []),
    {0, []};
compare_rep_history([{S} | SourceRest], [{T} | TargetRest] = Target) ->
    SourceId = get_value(<<"session_id">>, S),
    case has_session_id(SourceId, Target) of
    true ->
        RecordSeqNum = get_value(<<"recorded_seq">>, S, 0),
        ?LOG_INFO("found a common replication record with source_seq ~p",
            [RecordSeqNum]),
        {RecordSeqNum, SourceRest};
    false ->
        TargetId = get_value(<<"session_id">>, T),
        case has_session_id(TargetId, SourceRest) of
        true ->
            RecordSeqNum = get_value(<<"recorded_seq">>, T, 0),
            ?LOG_INFO("found a common replication record with source_seq ~p",
                [RecordSeqNum]),
            {RecordSeqNum, TargetRest};
        false ->
            compare_rep_history(SourceRest, TargetRest)
        end
    end.


has_session_id(_SessionId, []) ->
    false;
has_session_id(SessionId, [{Props} | Rest]) ->
    case get_value(<<"session_id">>, Props, nil) of
    SessionId ->
        true;
    _Else ->
        has_session_id(SessionId, Rest)
    end.


process_seq_changes_done(Changes, State) ->
    #rep_state{
        seqs_in_progress = SeqsInProgress,
        next_through_seqs = DoneSeqs,
        current_through_seq = ThroughSeq
    } = State,

    {SeqsInProgress2, LastSeq} = lists:foldl(
        fun({Seq, ChangesDone}, {SeqTree, MaxSeqDone}) ->
            Total = gb_trees:get(Seq, SeqTree),
            case Total - ChangesDone of
            0 ->
                NewMaxSeqDone = case MaxSeqDone of
                nil ->
                    Seq;
                _ ->
                    lists:max([Seq, MaxSeqDone])
                end,
                {gb_trees:delete(Seq, SeqTree), NewMaxSeqDone};
            NewTotal when NewTotal > 0 ->
                {gb_trees:update(Seq, NewTotal, SeqTree), MaxSeqDone}
            end
        end,
        {SeqsInProgress, nil}, Changes),

    DoneSeqs2 = add_done_seq(LastSeq, DoneSeqs),
    {NewThroughSeq, DoneSeqs3} =
        get_next_through_seq(ThroughSeq, SeqsInProgress2, DoneSeqs2),

    State#rep_state{
        seqs_in_progress = SeqsInProgress2,
        next_through_seqs = DoneSeqs3,
        current_through_seq = NewThroughSeq
    }.


add_done_seq(nil, DoneSeqs) ->
    DoneSeqs;
add_done_seq(Seq, DoneSeqs) ->
    ordsets:add_element(Seq, DoneSeqs).


get_next_through_seq(Current, InProgress, Done) ->
    case gb_trees:is_empty(InProgress) of
    true ->
        case Done of
        [] ->
            {Current, Done};
        _ ->
            {lists:last(Done), []}
        end;
    false ->
        {SmallestInProgress, _} = gb_trees:smallest(InProgress),
        get_next_through_seq(Current, SmallestInProgress, Done, [])
    end.


get_next_through_seq(Current, _SmallestInProgress, [], NewDone) ->
    {Current, NewDone};
get_next_through_seq(Current, SmallestInProgress, Done, NewDone) ->
    LargestDone = lists:last(Done),
    case LargestDone =< SmallestInProgress of
    true ->
        {LargestDone, NewDone};
    false ->
        get_next_through_seq(Current, SmallestInProgress,
            ordsets:del_element(LargestDone, NewDone), [LargestDone | NewDone])
    end.

