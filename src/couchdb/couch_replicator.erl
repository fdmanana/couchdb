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

 % meant to be used only by the replicator database listener
-export([async_replicate/1]).
-export([end_replication/1]).

% gen_server callbacks
-export([init/1, terminate/2, code_change/3]).
-export([handle_call/3, handle_cast/2, handle_info/2]).

-include("couch_db.hrl").
-include("couch_api_wrap.hrl").

-import(couch_util, [
    get_value/2,
    get_value/3
]).

-import(couch_replicator_utils, [
    update_rep_doc/2
]).

-record(rep_state, {
    rep_details,
    source_name,
    target_name,
    source,
    target,
    history,
    checkpoint_history,
    start_seq,
    committed_seq,
    current_through_seq,
    done_seqs = dict:new(),
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
    workers,
    worker_steps,
    stats = #rep_stats{},
    session_id,
    source_db_update_notifier = nil,
    target_db_update_notifier = nil,
    source_monitor = nil,
    target_monitor = nil
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


do_replication_loop(#rep{id = {BaseId,_} = Id, options = Options} = Rep) ->
    case async_replicate(Rep) of
    {ok, _Pid} ->
        case get_value(continuous, Options, false) of
        true ->
            {ok, {continuous, ?l2b(BaseId)}};
        false ->
            wait_for_result(Id)
        end;
    Error ->
        Error
    end.


async_replicate(#rep{id = {BaseId, Ext}, source = Src, target = Tgt} = Rep) ->
    RepChildId = BaseId ++ Ext,
    Source = couch_api_wrap:db_uri(Src),
    Target = couch_api_wrap:db_uri(Tgt),
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
        ?LOG_INFO("starting new replication `~s` at ~p (`~s` -> `~s`)",
            [RepChildId, Pid, Source, Target]),
        {ok, Pid};
    {error, already_present} ->
        case supervisor:restart_child(couch_rep_sup, RepChildId) of
        {ok, Pid} ->
            ?LOG_INFO("restarting replication `~s` at ~p (`~s` -> `~s`)",
                [RepChildId, Pid, Source, Target]),
            {ok, Pid};
        {error, running} ->
            %% this error occurs if multiple replicators are racing
            %% each other to start and somebody else won. Just grab
            %% the Pid by calling start_child again.
            {error, {already_started, Pid}} =
                supervisor:start_child(couch_rep_sup, ChildSpec),
            ?LOG_DEBUG("replication `~s` already running at ~p (`~s` -> `~s`)",
                [RepChildId, Pid, Source, Target]),
            {ok, Pid};
        {error, _} = Error ->
            Error
        end;
    {error, {already_started, Pid}} ->
        ?LOG_DEBUG("replication `~s` already running at ~p (`~s` -> `~s`)",
            [RepChildId, Pid, Source, Target]),
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
    receive
    {finished, RepId, RepResult} ->
        {ok, RepResult};
    {error, RepId, Reason} ->
        {error, Reason}
    end.


end_replication({BaseId, Extension}) ->
    FullRepId = BaseId ++ Extension,
    case supervisor:terminate_child(couch_rep_sup, FullRepId) of
    ok ->
        ok = supervisor:delete_child(couch_rep_sup, FullRepId),
        {ok, {cancelled, ?l2b(BaseId)}};
    Error ->
        Error
    end.


init(InitArgs) ->
    try
        do_init(InitArgs)
    catch
    throw:{unauthorized, DbUri} ->
        {stop, {unauthorized,
            <<"unauthorized to access database ", DbUri/binary>>}};
    throw:{db_not_found, DbUri} ->
        {stop, {db_not_found, <<"could not open ", DbUri/binary>>}};
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

    {RevFindersCount, CopiersCount} = case list_to_integer(
        couch_config:get("replicator", "worker_processes", "10")) of
    Small when Small < 2 ->
        ?LOG_ERROR("The number of worker processes for the replicator "
            "should be at least 2", []),
        {1, 1};
    N ->
        {N div 2, N div 2 + N rem 2}
    end,
    BatchSize = list_to_integer(
        couch_config:get("replicator", "worker_batch_size", "500")),
    {ok, MissingRevsQueue} = couch_work_queue:new([
        {multi_workers, true},
        {max_items, trunc(BatchSize * CopiersCount * 1.10)}
    ]),
    {ok, ChangesQueue} = couch_work_queue:new([
        {multi_workers, true},
        {max_items, trunc(BatchSize * RevFindersCount * 1.10)}
    ]),
    % This starts the _changes reader process. It adds the changes from
    % the source db to the ChangesQueue.
    ChangesReader = spawn_changes_reader(
        StartSeq, Source, ChangesQueue, Options),
    % This starts the missing rev finders. They check the target for changes
    % in the ChangesQueue to see if they exist on the target or not. If not,
    % adds them to MissingRevsQueue.
    MissingRevFinders = couch_replicator_rev_finders:spawn_missing_rev_finders(
        Target, ChangesQueue, MissingRevsQueue, RevFindersCount, BatchSize),
    % This starts the doc copy processes. They fetch documents from the
    % MissingRevsQueue and copy them from the source to the target database.
    Workers = couch_replicator_doc_copiers:spawn_doc_copiers(
        self(), Source, Target, MissingRevsQueue, CopiersCount),

    maybe_set_triggered(Rep),

    {ok, State#rep_state{
            missing_revs_queue = MissingRevsQueue,
            changes_queue = ChangesQueue,
            changes_reader = ChangesReader,
            missing_rev_finders = MissingRevFinders,
            workers = Workers,
            worker_steps = dict:from_list([{Pid, 1} || Pid <- Workers])
        }
    }.


handle_info({'DOWN', Ref, _, _, Why}, #rep_state{source_monitor = Ref} = St) ->
    ?LOG_ERROR("Source database is down. Reason: ~p", [Why]),
    {stop, source_db_down, St};

handle_info({'DOWN', Ref, _, _, Why}, #rep_state{target_monitor = Ref} = St) ->
    ?LOG_ERROR("Target database is down. Reason: ~p", [Why]),
    {stop, target_db_down, St};

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
        workers = Workers,
        missing_rev_finders = RevFinders,
        missing_revs_queue = RevsQueue
    } = State,
    case lists:member(Pid, RevFinders) of
    false ->
        case lists:member(Pid, Workers) of
        false ->
            {stop, {unknown_process_died, Pid, normal}, State};
        true ->
            case Workers -- [Pid] of
            [] ->
                {stop, normal, do_last_checkpoint(State)};
            Workers2 ->
                {noreply, State#rep_state{workers = Workers2}}
            end
        end;
    true ->
        case RevFinders -- [Pid] of
        [] ->
            couch_work_queue:close(RevsQueue),
            {noreply, State#rep_state{missing_rev_finders = []}};
        RevFinders2 ->
            {noreply, State#rep_state{missing_rev_finders = RevFinders2}}
        end
    end;

handle_info({'EXIT', Pid, Reason}, State) ->
    #rep_state{
        workers = Workers,
        missing_rev_finders = RevFinders
    } = State,
    State2 = cancel_timer(State),
    case lists:member(Pid, Workers) of
    false ->
        case lists:member(Pid, RevFinders) of
        false ->
            {stop, {unknown_process_died, Pid, Reason}, State2};
        true ->
            ?LOG_ERROR("RevsFinder ~p died with reason: ~p", [Pid, Reason]),
            {stop, {revs_finder_died, Pid, Reason}, State2}
        end;
    true ->
        ?LOG_ERROR("DocCopier ~p died with reason: ~p", [Pid, Reason]),
        {stop, {doc_copier_died, Pid, Reason}, State2}
    end.


handle_call(Msg, _From, State) ->
    ?LOG_ERROR("Replicator received an unexpected synchronous call: ~p", [Msg]),
    {stop, unexpected_sync_message, State}.


handle_cast(reopen_source_db, #rep_state{source = Source} = State) ->
    {ok, NewSource} = couch_db:reopen(Source),
    {noreply, State#rep_state{source = NewSource}};

handle_cast(reopen_target_db, #rep_state{target = Target} = State) ->
    {ok, NewTarget} = couch_db:reopen(Target),
    {noreply, State#rep_state{target = NewTarget}};

handle_cast(checkpoint, State) ->
    State2 = do_checkpoint(State),
    {noreply, State2#rep_state{timer = start_timer(State)}};

handle_cast({seq_done, Seq, StatsInc, Pid},
    #rep_state{stats = Stats, done_seqs = DoneSeqs, worker_steps = Steps,
        workers = Workers, current_through_seq = ThroughSeq} = State) ->
    Step = dict:fetch(Pid, Steps),
    DoneSeqs2 = case dict:find(Pid, DoneSeqs) of
    error ->
        dict:store(Pid, [{Step, Seq}], DoneSeqs);
    {ok, WorkerSeqs} ->
        dict:store(Pid, WorkerSeqs ++ [{Step, Seq}], DoneSeqs)
    end,
    {NewThroughSeq, NewDoneSeqs} = next_checkpoint_seq(
        ThroughSeq, DoneSeqs2, Workers),
    NewState = State#rep_state{
        stats = sum_stats([Stats, StatsInc]),
        done_seqs = NewDoneSeqs,
        current_through_seq = NewThroughSeq,
        worker_steps = dict:store(Pid, Step + 1, Steps)
    },
    {noreply, NewState};

handle_cast({add_stats, StatsInc}, #rep_state{stats = Stats} = State) ->
    {noreply, State#rep_state{stats = sum_stats([Stats, StatsInc])}};

handle_cast(Msg, State) ->
    ?LOG_ERROR("Replicator received an unexpected asynchronous call: ~p", [Msg]),
    {stop, unexpected_async_message, State}.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


terminate(normal, #rep_state{rep_details = #rep{id = RepId, doc = RepDoc},
    checkpoint_history = CheckpointHistory} = State) ->
    terminate_cleanup(State),
    update_rep_doc(RepDoc, [{<<"_replication_state">>, <<"completed">>}]),
    couch_replication_notifier:notify({finished, RepId, CheckpointHistory});

terminate(shutdown, State) ->
    % cancelled replication throught ?MODULE:end_replication/1
    terminate_cleanup(State);

terminate(Reason, #rep_state{rep_details = Rep} = State) ->
    terminate_cleanup(State),
    update_rep_doc(Rep#rep.doc, [{<<"_replication_state">>, <<"completed">>}]),
    couch_replication_notifier:notify({error, Rep#rep.id, Reason}).


terminate_cleanup(State) ->
    stop_db_update_notifier(State#rep_state.source_db_update_notifier),
    stop_db_update_notifier(State#rep_state.target_db_update_notifier),
    couch_api_wrap:db_close(State#rep_state.source),
    couch_api_wrap:db_close(State#rep_state.target).


do_last_checkpoint(#rep_state{done_seqs = DoneSeqs,
        current_through_seq = ThroughSeq} = State) ->
    State2 = do_checkpoint(State#rep_state{
        current_through_seq = last_checkpoint_seq(DoneSeqs, ThroughSeq)}),
    cancel_timer(State2).


start_timer(State) ->
    After = checkpoint_interval(State),
    case timer:apply_after(After, gen_server, cast, [self(), checkpoint]) of
    {ok, Ref} ->
        Ref;
    Error ->
        ?LOG_ERROR("Replicator, error scheduling checkpoint:  ~p", [Error]),
        nil
    end.


cancel_timer(#rep_state{timer = nil} = State) ->
    State;
cancel_timer(#rep_state{timer = Timer} = State) ->
    {ok, cancel} = timer:cancel(Timer),
    State#rep_state{timer = nil}.


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
        tgt_starttime = get_value(<<"instance_start_time">>, TargetInfo),
        session_id = couch_uuids:random(),
        source_db_update_notifier = source_db_update_notifier(Source),
        target_db_update_notifier = target_db_update_notifier(Target),
        source_monitor = db_monitor(Source),
        target_monitor = db_monitor(Target)
    },
    State#rep_state{timer = start_timer(State)}.


spawn_changes_reader(StartSeq, Source, ChangesQueue, Options) ->
    spawn_link(
        fun()->
            couch_api_wrap:changes_since(Source, all_docs, StartSeq,
                fun(DocInfo) ->
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
        stats = Stats,
        rep_details = #rep{options = Options},
        session_id = SessionId
    } = State,
    case commit_to_both(Source, Target) of
    {SrcInstanceStartTime, TgtInstanceStartTime} ->
        ?LOG_INFO("recording a checkpoint for `~s` -> `~s` at source update_seq ~p",
            [SourceName, TargetName, NewSeq]),
        StartTime = ?l2b(ReplicationStartTime),
        EndTime = ?l2b(httpd_util:rfc1123_date()),
        NewHistoryEntry = {[
            {<<"session_id">>, SessionId},
            {<<"start_time">>, StartTime},
            {<<"end_time">>, EndTime},
            {<<"start_last_seq">>, StartSeq},
            {<<"end_last_seq">>, NewSeq},
            {<<"recorded_seq">>, NewSeq},
            {<<"missing_checked">>, Stats#rep_stats.missing_checked},
            {<<"missing_found">>, Stats#rep_stats.missing_found},
            {<<"docs_read">>, Stats#rep_stats.docs_read},
            {<<"docs_written">>, Stats#rep_stats.docs_written},
            {<<"doc_write_failures">>, Stats#rep_stats.doc_write_failures}
        ]},
        BaseHistory = [
            {<<"session_id">>, SessionId},
            {<<"source_last_seq">>, NewSeq}
        ] ++ case get_value(doc_ids, Options) of
        undefined ->
            [];
        _DocIds ->
            % backwards compatibility with the result of a replication by
            % doc IDs in versions 0.11.x and 1.0.x
            % TODO: deprecate (use same history format, simplify code)
            [
                {<<"start_time">>, StartTime},
                {<<"end_time">>, EndTime},
                {<<"docs_read">>, Stats#rep_stats.docs_read},
                {<<"docs_written">>, Stats#rep_stats.docs_written},
                {<<"doc_write_failures">>, Stats#rep_stats.doc_write_failures}
            ]
        end,
        % limit history to 50 entries
        NewRepHistory = {
            BaseHistory ++
            [{<<"history">>, lists:sublist([NewHistoryEntry | OldHistory], 50)}]
        },

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
        OldSeqNum = get_value(<<"source_last_seq">>, RepRecProps, ?LOWEST_SEQ),
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
    {?LOWEST_SEQ, []};
compare_rep_history([{S} | SourceRest], [{T} | TargetRest] = Target) ->
    SourceId = get_value(<<"session_id">>, S),
    case has_session_id(SourceId, Target) of
    true ->
        RecordSeqNum = get_value(<<"recorded_seq">>, S, ?LOWEST_SEQ),
        ?LOG_INFO("found a common replication record with source_seq ~p",
            [RecordSeqNum]),
        {RecordSeqNum, SourceRest};
    false ->
        TargetId = get_value(<<"session_id">>, T),
        case has_session_id(TargetId, SourceRest) of
        true ->
            RecordSeqNum = get_value(<<"recorded_seq">>, T, ?LOWEST_SEQ),
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


next_checkpoint_seq(LastCheckpointSeq, DoneSeqs, Workers) ->
    {LowestSteps, Seqs} = dict:fold(
        fun(_Pid, [], Acc) ->
                Acc;
            (_Pid, [{Step, Seq} | _], {StepAcc, SeqAcc}) ->
                {[Step | StepAcc], [Seq | SeqAcc]}
        end,
        {[], []}, DoneSeqs),
    case {length(LowestSteps) >= length(Workers), lists:usort(LowestSteps)} of
    {true, [_]} ->
        CheckpointSeq = lists:max([LastCheckpointSeq | Seqs]),
        DoneSeqs2 = dict:map(
            fun(_Pid, []) ->
                    [];
                (_Pid, [_ | Rest]) ->
                    Rest
            end,
            DoneSeqs),
        {CheckpointSeq, DoneSeqs2};
    _ ->
        {LastCheckpointSeq, DoneSeqs}
    end.


last_checkpoint_seq(DoneSeqs, DefaultSeq) ->
    MaxSeqs = dict:fold(
        fun(_Pid, [], Acc) ->
                Acc;
             (_Pid, SeqList, Acc) ->
                {_, Seq} = lists:last(SeqList),
                [Seq | Acc]
        end,
        [], DoneSeqs),
    lists:max([DefaultSeq | MaxSeqs]).


sum_stats([Stats1 | RestStats]) ->
    lists:foldl(
        fun(Stats, Acc) ->
            #rep_stats{
                missing_checked = Stats#rep_stats.missing_checked +
                    Acc#rep_stats.missing_checked,
                missing_found = Stats#rep_stats.missing_found +
                    Acc#rep_stats.missing_found,
                docs_read = Stats#rep_stats.docs_read + Acc#rep_stats.docs_read,
                docs_written = Stats#rep_stats.docs_written +
                    Acc#rep_stats.docs_written,
                doc_write_failures = Stats#rep_stats.doc_write_failures +
                    Acc#rep_stats.doc_write_failures
            }
        end,
        Stats1, RestStats).


source_db_update_notifier(#db{name = DbName}) ->
    Server = self(),
    {ok, Notifier} = couch_db_update_notifier:start_link(
        fun({compacted, DbName1}) when DbName1 =:= DbName ->
                ok = gen_server:cast(Server, reopen_source_db);
            (_) ->
                ok
        end),
    Notifier;
source_db_update_notifier(_) ->
    nil.

target_db_update_notifier(#db{name = DbName}) ->
    Server = self(),
    {ok, Notifier} = couch_db_update_notifier:start_link(
        fun({compacted, DbName1}) when DbName1 =:= DbName ->
                ok = gen_server:cast(Server, reopen_target_db);
            (_) ->
                ok
        end),
    Notifier;
target_db_update_notifier(_) ->
    nil.


stop_db_update_notifier(nil) ->
    ok;
stop_db_update_notifier(Notifier) ->
    couch_db_update_notifier:stop(Notifier).


maybe_set_triggered(#rep{id = {BaseId, _}, doc = {RepProps} = RepDoc}) ->
    case get_value(<<"_replication_state">>, RepProps) of
    <<"triggered">> ->
        ok;
    _ ->
        update_rep_doc(
            RepDoc,
            [
                {<<"_replication_state">>, <<"triggered">>},
                {<<"_replication_id">>, ?l2b(BaseId)}
            ])
    end.


db_monitor(#db{} = Db) ->
    couch_db:monitor(Db);
db_monitor(_HttpDb) ->
    nil.
