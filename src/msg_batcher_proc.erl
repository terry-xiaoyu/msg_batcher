%% This module implements a msg queue using an ordered_set ets table.
%% The table is flushed periodically, or when the number of msgs in the table
%% reaches a threshold.
%%         (msgs)         (flush)
%% clients ========> ETS =========> callback([msgs]).
-module(msg_batcher_proc).

-behaviour(gen_server).

%% API
-export([start_link/4, start_link/5, stop/1]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2,
         handle_continue/2
        ]).

-export([enqueue/2]).

-define(DROP_FACTOR, 10).
-define(INFREQUENT_INTERVAL, 1000).
-define(FORCE_FLUSH, '$force_flush').
-define(TIMER_FLUSH, '$timer_flush').

-define(CINDEX_QUEUE_LEN, 1).
-define(CINDEX_TOTAL_DROPPED, 2).
-define(CINDEX_TOTAL_FLUSH, 3).
-define(CINDEX_LAST_FLUSH, 4).
-define(CINDEX_MAX, 8).

-define(IF_EXPORTED(MOD, FUN, ARITY, EXPR_TURE, EXPR_FALSE),
    case erlang:function_exported(Mod, FUN, ARITY) of
        true -> EXPR_TURE;
        _ -> EXPR_FALSE
    end).

start_link(BhvMod, InitArgs, GenOpts, Opts) ->
    TabName = maps:get(tab_name, Opts, ?MODULE),
    gen_server:start_link(?MODULE, {TabName, BhvMod, InitArgs, Opts}, GenOpts).

start_link(Name, BhvMod, InitArgs, GenOpts, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, {Name, BhvMod, InitArgs, Opts}, GenOpts).

stop(Name) ->
    gen_server:stop(Name).

enqueue(Name, Msg) ->
    case msg_batcher_object:get(Name) of
        not_found ->
            {error, batcher_not_found};
        BatchObj ->
            do_enqueue(Name, Msg, BatchObj)
    end.

do_enqueue(Name, Msg, #{batch_size := BatchSize, counter_ref := CRef,
                        tab_ref := Tab, store_format := StoreFormat,
                        drop_factor := DropFactor, sender_punish_time := PunishTime}) ->
    UpperLimit = max(BatchSize * DropFactor, BatchSize + 1),
    case get_queue_size(CRef) + 1 of
        Size when Size < BatchSize ->
            ensure_msg_queued(Tab, Msg, StoreFormat, CRef);
        Size when Size == BatchSize ->
            ensure_msg_queued(Tab, Msg, StoreFormat, CRef),
            Name ! ?FORCE_FLUSH,
            ok;
        Size when Size > BatchSize, Size =< UpperLimit ->
            ensure_msg_queued(Tab, Msg, StoreFormat, CRef),
            %% we have a "no notifiction window" to avoid notifying too frequently
            case Size < max(BatchSize div 2, BatchSize + 1) of
                true -> ok;
                false -> Name ! ?FORCE_FLUSH, ok
            end;
        Size ->
            logger:warning("[ets-batcher] overloaded, dropping msg. current queue length: ~p", [Size]),
            Name ! ?FORCE_FLUSH,
            counters:add(CRef, ?CINDEX_TOTAL_DROPPED, 1),
            maybe_punish_sender(PunishTime, Size),
            {error, overloaded}
    end.

init({TabName, BhvMod, InitArgs, #{
            batch_size := BatchSize,
            batch_time := BatchTime
       } = Opts}) ->
    _ = erlang:process_flag(trap_exit, true),
    Tab = ets:new(TabName, [ordered_set, public, {write_concurrency, true}]),
    CRef = counters:new(?CINDEX_MAX, [write_concurrency]),
    TRef = send_flush_after(BatchTime),
    DropFactor = maps:get(drop_factor, Opts, ?DROP_FACTOR),
    PunishTime = maps:get(sender_punish_time, Opts, donot_punish),
    StoreFormat = maps:get(store_format, Opts, term),
    msg_batcher_object:put(self(), #{
        batch_time => BatchTime,
        batch_size => BatchSize,
        counter_ref => CRef,
        tab_ref => Tab,
        drop_factor => DropFactor,
        sender_punish_time => PunishTime,
        store_format => StoreFormat
    }),
    Data = #{behaviour_module => BhvMod,
             counter_ref => CRef, timer_ref => TRef},
    case BhvMod of
        undefined ->
            {ok, Data#{batch_callback => maps:get(batch_callback, Opts),
                       batch_callback_state => maps:get(batch_callback_state, Opts, no_state)}};
        _ ->
            handle_return(BhvMod:init(InitArgs),
                Data#{batch_callback => {BhvMod, handle_batch, []}})
    end.

handle_call(_Request, _From, #{behaviour_module := undefined} = Data) ->
    logger:error("[ets-batcher] Unknown call: ~p", [_Request]),
    {reply, ok, Data};
handle_call(Request, From, #{behaviour_module := Mod, batch_callback_state := CallbackState} = Data) ->
    ?IF_EXPORTED(Mod, handle_call, 3,
        handle_return(Mod:handle_call(Request, From, CallbackState), Data), {reply, ok, Data}).

handle_cast(_Msg, #{behaviour_module := undefined} = Data) ->
    logger:error("[ets-batcher] Unknown cast: ~p", [_Msg]),
    {noreply, Data};
handle_cast(_Msg, #{behaviour_module := Mod, batch_callback_state := CallbackState} = Data) ->
    ?IF_EXPORTED(Mod, handle_cast, 2,
        handle_return(Mod:handle_cast(_Msg, CallbackState), Data), {noreply, Data}).

handle_info(?FORCE_FLUSH, #{timer_ref := TRef} = Data0) ->
    #{batch_size := BatchSize, batch_time := BatchTime, drop_factor := DropFactor}
        = BatcherObj = msg_batcher_object:get(self()),
    _ = erlang:cancel_timer(TRef),
    clean_mailbox(?FORCE_FLUSH, BatchSize * DropFactor),
    Data = flush(BatcherObj, Data0),
    {noreply, Data#{timer_ref => send_flush_after(BatchTime)}};
handle_info(?TIMER_FLUSH, Data0) ->
    #{batch_time := BatchTime, counter_ref := CRef} = BatcherObj = msg_batcher_object:get(self()),
    Data = case get_queue_size(CRef) of
        0 -> Data0;
        _ -> flush(BatcherObj, Data0)
    end,
    CheckTime = suitable_periodical_check_time(Data, BatchTime),
    {noreply, Data#{timer_ref => send_flush_after(CheckTime)}};

handle_info(Info, #{behaviour_module := undefined} = Data) ->
    logger:error("[ets-batcher] Unknown message: ~p", [Info]),
    {noreply, Data};

handle_info(Info, #{behaviour_module := Mod, batch_callback_state := CallbackState} = Data) ->
    ?IF_EXPORTED(Mod, handle_info, 2,
        handle_return(Mod:handle_info(Info, CallbackState), Data), {noreply, Data}).

terminate(Reason, #{behaviour_module := Mod, batch_callback_state := CallbackState}) ->
    %% TODO: we need a monitor process to won the ETS table, and flush the msgs
    %%   inserted after the batcher_proc is terminated.
    msg_batcher_object:delete(self()),
    ?IF_EXPORTED(Mod, terminate, 2,
        Mod:terminate(Reason, CallbackState), ok).

code_change(OldVsn, #{behaviour_module := Mod, batch_callback_state := CallbackState} = Data, Extra) ->
    ?IF_EXPORTED(Mod, code_change, 3,
        handle_return(Mod:code_change(OldVsn, CallbackState, Extra), Data), {ok, Data}).

handle_continue(_Info, #{behaviour_module := undefined} = Data) ->
    {noreply, Data};
handle_continue(Info, #{behaviour_module := Mod, batch_callback_state := CallbackState} = Data) ->
    ?IF_EXPORTED(Mod, handle_continue, 2,
        handle_return(Mod:handle_continue(Info, CallbackState), Data), {noreply, Data}).

format_status(Opt, [_PDict, #{behaviour_module := undefined} = Data]) ->
    case Opt of
        terminate -> Data;
        _ -> [{data, [{"State", Data}]}]
    end;
format_status(Opt, [PDict, #{behaviour_module := Mod, batch_callback_state := CallbackState} = Data]) ->
    DefStatus = case Opt of
            terminate -> Data;
            _ -> [{data, [{"State", Data}]}]
        end,
    ?IF_EXPORTED(Mod, format_status, 2,
            case catch Mod:format_status(Opt, [PDict, CallbackState]) of
                {'EXIT', _} -> DefStatus;
                Else -> Else
            end, DefStatus).

%% =============================================================================
%% Call the behavior implementation module
%% =============================================================================
handle_return(ignore, _Data) ->
    ignore;
handle_return({ok, NState}, Data) ->
    {ok, Data#{batch_callback_state => NState}};
handle_return({ok, NState, Any}, Data) ->
    {ok, Data#{batch_callback_state => NState}, Any};
handle_return({reply, Reply, NState}, Data) ->
    {reply, Reply, Data#{batch_callback_state => NState}};
handle_return({reply, Reply, NState, Any}, Data) ->
    {reply, Reply, Data#{batch_callback_state => NState}, Any};
handle_return({noreply, NState}, Data) ->
    {noreply, Data#{batch_callback_state => NState}};
handle_return({noreply, NState, Any}, Data) ->
    {noreply, Data#{batch_callback_state => NState}, Any};
handle_return({stop, Reason, Reply, NState}, Data) ->
    {stop, Reason, Reply, Data#{batch_callback_state => NState}};
handle_return({stop, Reason, NState}, Data) ->
    {stop, Reason, Data#{batch_callback_state => NState}};
handle_return({continue, NState}, Data) ->
    {continue, Data#{batch_callback_state => NState}};
handle_return({error, Reason}, _Data) ->
    {error, Reason}.

%% =============================================================================
%% Internal functions
%% =============================================================================

ensure_msg_queued(Tab, Msg, StoreFormat, CRef) ->
    case ets:insert_new(Tab, buffer_record(Msg, StoreFormat)) of
        true -> incr_queue_size(CRef);
        false ->
            %% the msg_ts() does not garantee an unique timestamp if called in parallel
            ensure_msg_queued(Tab, Msg, StoreFormat, CRef)
    end.

buffer_record(Msg, term) ->
    {msg_ts(), Msg};
buffer_record(Msg, binary) ->
    {msg_ts(), erlang:term_to_binary(Msg)}.

maybe_decode_msg(Msg, term) ->
    Msg;
maybe_decode_msg(Msg, binary) ->
    erlang:binary_to_term(Msg).

clean_mailbox(_Msg, 0) ->
    ok;
clean_mailbox(Msg, MaxCnt) ->
    receive Msg -> clean_mailbox(Msg, MaxCnt - 1)
    after 0 -> ok
    end.

send_flush_after(BatchTime) ->
    erlang:send_after(BatchTime, self(), ?TIMER_FLUSH).

maybe_punish_sender(donot_punish, _) ->
    ok;
maybe_punish_sender(PunishTime, Size) ->
    %% now the batcher got overloaded, we punish the caller by sleeping for a while
    logger:warning("[ets-batcher] overloaded, current queue length: ~p, the sender process is punished to sleep ~p ms", [Size, PunishTime]),
    timer:sleep(PunishTime).

flush(#{batch_size := BatchSize, tab_ref := Tab, drop_factor := DropFactor,
        store_format := StoreFormat},
      #{batch_callback := Callback, batch_callback_state := CallbackState,
        counter_ref := CRef} = Data) ->
    {FlushCnt, DropCnt, NState} =
        do_flush(Tab, BatchSize, Callback, CallbackState, CRef, DropFactor, StoreFormat),
    counters:put(CRef, ?CINDEX_LAST_FLUSH, FlushCnt),
    counters:add(CRef, ?CINDEX_TOTAL_FLUSH, FlushCnt),
    counters:add(CRef, ?CINDEX_TOTAL_DROPPED, DropCnt),
    Data#{
        batch_callback_state => NState,
        total_flush_cnt => incr_cnt(total_flush_cnt, Data, FlushCnt),
        total_dropped_cnt => incr_cnt(total_dropped_cnt, Data, DropCnt),
        last_n_flush_cnt => incr_last_flush_cnt(Data, FlushCnt)
    }.

do_flush(Tab, BatchSize, Callback, CallbackState, CRef, DropFactor, StoreFormat) ->
    do_flush(Tab, BatchSize, Callback, CallbackState, CRef, DropFactor, StoreFormat, 0, 0).

do_flush(Tab, BatchSize, Callback, CallbackState, CRef, DropFactor, StoreFormat, CntAcc, DropAcc) ->
    case ets:first(Tab) of
        '$end_of_table' ->
            {CntAcc, DropAcc, CallbackState};
        FirstKey ->
            BatchMsgs = take_first_n_msg(Tab, FirstKey, StoreFormat, BatchSize - 1,
                            [fetch_msg(Tab, FirstKey, StoreFormat)]),
            Cnt = length(BatchMsgs),
            decr_queue_size(CRef, Cnt),
            case get_queue_size(CRef) of
                0 ->
                    {CntAcc + Cnt, DropAcc, call_handle_batch(Callback, CallbackState, BatchMsgs)};
                Size when Size < BatchSize * DropFactor ->
                    NState = call_handle_batch(Callback, CallbackState, BatchMsgs),
                    %% we still have some msgs, flush again until the table is empty
                    %% TODO: we need to give the process a chance to handle other msgs
                    do_flush(Tab, BatchSize, Callback, NState, CRef, DropFactor,
                        StoreFormat, CntAcc + Cnt, DropAcc);
                Size when Size >= BatchSize * DropFactor ->
                    %% the batcher got overloaded so the ETS table cannot be flushed in time.
                    %% we now simply drop msgs taken from the table
                    logger:warning("[ets-batcher] overloaded, current queue length: ~p, dropped ~p msgs", [Size, Cnt]),
                    do_flush(Tab, BatchSize, Callback, CallbackState, CRef, DropFactor,
                        StoreFormat, CntAcc, DropAcc + Cnt)
            end
    end.

take_first_n_msg(_Tab, _Key, _, N, MsgAcc) when N =< 0 ->
    lists:reverse(MsgAcc);
take_first_n_msg(Tab, Key, StoreFormat, N, MsgAcc) ->
    case ets:next(Tab, Key) of
        '$end_of_table' ->
            lists:reverse(MsgAcc);
        NextKey ->
            take_first_n_msg(Tab, NextKey, StoreFormat, N - 1, [fetch_msg(Tab, NextKey, StoreFormat) | MsgAcc])
    end.

fetch_msg(Tab, Key, StoreFormat) ->
    %% read value of Key from ets table, and then delete it
    case ets:take(Tab, Key) of
        [] -> throw({key_not_found, Key});
        [{_, Msg}] -> maybe_decode_msg(Msg, StoreFormat)
    end.

call_handle_batch({M, F, A}, no_state, BatchMsgs) ->
    _ = safe_apply(M, F, A ++ [BatchMsgs]),
    no_state;
call_handle_batch({M, F, A}, CallbackState, BatchMsgs) ->
    case safe_apply(M, F, A ++ [BatchMsgs, CallbackState]) of
        ok -> CallbackState;
        {ok, NewState} -> NewState
    end.

safe_apply(M, F, A) ->
    try erlang:apply(M, F, A)
    catch
        Err:Reason:ST ->
            logger:error("[ets-batcher] Error when calling ~p:~p/~p: ~p:~p, stacktrace:~p",
                [M, F, length(A), Err, Reason, ST])
    end.

msg_ts() ->
    erlang:monotonic_time(nanosecond).

incr_queue_size(CRef) ->
    counters:add(CRef, ?CINDEX_QUEUE_LEN, 1).

get_queue_size(CRef) ->
    counters:get(CRef, ?CINDEX_QUEUE_LEN).

decr_queue_size(CRef, Count) ->
    counters:sub(CRef, ?CINDEX_QUEUE_LEN, Count).

suitable_periodical_check_time(Data, BatchTime) when BatchTime < ?INFREQUENT_INTERVAL ->
    %% avoid too frequent flush if the batcher is relatively free
    case is_last_n_flush_zero(Data) of
        true -> ?INFREQUENT_INTERVAL;
        false -> BatchTime
    end;
suitable_periodical_check_time(_, BatchTime) ->
    BatchTime.

incr_cnt(Key, Data, Cnt) ->
    maps:get(Key, Data, 0) + Cnt.

incr_last_flush_cnt(#{last_n_flush_cnt := #{1 := Last1Cnt}}, Cnt) ->
    #{1 => Cnt, 2 => Last1Cnt};
incr_last_flush_cnt(_, Cnt) ->
    #{1 => Cnt}.

is_last_n_flush_zero(#{last_n_flush_cnt := #{1 := 0, 2 := 0}}) ->
    true;
is_last_n_flush_zero(_) ->
    false.
