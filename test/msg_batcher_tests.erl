-module(msg_batcher_tests).

-include_lib("eunit/include/eunit.hrl").

-behaviour(msg_batcher).

%% Callbacks are just the same as gen_server callbacks, other than the handle_batch/2.
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , handle_continue/2
        , handle_batch/2
        , terminate/2
        ]).

simple_batcher_test() ->
    {ok, _} = application:ensure_all_started(msg_batcher),
    BatcherOpts = #{
        batch_size => 100,
        batch_time => 500,
        drop_factor => 100
    },
    Callback = {erlang, send, [self()]},
    Name = abcd,
    {ok, _Pid} = msg_batcher:start_supervised_simple(Name, Callback, BatcherOpts),
    ?assertMatch(#{batch_size := 100, batch_time := 500,
                   drop_factor := 100, sender_punish_time := donot_punish},
        msg_batcher_object:get(Name)),
    lists:foreach(fun(_) ->
            ok = msg_batcher:enqueue(Name, _Msg = <<"hello">>)
        end, lists:seq(1, 2000)),

    RecvBatch = fun Recv(Acc) ->
        receive BatchMsgs ->
            Recv(BatchMsgs ++ Acc)
        after 1000 ->
            Acc
        end
    end,
    BatchMsgs = RecvBatch([]),
    ?assertEqual(2000, length(BatchMsgs)),

    ok = msg_batcher:stop_supervised(Name),
    ?assertEqual(undefined, whereis(Name)),
    ?assertEqual(not_found, msg_batcher_object:get(Name)).

behaviour_batcher_test() ->
    {ok, _} = application:ensure_all_started(msg_batcher),
    BatcherOpts = #{
        batch_size => 100,
        batch_time => 500,
        drop_factor => 100
    },
    {ok, Pid} = msg_batcher:start_link(?MODULE, ?MODULE, {self(), cont}, [], BatcherOpts),
    receive {continued, cont} -> ok after 1000 -> throw(continue_not_called) end,

    ?assertMatch(#{batch_size := 100, batch_time := 500,
                   drop_factor := 100, sender_punish_time := donot_punish},
        msg_batcher_object:get(?MODULE)),

    %% test sending messges
    lists:foreach(fun(_) ->
            ok = msg_batcher:enqueue(?MODULE, _Msg = <<"hello">>)
        end, lists:seq(1, 2000)),
    timer:sleep(1000),
    ?assertMatch(#{batch_callback_state := #{cnt := 2000},
                   behaviour_module := ?MODULE},
        sys:get_state(Pid)),

    %% test gen_server msgs
    ok = gen_server:call(?MODULE, <<"call">>),
    ok = gen_server:cast(?MODULE, <<"cast">>),
    Pid ! <<"info1">>,
    Pid ! <<"info2">>,
    ?assertMatch(#{batch_callback_state := #{
            cnt := 2000,
            got_call := [<<"call">>],
            got_cast := [<<"cast">>],
            got_info := [<<"info2">>, <<"info1">>]
        }}, sys:get_state(Pid)),

    ok = msg_batcher:stop(?MODULE),
    receive {terminated, _} -> ok after 1000 -> throw(terminate_not_called) end,
    ?assertEqual(undefined, whereis(?MODULE)),
    ?assertEqual(not_found, msg_batcher_object:get(?MODULE)).

%%==============================================================================
%% callbacks of msg_batcher
init({Parent, ContineInfo}) ->
    {ok, #{cnt => 0, parent => Parent, got_call => [], got_cast => [], got_info => []},
        {continue, ContineInfo}}.
handle_continue(Info, #{parent := Parent} = State) ->
    Parent ! {continued, Info},
    {noreply, State}.
handle_call(Request, _From, #{got_call := Calls} = State) ->
    {reply, ok, State#{got_call => [Request | Calls]}}.
handle_cast(Msg, #{got_cast := Casts} = State) ->
    {noreply, State#{got_cast => [Msg | Casts]}}.
handle_info(Info, #{got_info := Infos} = State) ->
    {noreply, State#{got_info => [Info | Infos]}}.
handle_batch(BatchMsgs, #{cnt := Cnt} = State) ->
    {ok, State#{cnt => Cnt + length(BatchMsgs)}}.
terminate(Reason, #{parent := Parent}) ->
    Parent ! {terminated, Reason}.
%%==============================================================================
