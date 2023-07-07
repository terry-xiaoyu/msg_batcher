-module(msg_batcher_tests).

-include_lib("eunit/include/eunit.hrl").

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
