msg_batcher
=====

A `msg_batcher` is a process that accumulates messages in ETS tables and then
handles them in batches.

Sometimes there is too much messaging passing in your system, resulting in very high CPU usage.
At this point, you can use `msg_batcher` to buffer the messages and batch process them.

It can be started as a standalone process or be used as a `gen_server` like `behaviour`.

Build
-----

    $ rebar3 compile

Usage
-----

### Use it as a standalone process

```erlang
%% Start a msg_batcher process
BatcherParams = #{
    batch_size => 100,
    batch_time => 500,
    callbacks => {erlang, display, []},
    opts => #{drop_factor => 10}
},
Handler = msg_batcher:start_supervised(_Name = abcd, BatcherParams).

%% Put messages to the buffer. The bacher will call the _Callback when the buffer
%% is full (_BatchSize reached) or timeout (after _BatchTime milliseconds).
ok = msg_batcher:enqueue(Handler, _Msg = <<"hello">>).
```

### Use it as a `gen_server` like `behaviour`

1. Define a module that implements the `msg_batcher` `behaviour`.

```erlang
-module(my_process).

-behaviour(msg_batcher).

%% API
-export([start_link/1, send_msg/1]).

%% Callbacks are just the same as gen_server callbacks, other than the handle_batch/2.
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_batch/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

start_link(Args) ->
    BatcherParams = #{
        batch_size => 100,
        batch_time => 500,
        opts => #{drop_factor => 10}
    },
    msg_batcher:start_link(_Name = ?MODULE, ?MODULE, Args, [], BatcherParams).

send_msg(Msg) ->
    msg_batcher:enqueue(_Name = ?MODULE, Msg).

init(Args) ->
    {ok, _State = #{foo => bar}}.
handle_call(_Request, _From, State) ->
    {reply, ok, State}.
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_batch(_BatchMsgs, State) ->
    io:format("received msgs in batch: ~p~n", [_BatchMsgs]),
    {ok, State}.
handle_info(_Info, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
terminate(_Reason, _State) ->
    ok.
```

2. "Send" messages to the process (buffer). The messages are actually put to the buffer
   and the process will handle them in batches.

```erlang

{ok, _Pid} = my_process:start_link(_Args = #{batch_size => 10000, batch_time => 30000}).

my_process:send_msg(<<"hello">>).

```
