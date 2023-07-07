-module(msg_batcher).

-export([ start_link/5
        , stop/1
        , start_supervised_simple/3
        , start_supervised/5
        , stop_supervised/1
        , enqueue/2
        ]).

-type server_name() :: atom().

-type simple_callback() :: {module(), atom(), [term()]}
                         | {module(), atom(), [term()], term()}.

-type debug_flag() :: 'trace' | 'log' | 'statistics' | 'debug'
                    | {'logfile', string()}.

-type start_opt() :: {'timeout', timeout()}
                   | {'debug', [debug_flag()]}
                   | {'hibernate_after', timeout()}
                   | {'spawn_opt', [proc_lib:spawn_option()]}.

-type batcher_opts() :: #{
        batch_size := pos_integer(),
        batch_time := pos_integer(),
        batcher_type => ets,
        batch_callback => {module(), atom(), [term()]},
        %% defaults to ?DROP_FACTOR, set to a large number to "disable" dropping
        drop_factor => 1..10_000_000_000,
        %% defaults to donot_punish
        sender_punish_time => pos_integer() | donot_punish
    }.

%% Following callbacks are just the same as gen_server.
-callback init(Args :: term()) ->
    {ok, State :: term()} | {ok, State :: term(), timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term()} | ignore.
-callback handle_call(Request :: term(), From :: {pid(), Tag :: term()},
                      State :: term()) ->
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), timeout() | hibernate | {continue, term()}} |
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_cast(Request :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_info(Info :: timeout | term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term(), NewState :: term()}.
-callback handle_continue(Info :: term(), State :: term()) ->
    {noreply, NewState :: term()} |
    {noreply, NewState :: term(), timeout() | hibernate | {continue, term()}} |
    {stop, Reason :: term(), NewState :: term()}.
-callback terminate(Reason :: (normal | shutdown | {shutdown, term()} |
                               term()),
                    State :: term()) ->
    term().
-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(),
                      Extra :: term()) ->
    {ok, NewState :: term()} | {error, Reason :: term()}.
-callback format_status(Opt, StatusData) -> Status when
      Opt :: 'normal' | 'terminate',
      StatusData :: [PDict | State],
      PDict :: [{Key :: term(), Value :: term()}],
      State :: term(),
      Status :: term().

%% This is the additional callback to handle batched messages.
-callback handle_batch(BatchedMsgs :: [term()], State :: term()) ->
    ok | {ok, NewState :: term()}.

-optional_callbacks(
    [handle_info/2, handle_continue/2, terminate/2, code_change/3, format_status/2]).

-spec start_link(ServerName :: server_name(),
                 Module :: module(),
                 Args :: term(),
                 Options :: [start_opt()],
                 BatcherOpts :: batcher_opts()) -> {ok, pid()} | ignore | {error, term()}.
start_link(ServerName, Module, Args, Options, BatcherOpts) ->
    msg_batcher_proc:start_link(ServerName, Module, Args, Options, BatcherOpts).

stop(ServerName) ->
    msg_batcher_proc:stop(ServerName).

-spec start_supervised_simple(server_name(), simple_callback(), batcher_opts()) ->
    supervisor:startchild_ret().
start_supervised_simple(ServerName, Callback, BatcherOpts) ->
    msg_batcher_sup:start_supervised_simple(ServerName, Callback, BatcherOpts).

start_supervised(Id, Module, InitArgs, Options, BatcherOpts) ->
    msg_batcher_sup:start_supervised(Id, Module, InitArgs, Options, BatcherOpts).

stop_supervised(Id) ->
    msg_batcher_sup:stop_supervised(Id).

-spec enqueue(server_name(), term()) -> ok.
enqueue(ServerName, Msg) ->
    msg_batcher_proc:enqueue(ServerName, Msg).
