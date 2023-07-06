-module(msg_batcher).

-export([ start_link/2
        , start_supervised/2
        ]).

-type server_name() :: atom().

-type start_opt() :: {'timeout', timeout()}
                   | {'debug', [debug_flag()]}
                   | {'hibernate_after', timeout()}
                   | {'spawn_opt', [proc_lib:spawn_option()]}.

-type batcher_opt() :: #{batch_size := pos_integer(),
                         batch_time := pos_integer(),
                         callback => {module(), atom(), [term()]},
                         opts => #{
                            drop_factor => pos_integer(),
                            sender_punish_time => pos_integer()
                         }}.
-spec start_link(ServerName :: server_name(),
                 Module :: module(),
                 Args :: term(),
                 Options :: [start_opt()],
                 Type :: ets,
                 BatcherOpts :: batcher_opt()) -> {ok, pid()} | ignore | {error, term()}.

start_link(ServerName, Module, Args, Options, ets, BatcherOpts) ->
    msg_batcher_ets:start_link(ServerName, Module, Args, Options, BatcherOpts).

start_supervised() ->
    .