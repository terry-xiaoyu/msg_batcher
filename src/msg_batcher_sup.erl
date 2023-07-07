%%%-------------------------------------------------------------------
%% @doc msg_batcher top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(msg_batcher_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([ start_supervised_simple/3
        , start_supervised/5
        , stop_supervised/1
        ]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_supervised_simple(Id, {_Mod, _Func, _Args} = Callback, BatcherOpts) ->
    start_supervised(Id, undefined, [], [],
        BatcherOpts#{
            batch_callback => Callback
        });
start_supervised_simple(Id, {Mod, Func, Args, InitState}, BatcherOpts) ->
    start_supervised(Id, undefined, [], [],
        BatcherOpts#{
            batch_callback => {Mod, Func, Args},
            batch_callback_state => InitState
        }).

start_supervised(Id, Module, InitArgs, Options, BatcherOpts) ->
    BatcherMod = msg_batcher_proc,
    MFA = {BatcherMod, start_link, [Id, Module, InitArgs, Options, BatcherOpts]},
    supervisor:start_child(?SERVER, #{
        id => Id,
        start => MFA,
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [BatcherMod]
    }).

stop_supervised(Id) ->
    case supervisor:terminate_child(?SERVER, Id) of
        ok ->
            case supervisor:delete_child(?SERVER, Id) of
                ok -> ok;
                {error, not_found} -> ok
            end;
        {error, not_found} -> ok;
        {error, _} = Err -> Err
    end.

init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
