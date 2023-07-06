%%%-------------------------------------------------------------------
%% @doc msg_batcher top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(msg_batcher_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_supervised() ->
    supervisor:start_child(?SERVER, #{
        id => {ets_batcher, Id},
        start => {emqx_rule_actions_ets_batcher, start_link, [Id, BatchSize, BatchTime, Callback, Opts]},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_rule_actions_ets_batcher]
    }).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
