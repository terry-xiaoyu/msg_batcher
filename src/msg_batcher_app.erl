%%%-------------------------------------------------------------------
%% @doc msg_batcher public API
%% @end
%%%-------------------------------------------------------------------

-module(msg_batcher_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    msg_batcher_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
