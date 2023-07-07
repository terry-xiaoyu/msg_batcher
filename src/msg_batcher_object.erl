-module(msg_batcher_object).

-export([ put/2
        , get/1
        , delete/1
        ]).

-define(KEY(NAME), {?MODULE, get_pid(NAME)}).

%% =============================================================================
%% Batcher objects
%% =============================================================================

put(Pid, Handler) when is_pid(Pid) ->
    persistent_term:put(?KEY(Pid), Handler).
get(Name) ->
    persistent_term:get(?KEY(Name), not_found).
delete(Name) ->
    persistent_term:erase(?KEY(Name)).

get_pid(Name) when is_atom(Name) ->
    whereis(Name);
get_pid(Pid) when is_pid(Pid) ->
    Pid.
