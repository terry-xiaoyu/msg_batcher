-module(msg_batcher_object).

-export([ put/2
        , get/1
        , delete/1
        ]).

%% =============================================================================
%% Batcher objects
%% =============================================================================

put(Id, Handler) ->
    persistent_term:put({?MODULE, Id}, Handler).
get(Id) ->
    persistent_term:get({?MODULE, Id}, not_found).
delete(Id) ->
    persistent_term:erase({?MODULE, Id}).
