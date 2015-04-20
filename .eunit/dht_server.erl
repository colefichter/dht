-module(dht_server).
-behaviour(gen_server).

-export([start_link/0]).


-define(BOOTSTRAP, {dht_server, 'bootstrap'}).

%%%===================================================================
%%% Client API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


store(Value) ->
    Key = dht_util:hash(Value),
    store(Key, Value).

await_reply() ->
    receive
        Reply -> Reply
    after 1000 -> {error, request_timed_out}
    end.

%%%===================================================================
%%% Internal API for talking to the server(s)
%%%===================================================================
% Consistent hashing in a DHT means we hash before sending the query to the server.
% In fact, the server will be chosen based on the Key hash. Having store/2 also allows
% for easy unit testing.
store(Key, Value) ->
    {_ServerId, ServerNode} = key_lookup(Key),
    gen_server:cast({dht_server, ServerNode}, {store, Key, Value}),
    Key.


% Locate the {ServerId, ServerPid} responsible for storing the given Key hash.
key_lookup(Key) ->
    gen_server:cast(?BOOTSTRAP, {key_lookup, Key}),
    await_reply().

% Find the {Id, Pid} details for the next and previous servers of a given Id hash.
find_neighbours(Id) ->
    gen_server:cast(?BOOTSTRAP, {find_neighbours, Id}),
    await_reply().

%%%===================================================================
%%% Server callbacks
%%%===================================================================

init([]) ->
    case node() of
        'bootstrap' -> ok;
        _ -> gen_server:cast(?MODULE, {join})
    end,
    {ok, {dict:new(), dht_util:node_id(), node()}}.