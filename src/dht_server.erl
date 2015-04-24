-module(dht_server).
-behaviour(gen_server).

-export([start_link/0, store/1, fetch/1, key_lookup/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

% Solve the peer discovery problem when joining the ring by bootstrapping from a known node.
% You may need to change the node bootstrap@localhost to something else for your computer.
-define(BOOTSTRAP_NODE, bootstrap@localhost).
-define(BOOTSTRAP, {dht_server, ?BOOTSTRAP_NODE}).

%%%===================================================================
%%% Client API
%%%===================================================================

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

store(Value) ->
    Key = dht_util:hash(Value),
    store(Key, Value).

% Get a value from the DHT by its key, regardless of which node is holding the value.
fetch(Key) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:call(ServerPid, {fetch, Key}).

%%%===================================================================
%%% Internal API for talking to the server(s)
%%%===================================================================

% Consistent hashing in a DHT means we hash before sending the query to the server.
% In fact, the server will be chosen based on the Key hash. Having store/2 also allows
% for easy unit testing.
store(Key, Value) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:cast(ServerPid, {store, Key, Value}),
    Key. % Server reply is not important... we already know the Key.

% Locate the {ServerId, ServerPid} responsible for storing the given Key hash.
key_lookup(Key) ->
    gen_server:cast(?MODULE, {key_lookup, self(), Key}),
    await_reply().

% Find the {Id, Pid} details for the next and previous servers of a given Id hash.
find_neighbours(Id) ->
    % When joining the ring, we've got a peer discovery problem. Contact a known node.
    gen_server:cast(?BOOTSTRAP, {find_neighbours, self(), Id}),
    await_reply().

%%%===================================================================
%%% Server callbacks
%%%===================================================================

init([]) ->
    Id = dht_util:node_id(),
    io:format("~n~n*** STARTING NODE ~p ***~n~n", [Id]),
    case node() of
        ?BOOTSTRAP_NODE -> ok;
        _ -> 
            gen_server:cast(?MODULE, {join})
    end,
    {ok, {dict:new(), Id, {Id, self()}}}.

handle_call({fetch, Key}, _From, State = {Dict, _, _}) ->
    Reply = dict:find(Key, Dict),
    {reply, Reply, State}.

handle_cast({store, Key, Value}, {Dict, Id, Next}) ->
    NewDict = dict:store(Key, Value, Dict),
    {noreply, {NewDict, Id, Next}};
handle_cast({key_lookup, From, Key}, State = {_Dict, Id, Next}) ->
    handle_key_lookup(From, Key, Id, Next),
    {noreply, State};
handle_cast({find_neighbours, From, HashId}, State = {_Dict, Id, Next}) ->
    handle_find_neighbours(From, HashId, Id, Next),
    {noreply, State};
handle_cast({join}, {Dict, Id, _Next}) ->
    %Figure out who are my neighbors and adjust the ring to insert myself in between them.
    {PrevId, PrevPid, NextId, NextPid} = find_neighbours(Id),
    gen_server:cast(PrevPid, {set_next, Id, self()}), %Tell previous node to point at me
    {noreply, {Dict, Id, {NextId, NextPid}}};
handle_cast({set_next, NextId, NextPid}, {Dict, Id, _}) ->
    {noreply, {Dict, Id, {NextId, NextPid}}}.

handle_info(Info, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%===================================================================
%%% Internal helpers
%%%===================================================================
await_reply() ->
    receive
        Reply -> Reply
    after 1000 -> {error, request_timed_out}
    end.

% There is only one node in the ring right now
handle_find_neighbours(From, _HashId, ServerId, {ServerId, ServerPid}) ->
    From ! {ServerId, ServerPid, ServerId, ServerPid};

% Handle wrap-around of the ring...
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId -> 
    case (HashId > ServerId orelse HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> gen_server:cast(NextPid, {find_neighbours, From, HashId})
    end;

% Handle all the other nodes that do not wrap around.
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> gen_server:cast(NextPid, {find_neighbours, From, HashId})
    end.


% The hash is the same as the server ID, or there is only one server in the ring.
handle_key_lookup(From, HashId, ServerId, {NextId, _NextPid}) 
    when HashId == ServerId; ServerId == NextId ->
    From ! {ServerId, self()};

% Handle wrap-around of the ring...
handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId ->
    case (HashId > ServerId orelse HashId =< NextId) of
        true -> From ! {NextId, NextPid};
        false -> gen_server:cast(NextPid, {key_lookup, From, HashId})
    end;

handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {NextId, NextPid};
        false -> gen_server:cast(NextPid, {key_lookup, From, HashId})
    end.