-module(dht_server).
-behaviour(gen_server).

% Client API:
-export([start_link/0, store/1, fetch/1]).

% Server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

% Solve the peer discovery problem when joining the ring by bootstrapping from a known node.
% You may need to change the node bootstrap@localhost to something else for your computer.
-define(BOOTSTRAP_NODE, bootstrap@localhost).
-define(BOOTSTRAP, {dht_server, ?BOOTSTRAP_NODE}).

%---------------------------------------------------------------------------------------------
% Client API
%---------------------------------------------------------------------------------------------
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

store(Value) ->
    Key = dht_util:hash(Value),
    store(Key, Value).

% Get a value from the DHT by its key, regardless of which node is holding the value.
fetch(Key) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:call(ServerPid, {fetch, Key}).

%---------------------------------------------------------------------------------------------
% Internal API for talking to the servers
%---------------------------------------------------------------------------------------------
join() -> gen_server:cast(?MODULE, join).

set_next(Pid, Id) -> gen_server:cast(Pid, {set_next, Id, self()}).

% Consistent hashing in a DHT means we hash before sending the query to the server.
% In fact, the server will be chosen based on the Key hash. Having store/2 also allows
% for easy unit testing.
store(Key, Value) ->
    {_ServerId, ServerPid} = key_lookup(Key),
    gen_server:cast(ServerPid, {store, Key, Value}),
    Key.

% Locate the {ServerId, ServerPid} responsible for storing the given Key hash.
key_lookup(Key) ->
    gen_server:cast(?MODULE, {key_lookup, Key, self()}),
    await_reply().

% Find the {Id, Pid} details for the next and previous servers of a given Id hash.
find_neighbours(Id) ->
    % When joining the ring, we've got a peer discovery problem. Contact a known node.
    gen_server:cast(?BOOTSTRAP, {find_neighbours, Id, self()}),
    await_reply().

%---------------------------------------------------------------------------------------------
% Server implementation
%---------------------------------------------------------------------------------------------
init([]) ->
    join_if_not_bootstrap_node(),
    Id = dht_util:node_id(),
    io:format("~n~n*** STARTING NODE ~p ***~n~n", [Id]),
    State = {dict:new(), Id, {Id, self()}},
    {ok, State}.

handle_cast({store, Key, Value}, {Dict, Id, Next}) ->
    NewDict = dict:store(Key, Value, Dict),
    {noreply, {NewDict, Id, Next}};
handle_cast(join, {Dict, Id, _Next}) ->
    %Figure out who are my neighbors and adjust the ring to insert myself in between them.
    {_PrevId, PrevPid, NextId, NextPid} = find_neighbours(Id),
    set_next(PrevPid, Id), %Tell previous node to point at me
    {noreply, {Dict, Id, {NextId, NextPid}}}; %I'll now point at previous node's old Next node.
handle_cast({set_next, NextId, NextPid}, {Dict, Id, _Next}) -> {noreply, {Dict, Id, {NextId, NextPid}}};
handle_cast({key_lookup, Key, RequestingPid}, State = {_Dict, Id, Next}) ->
    handle_key_lookup(RequestingPid, Key, Id, Next),
    {noreply, State};
handle_cast({find_neighbours, HashId, RequestingPid}, State = {_Dict, Id, Next}) ->
    handle_find_neighbours(RequestingPid, HashId, Id, Next),
    {noreply, State}.

handle_call({fetch, Key}, _From, State = {Dict, _, _}) ->
    Reply = case dict:find(Key, Dict) of
        error -> {not_found, Key};
        Result -> Result
    end,
    {reply, Reply, State}.

%---------------------------------------------------------------------------------------------
% Unused gen_server callbacks
%---------------------------------------------------------------------------------------------
handle_info(_Any, State) -> {noreply, State}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%---------------------------------------------------------------------------------------------
% Helpers and utilities
%---------------------------------------------------------------------------------------------
join_if_not_bootstrap_node() -> join_if_not_bootstrap_node(node).
join_if_not_bootstrap_node(Node) when Node == ?BOOTSTRAP_NODE -> ok;
join_if_not_bootstrap_node(Node) when Node =/= ?BOOTSTRAP_NODE -> join().

% This allows us to get a response from a different server than the one we sent the request to!
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
        false -> gen_server:cast(NextPid, {find_neighbours, HashId, From})
    end;
% Handle all the other nodes that do not wrap around.
handle_find_neighbours(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {ServerId, self(), NextId, NextPid};
        false -> gen_server:cast(NextPid, {find_neighbours, HashId, From})
    end.

% The hash is the same as the server ID, or there is only one server in the ring.
handle_key_lookup(From, HashId, ServerId, {NextId, _NextPid}) 
    when HashId == ServerId; ServerId == NextId ->
    From ! {ServerId, self()};
% Handle wrap-around of the ring...
handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) when NextId < ServerId ->
    case (HashId > ServerId orelse HashId =< NextId) of
        true -> From ! {NextId, NextPid};
        false -> gen_server:cast(NextPid, {key_lookup, HashId, From})
    end;
handle_key_lookup(From, HashId, ServerId, {NextId, NextPid}) ->
    case (HashId > ServerId andalso HashId < NextId) of
        true -> From ! {NextId, NextPid};
        false -> gen_server:cast(NextPid, {key_lookup, HashId, From})
    end.