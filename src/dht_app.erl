-module(dht_app).

-behaviour(application).

%% Application callbacks
-export([start/0, start/2, stop/1]).

%Helpers
-export([first_node/0, second_node/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start() -> 
    dht_sup:start_link().

start(_StartType, _StartArgs) ->
    dht_sup:start_link().

stop(_State) ->
    ok.

%% ===================================================================
%% Helpers to get the app running in development mode
%% ===================================================================

first_node() ->
    application:start(dht),
    %sys:trace(dht_server, true),
    Doc = "This is a test document.",
    Key = dht_server:store(Doc),
    %sys:log(dht_server, print),
    {ok, Doc} = dht_server:fetch(Key),
    io:format(" *** KEY = ~p ***~n", [Key]).

second_node() ->
    application:start(dht),
    %sys:trace(dht_server, true),
    timer:sleep(100), % Wait for this node to join the ring
    {ok, "This is a test document."} = dht_server:fetch("cb80455993111c16fd13e70125852aefc911f31e"), 
    dht_server:key_lookup("cb80455993111c16fd13e70125852aefc911f31e").