-module(dht_util).

% API:
-export([distance/2, node_id/0, hash/1]).

% REQUIRES crypto OTP application!

% Generate a random node ID, returned in the form of a hash. With this format
% we can compare document hashes directly to node IDs to locate documents.
node_id() ->
    %The chance of collisions is small with a very large random integer, but
    %we will use the local node name as a salt to add extra protection.
    Salt = atom_to_list(node()),
    ID = random:uniform(99999999999999999999999999999999999999999999999999),
    hash(Salt ++ integer_to_list(ID)).

hash(Data) ->
    Binary160 = crypto:hash(sha, Data),
    hexstring(Binary160).

% Compute the distance between two hashes. Used to compare node ID hashes and document hashes
% when determining the location of a document.
distance(X, Y) when is_list(X), is_list(Y) ->
    %Convert hexstrings to integers for easy distance computations (i.e. subtraction).
    XInt = erlang:list_to_integer(X, 16),
    YInt = erlang:list_to_integer(Y, 16),
    distance(XInt, YInt);
distance(X, Y) when is_integer(X), is_integer(Y) ->
    X - Y.

%%%===================================================================
%%% Internal functions
%%%===================================================================

% Convert a 160-bit binary hash to a hex string.
hexstring(<<X:160/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~40.16.0b", [X])).

%%%===================================================================
%%% Unit Tests
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

node_id_returns_string_test() ->
    ?assert(is_list(node_id())).

hash_returns_string_test() ->
    ?assert(is_list(hash("TESTING TESTING 123!"))).

% The distance between any node and itself must always be zero.
distance_zero_test() ->
    % 160-Bit SHA hash of the string "One":
    X = "b58b5a8ced9db48b30e008b148004c1065ce53b1",
    ?assertEqual(0, distance(X, X)).

% We must always have distance(X, Y) == -1 * distance(Y, X).
distance_symmetry_test() ->
    % 160-Bit SHA hash of the string "One":
    X = "b58b5a8ced9db48b30e008b148004c1065ce53b1",
    % 160-Bit SHA hash of the string "Two":
    Y = "16e018ece5a1d3b750531de58d16b961de23d629",
    ?assertEqual(abs(distance(X, Y)), abs(distance(Y, X))).

% X > Y, so distance(X,Y) must return a positive number.
distance_positive_test() ->
    % 160-Bit SHA hash of the string "One":
    X = "b58b5a8ced9db48b30e008b148004c1065ce53b1",
    % 160-Bit SHA hash of the string "Two":
    Y = "16e018ece5a1d3b750531de58d16b961de23d629",
    ?assert(distance(X, Y) > 0).   

% X > Y, so distance(Y, X) must return a negative number.
distance_negative_test() ->
    % 160-Bit SHA hash of the string "One":
    X = "b58b5a8ced9db48b30e008b148004c1065ce53b1",
    % 160-Bit SHA hash of the string "Two":
    Y = "16e018ece5a1d3b750531de58d16b961de23d629",
    ?assert(distance(Y, X) < 0).