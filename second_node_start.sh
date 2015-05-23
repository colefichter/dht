#!/usr/bin/env sh

./rebar compile && erl -pa ebin -s dht_app second_node -sname two@localhlhost -cookie X