#!/usr/bin/env sh

./rebar compile && erl -pa ebin -s dht_app first_node -sname bootstrap@localhost -cookie X