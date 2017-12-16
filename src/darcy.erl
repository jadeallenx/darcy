%% @doc This is the main API for the library.
%%
%% The low level API calls are in `darcy_ddb_api.erl', but generally
%% users shouldn't call those directly.
-module(darcy).
-include("darcy.hrl").

-export([
    start/0,
    make_client/1,
    make_local_client/1
]).

start() ->
    application:ensure_all_started(darcy).

make_client(Region) ->
    do_make_client(Region, <<"https">>, <<"amazonaws.com">>, <<"443">>).

make_local_client(Port) when is_integer(Port) ->
    do_make_client(<<"us-east-1">>, <<"http">>, <<"localhost">>, integer_to_binary(Port));
make_local_client(Port) ->
    do_make_client(<<"us-east-1">>, <<"http">>, <<"localhost">>, Port);

do_make_client(Region, Scheme, Endpoint, Port) ->
    #{ credentials => erliam:credentials(),
       region => Region,
       scheme => Scheme,
       port => Port,
       endpoint => Endpoint
     }.

make_table_spec(AttributeDefs, KeySchema, Throughput, TableName, Extra) ->
    Initial = #{ <<"AttributeDefinitions">> => AttributeDefs,
       <<"KeySchema">> => KeySchema,
       <<"ProvisionedThroughput">> => Throughput,
       <<"TableName">> => TableName
    },
    lists:foldl(fun(M, Acc) -> maps:merge(M, Acc) end, Initial, Extra).

%% @doc Make a table if it doesn't already exist.
make_table_if_not_exists(Client, TableName, TableSpec) ->
    case darcy_ddb_api:describe_table(Client, #{ <<"TableName">> => TableName }) of
        {ok, _Result, {200, _Headers}} -> ok;
        {ok, Error, {400, Headers}} -> attempt_make_table(Client, TableSpec)
    end.


add_data_types(M) when is_map(M) ->
    maps:map(fun(_K, V) -> ddt(V) end, M);
add_data_types(Other) -> erlang:error({error, badarg}, [Other]).

ddt(undefined) -> #{ <<"NULL">> => <<>> };
ddt(V) when is_integer(V) -> #{ <<"N">> => V };
ddt(V) when is_float(V) -> #{ <<"N">> => V };
ddt(V) when is_binary(V) -> #{ <<"S">> => V };
ddt(V) when is_boolean(V) -> #{ <<"BOOL">> => V };
ddt(V) when is_map(V) -> #{ <<"M">> => maps:map(fun(_K, Val) -> ddt(Val) end, V) };
ddt(V) when is_list(V) ->
    try
        #{ <<"S">> => list_to_binary(V) }
    catch
        _:_ ->
             #{ <<"L">> => [ ddt(E) || E <- V ] }
    end;
ddt(Other) -> erlang:error({error, badarg}, [Other]).
