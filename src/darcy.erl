%% @doc This is the main API for the library.
%%
%% The low level API calls are in `darcy_ddb_api.erl', but generally
%% users shouldn't call those directly.
-module(darcy).
-include("darcy.hrl").

-export([
    start/0,
    make_client/1,
    make_local_client/1,
    to_map/1,
    to_ddb/1,
    default_decode/1,
    default_encode/1,
    make_table_spec/5,
    make_table_if_not_exists/3
]).

start() ->
    application:ensure_all_started(darcy).

make_client(Region) ->
    do_make_client(Region, <<"https">>, <<"amazonaws.com">>, <<"443">>).

make_local_client(Port) when is_integer(Port) ->
    do_make_client(<<"us-east-1">>, <<"http">>, <<"localhost">>, integer_to_binary(Port));
make_local_client(Port) ->
    do_make_client(<<"us-east-1">>, <<"http">>, <<"localhost">>, Port).

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
        {ok, _Error, {400, _Headers}} -> attempt_make_table(Client, TableSpec)
    end.

attempt_make_table(_, _) -> ok.

default_decode(Blob) ->
    zlib:uncompress(base64:decode(Blob)).

default_encode(Data) ->
    base64:encode(zlib:compress(Data)).

to_map(M) when is_map(M) ->
    maps:map(fun(_K, V) when is_map(V) -> unddt(V);
                (_K, V) -> V end,
             M).

unddt(#{ <<"B">> := V }) ->
    {M, F, A} = application:get_env(darcy, blob_decode_fun,
                                    {darcy, default_decode, []}),
    {blob, erlang:apply(M, F, [V | A])};
unddt(#{ <<"N">> := V }) -> binary_to_integer(V);
unddt(#{ <<"S">> := V }) -> V;
unddt(#{ <<"BOOL">> := <<"true">> }) -> true;
unddt(#{ <<"BOOL">> := <<"false">> }) -> false;
unddt(#{ <<"L">> := V }) -> [ unddt(E) || E <- V ];
unddt(#{ <<"M">> := V }) -> maps:map(fun(_K, Val) -> unddt(Val) end, V);
unddt(#{ <<"SS">> := V }) -> ordsets:from_list([ E || E <- V ]);
unddt(#{ <<"NS">> := V }) ->
    ordsets:from_list([ binary_to_integer(E) || E <- V ]);
unddt(#{ <<"NULL">> := _V }) -> undefined;
unddt(Other) -> erlang:error({error, badarg}, [Other]).

to_ddb(M) when is_map(M) ->
    maps:map(fun(_K, V) -> ddt(V) end, M);
to_ddb(Other) -> erlang:error({error, badarg}, [Other]).

ddt(undefined) -> #{ <<"NULL">> => <<>> };
ddt({blob, Data}) ->
    {M, F, A} = application:get_env(darcy, blob_encode_fun,
                                    {darcy, default_encode, []}),
    #{ <<"B">> => erlang:apply(M, F, [ Data | A ]) };
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
