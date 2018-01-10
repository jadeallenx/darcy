%% @doc This is the main API for the library.
%%
%% You can think of this module as an abstraction layer
%% on the raw API which attempts to add some convenience
%% to plumbing the entire thing by hand-coding maps with
%% the appropriate AWS magic.
%%
%% The low level API calls are in `darcy_ddb_api.erl', but generally
%% users shouldn't call those directly.
-module(darcy).
-include("darcy.hrl").

-export([
    start/0,
    to_map/1,
    clean_map/1,
    to_ddb/1,
    default_decode/1,
    default_encode/1,
    make_attribute_defs/1,
    make_key_schema/1,
    make_provisioned_throughput/2,
    table_name/1,
    make_table_spec/3,
    make_table_spec/5,
    make_global_index_spec/3,
    make_global_index_spec/5,
    add_global_index/2,
    make_table_if_not_exists/2,
    describe_table/2,
    delete_table/2,
    get_item/3,
    batch_get_items/2,
    put_item/3,
    batch_write_items/3,
    query/3,
    query/4
]).

-type lookup_value() :: integer() | float() | binary() | {blob, binary()}.
%-type set_value() :: {number_set, [ integer() | float() ] } | {string_set, [ binary() ]}.
%-type list_value() :: {list, [ map() | set_value() | lookup_value() ]}.

%% @doc Convenience function to start `darcy' and all
%% of its dependent applications.
start() ->
    application:ensure_all_started(darcy).

%% @doc Return a map with the key of `AttributeDefinitions'
%% suitable for using in a table or index specification.
-spec make_attribute_defs(
        [ { AttributeName :: binary(),
            AttributeType :: binary() } ]
       ) -> AttributeDefinitions :: map().
make_attribute_defs(Attributes) when is_list(Attributes) ->
    #{ <<"AttributeDefinitions">> =>
      [ #{ <<"AttributeName">> => N,
           <<"AttributeType">> => T } || {N, T} <- Attributes ] }.

%% @doc Return a `KeySchema' map suitable for use in a
%% table or index specification.
%%
%% If you pass one attribute, it will be assigned the `HASH'
%% key type.  If you pass two attributes, the first will be
%% `HASH' and the second will be the `RANGE' type.
%%
%% You can <a href="http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.Partitions.html">read more about hash and range keys</a>
%% in the official Dynamo documentation.
-spec make_key_schema(
        [ Keys :: binary() ] ) -> KeySchema :: map().
make_key_schema([HashKey]) ->
    make_schema_impl([{HashKey, <<"HASH">>}]);
make_key_schema([HashKey, RangeKey]) ->
    make_schema_impl([{HashKey, <<"HASH">>}, {RangeKey, <<"RANGE">>}]).

make_schema_impl(Schema) ->
    #{ <<"KeySchema">> =>
      [ #{ <<"AttributeName">> => N,
           <<"KeyType">> => T } || {N, T} <- Schema ] }.

%% @doc Makes a `ProvisionedThroughput' map to indicate the number
%% read and write units Dynamo should reserve for your index or
%% table.
-spec make_provisioned_throughput(
        ReadUnits :: pos_integer(),
        WriteUnits :: pos_integer() ) -> ProvisionedThroughput :: map().
make_provisioned_throughput(ReadUnits, WriteUnits) ->
    #{ <<"ProvisionedThroughput">> =>
          #{ <<"ReadCapacityUnits">> => ReadUnits,
         <<"WriteCapacityUnits">> => WriteUnits } }.

%% @doc Makes a `TableName' map suitable for use in a table
%% or index specification.
-spec table_name( Name :: binary() ) -> TableName :: map().
table_name(N) when is_binary(N) -> #{ <<"TableName">> => N }.

%% @doc Convenience function which returns a complete
%% table specification.  This function uses the default
%% number of read and write units (currently 5 each).
-spec make_table_spec(
        TableName :: binary(),
        Attr :: [{ AttrName :: binary(),
                   AttrType :: binary() }],
        Keys :: [ Keys :: binary() ] ) -> TableSpec :: map().
make_table_spec(TableName, Attributes, Keys) ->
    make_table_spec(TableName, Attributes, Keys,
                    ?DEFAULT_READ_UNITS, ?DEFAULT_WRITE_UNITS).

%% @doc Convenience function which returns a complete
%% table specification.
-spec make_table_spec(
        TableName :: binary(),
        Attr :: [{ AttrName :: binary(),
                   AttrType :: binary() }],
        Keys :: [ binary() ],
        ReadUnits :: pos_integer(),
        WriteUnits :: pos_integer() ) -> TableSpec :: map().
make_table_spec(TableName, Attributes, Keys, Read, Write) ->
    lists:foldl(fun(M, Acc) -> maps:merge(M, Acc) end, #{},
                [make_attribute_defs(Attributes),
                 make_key_schema(Keys),
                 make_provisioned_throughput(Read, Write),
                 table_name(TableName)]).

%% @doc Convenience function which returns a global index
%% specification. This function uses the default read and
%% write units (currently 5 each).
-spec make_global_index_spec(
        IndexName :: binary(),
        Keys :: [ binary() ],
        ProjectionSpec :: {} |
            { [ binary() ], binary() }
       ) -> GlobalIndexSpec :: map().
make_global_index_spec(IndexName, Keys, ProjectionSpec) ->
    make_global_index_spec(IndexName, Keys, ProjectionSpec,
                           ?DEFAULT_READ_UNITS, ?DEFAULT_WRITE_UNITS).

%% @doc Convenience function which returns a global index
%% specification.
-spec make_global_index_spec(
        IndexName :: binary(),
        Keys :: [ binary() ],
        ProjectionSpec :: {} |
            { [ NonKeyAttribute :: binary() ], binary() },
        ReadUnits :: pos_integer(),
        WriteUnits :: pos_integer()
       ) -> GlobalIndexSpec :: map().
make_global_index_spec(IndexName, Keys, ProjectionSpec, Read, Write) ->
    lists:foldl(fun(M, Acc) -> maps:merge(M, Acc) end, #{},
                [make_key_schema(Keys),
                 make_provisioned_throughput(Read, Write),
                 make_projection(ProjectionSpec),
                 index_name(IndexName)]).

%% @doc Add a global index specification to an existing table
%% specification.  If a global index specification has already
%% been added, this function will add the new one to the
%% current one.
%%
%% Tables may not have more than two global indices.
-spec add_global_index(
        TableSpec :: map(),
        GlobalIndexSpec :: map() ) -> NewTableSpec :: map().
add_global_index(#{ <<"GlobalSecondaryIndexes">> := CurrentGSI } = TableSpec, GSISpec) ->
    maps:put(<<"GlobalSecondaryIndexes">>, [ GSISpec | CurrentGSI ], TableSpec);
add_global_index(TableSpec, GSISpec) ->
    maps:put(<<"GlobalSecondaryIndexes">>, [ GSISpec ], TableSpec).

%% @doc Create an `IndexName' map for use in an index specification.
-spec index_name( Name :: binary() ) -> IndexName :: map().
index_name(N) when is_binary(N) -> #{ <<"IndexName">> => N }.

%% @doc This function returns a `Projection' map suitable for
%% use in an index specification.  It is expected that if you
%% want the `ALL' or `KEYS_ONLY' projection type, your list of
%% non-key attributes will be empty.
%%
%% If you want an empty projection map, pass in an empty tuple
%% `{}'.
-spec make_projection(
        ProjectionSpec :: {} |
            { [ NonKeyAttribute :: binary() ], binary() }) -> Projection :: map().
make_projection({}) -> #{ <<"Projection">> => #{} };
make_projection({Attr, <<"INCLUDE">>}) -> #{ <<"Projection">> => #{ <<"NonKeyAttributes">> => Attr,
                                                                    <<"ProjectionType">> => <<"INCLUDE">> } };
make_projection({[], T}) -> #{ <<"Projection">> => #{ <<"ProjectionType">> => T } }.

%% @doc Make a table if it doesn't already exist.
-spec make_table_if_not_exists( Client :: darcy_client:aws_client(),
                                TableName :: binary() ) -> ok | {error, Error :: term()}.
make_table_if_not_exists(Client, #{ <<"TableName">> := TableName} = Spec) ->
    case darcy_ddb_api:describe_table(Client, #{ <<"TableName">> => TableName }) of
           {ok, _Result, _Details} -> ok;
        {error, _Error,  {400,  _Headers, _Client}} -> attempt_make_table(Client, Spec);
        {error, Error,   {Status, _Headers, _NClient}} -> {error, {table_creation_error, {Status, Error}}}
    end.

attempt_make_table(Client, Spec) ->
    case darcy_ddb_api:create_table(Client, Spec) of
           {ok, _Result, {   200, _Headers, _Client}} -> ok;
        {error, Error,   {Status, _Headers, _NewClient}} -> {error, {table_creation_failed, {Status, Error}}}
    end.

%% @doc Delete a Dynamo table with the given name.
-spec delete_table(    Client :: darcy_client:aws_client(),
                    TableName :: binary() ) -> ok | {error, Error :: term()}.
delete_table(Client, TableName) ->
    case darcy_ddb_api:delete_table(Client, table_name(TableName)) of
        {ok, #{ <<"TableDescription">> := Desc }, Details} -> ensure_deleting_state(Desc, Details);
        {error,              Error, {Status, _Headers, _C}} -> {error, {table_deletion_error, {Status, Error}}}
    end.

ensure_deleting_state( #{ <<"TableStatus">> := <<"DELETING">> }, _Details ) -> ok;
ensure_deleting_state( Other , {Status, _Headers, _C} ) -> {error, {table_deletion_error, {Status, Other}}}.

%% @doc This returns a map representing the current state of the
%% given Dynamo table.
-spec describe_table( Client :: darcy_client:aws_client(),
                      TableName :: binary() ) -> {ok, TableDesc :: map()} |
                                                 {error, Error :: term()}.
describe_table(Client, TableName) ->
    case darcy_ddb_api:describe_table(Client, table_name(TableName)) of
         {ok, Result, _Details                  } -> {ok, Result};
      {error,  Error, {Status, _Headers, Client}} -> {error, {table_description_error, {Status, Error}}}
    end.

%% GET ITEM

%% @doc Retrieve a single item from the given Dynamo table using
%% the hash and if needed, range keys.
-spec get_item( Client :: darcy_client:aws_client(),
                TableName :: binary(),
                Key :: #{ KeyName :: binary() => LookupValue :: lookup_value() }
              ) -> {ok, Item :: map()} |
                   {error, not_found} |
                   {error, Error :: term()}.
get_item(Client, TableName, Key) ->
    Request = #{ <<"TableName">> => TableName,
                 <<"Key">> => to_ddb(Key) },

    case darcy_ddb_api:get_item(Client, Request) of
          {ok, Raw, _Details} -> return_value(Raw);
          {error, Error, {Code, Headers, _Client}} -> {error, {Error, [Code, Headers]}}
    end.

%% @TODO Implement this
batch_get_items(Client, Request) ->
    darcy_ddb_api:batch_get_item(Client, Request).

%% PUT ITEM

%% @doc Put a single item into the given dynamo table.
-spec put_item( Client :: darcy_client:aws_client(),
                TableName :: binary(),
                Item :: map() ) -> ok | {error, Error :: term()}.
put_item(Client, TableName, Item) ->
    Request = #{ <<"TableName">> => TableName,
                 <<"Item">> => to_ddb(Item) },
    case darcy_ddb_api:put_item(Client, Request) of
         {ok, #{}, {200, _Headers, _Client}} -> ok;
    {error, Error, {Code, Headers, _Client}} -> {error, {Error, [Code, Headers]}}
    end.

%% @doc Put a list of items into the given Dynamo table.
%%
%% This function currently does not support deleting
%% items (although the underlying API supports this.)
%%
%% Items are automatically batched into groups of 25 or
%% less as required by AWS. Unprocessed keys are automatically
%% retried up to 5 times.
-spec batch_write_items( Client :: darcy_client:aws_client(),
                         TableName :: binary(),
                         Items :: [ map() ] ) -> ok | {error, Error :: term()}.
batch_write_items(Client, TableName, Items) when length(Items) =< 25 ->
    Request = make_batch_put(TableName, Items),
    Result = darcy_ddb_api:batch_write_item(Client, Request),
    handle_batch_write_result(Client, ?RETRIES, Result);
batch_write_items(Client, TableName, Items) ->
    {Part, Tail} = lists:split(25, Items),
    ok = batch_write_items(Client, TableName, Part),
    batch_write_items(Client, TableName, Tail).

make_batch_put(TableName, Items) when length(Items) =< 25 ->
    #{ <<"RequestItems">> =>
       #{ TableName => [
             #{ <<"PutRequest">> =>
                #{ <<"Item">> => to_ddb(I) }
              } || I <- Items ]
        }
     }.

handle_batch_write_result(_Client, _N,
                          {ok, #{ <<"UnprocessedItems">> := U }, _Details})
                          when map_size(U) == 0 -> ok;

handle_batch_write_result(Client, N,
                          {ok, #{ <<"UnprocessedItems">> := U }, _Details}) ->
                          reprocess_batch_write(Client, N, U);

handle_batch_write_result(_Client, _N,
                           {error, Error, {Status, Headers, _Ref}}) ->
                           {error, {Error, [Status, Headers]}}.

reprocess_batch_write(_Client, 0, RetryItems) -> {error, {retries_exceeded, RetryItems}};
reprocess_batch_write(Client, N, RetryItems) ->
    Results = darcy_ddb_api:batch_write_item(Client, #{ <<"RequestItems">> => RetryItems }),
    handle_batch_write_result(Client, N-1, Results).

return_value(#{ <<"Item">> := Item }) -> {ok, clean_map(to_map(Item))};
return_value(#{} = M) when map_size(M) == 0 -> {error, not_found}.

%% QUERY

%% @doc Lookup records using the partition and range keys from
%% a table or an index.
%%
%% Unfortunately this call requires quite a bit of understanding
%% of both the Dynamo data model and the table and/or index
%% structures.
%%
%% The query expression should take the form of a map which follows
%% the <a href="http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html">query guidelines</a> laid out in the official AWS documentation.
%%
%% The return value also punts on the issue of result pagination.
-spec query( Client :: darcy_client:aws_client(),
             TableName :: binary(),
             QueryExpression :: map() ) -> {ok, Results :: map() } |
                                           {error, Error :: term()}.
query(Client, TableName, Expr) ->
    query_impl(Client, [table_name(TableName), Expr]).

%% @doc A query that operates on an index instead of a table.
-spec query( Client :: darcy_client:aws_client(),
             TableName :: binary(),
             IndexName :: binary(),
             QueryExpression :: map() ) -> {ok, Results :: map() } |
                                           {error, Error :: term()}.
query(Client, TableName, IndexName, Expr) ->
    query_impl(Client, [table_name(TableName), index_name(IndexName), Expr]).

query_impl(Client, Ops) ->
    Request = lists:foldl(fun(M, Acc) -> maps:merge(M, Acc) end, #{}, Ops),
    case darcy_ddb_api:query(Client, Request) of
         {ok, Result, _Details                  } -> process_result_set(Result);
      {error,  Error, {Status, Headers, _Client}} -> {error, {Error, {Status, Headers}}}
    end.

process_result_set(#{ <<"Count">> := C }) when C == 0 -> {error, not_found};
process_result_set(#{ <<"Items">> := Items, <<"Count">> := C }) ->
    {ok, #{ <<"Count">> => C, <<"Items">> => [ clean_map(to_map(I)) || I <- Items ] } };
process_result_set(Other) ->
    {error, {query_error, Other}}.

%% @doc This function returns a map without any Dynamo specific type tuples,
%% which is useful for passing around internally in an application that doesn't
%% care or understand Dynamo data types.
clean_map(M) when is_map(M) ->
    maps:map(fun(_K, {_, V}) -> V;
                (_K, V) -> V
             end,
             M).

%% @doc This is the default decoding function for binary data. It base64
%% decodes the binary, and decompresses it.
default_decode(Blob) ->
    zlib:uncompress(base64:decode(Blob)).

%% @doc This is the default encoding function for binary data. It compresses
%% the data and base64 encodes it.
default_encode(Data) ->
    base64:encode(zlib:compress(Data)).

%% @doc Translate from a "raw" JSON map representation of a Dynamo
%% data item to an Erlang data item.  Uses the following tuples
%% to remove ambiguities in Erlang JSON encoding:
%% <ul>
%%      <li>`NULL' values are returned as `undefined'</li>
%%      <li>`{blob, Binary}'</li>
%%      <li>`{list, List}'</li>
%%      <li>`{string_set, Set}' (internally stored as an ordset)</li>
%%      <li>`{number_set, Set}' (internally stored as an ordset)</li>
%% </ul>
to_map(M) when is_map(M) ->
    maps:map(fun(_K, V) when is_map(V) -> unddt(V);
                (_K, V) -> V end,
             M).

%% @private
unddt(#{ <<"B">> := V }) ->
    {M, F, A} = application:get_env(darcy, blob_decode_fun,
                                    {darcy, default_decode, []}),
    {blob, erlang:apply(M, F, [V | A])};
unddt(#{ <<"N">> := V }) ->
    %% could be an integer or a float. Try integer conversion
    %% first.
    try
        binary_to_integer(V)
    catch
        error:badarg -> binary_to_float(V)
    end;
unddt(#{ <<"S">> := V }) -> V;
unddt(#{ <<"BOOL">> := <<"true">> }) -> true;
unddt(#{ <<"BOOL">> := <<"false">> }) -> false;
unddt(#{ <<"L">> := V }) -> {list, [ unddt(E) || E <- V ]};
unddt(#{ <<"M">> := V }) -> maps:map(fun(_K, Val) -> unddt(Val) end, V);
unddt(#{ <<"SS">> := V }) -> {string_set, ?SET:from_list([ E || E <- V ])};
unddt(#{ <<"NS">> := V }) -> {number_set, ?SET:from_list([ binary_to_integer(E) || E <- V ])};
unddt(#{ <<"NULL">> := _V }) -> undefined;
unddt(Other) -> erlang:error({error, badarg}, [Other]).

%% @doc This function takes an Erlang map and attempts to encode it using Dynamo
%% data type annotations. Because there are ambiguities in how Erlang internally
%% represents things like strings, lists and sets, tagged tuples are used to
%% remove ambiguity.  They are the same tagged tuples as above:
%% <ul>
%%      <li>`undefined' is stored as a `NULL' data type</li>
%%      <li>`{blob, Binary}'</li>
%%      <li>`{list, List}'</li>
%%      <li>`{string_set, Set}' (internally stored as an ordset)</li>
%%      <li>`{number_set, Set}' (internally stored as an ordset)</li>
%% </ul>
%%
%% Generally, you should try to modify your internal data representation values
%% to remove these ambiguities <i>before</i> you pass them into this function.

to_ddb(M) when is_map(M) ->
    maps:map(fun(_K, V) -> ddt(V) end, M);
to_ddb(Other) -> erlang:error({error, badarg}, [Other]).

%% @private
ddt(undefined) -> #{ <<"NULL">> => <<>> };
ddt({blob, Data}) ->
    {M, F, A} = application:get_env(darcy, blob_encode_fun,
                                    {darcy, default_encode, []}),
    #{ <<"B">> => erlang:apply(M, F, [ Data | A ]) };
ddt({list, L}) -> #{ <<"L">> => [ ddt(E) || E <- L ] };
ddt({string_set, S}) -> #{ <<"SS">> => [ ddt(E) || E <- ?SET:to_list(S) ] };
ddt({number_set, S}) -> #{ <<"NS">> => [ ddt(E) || E <- ?SET:to_list(S) ] };
ddt(V) when is_integer(V) -> #{ <<"N">> => number_to_binary(V) };
ddt(V) when is_float(V) -> #{ <<"N">> => number_to_binary(V) };
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

number_to_binary(V) when is_integer(V) -> integer_to_binary(V);
number_to_binary(V) when is_float(V) -> float_to_binary(V, [{decimals, 20}, compact]).

%% Tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

to_ddb_test() ->
    Raw = #{ <<"Grades">> => {list, [17,39,76,27]},
             <<"Average">> => 39.75,
             <<"Student">> => <<"Quentin">>,
             <<"Subject">> => <<"Science">> },
    Expected = #{<<"Grades">> => #{<<"L">> => [#{<<"N">> => <<"17">>}, #{<<"N">> => <<"39">>}, #{<<"N">> => <<"76">>}, #{<<"N">> => <<"27">>}]},
                 <<"Average">> => #{<<"N">> => <<"39.75">>},
            <<"Student">> => #{<<"S">> => <<"Quentin">>},
            <<"Subject">> => #{<<"S">> => <<"Science">>}},
    ?assertEqual(Expected, to_ddb(Raw)).


to_map_test() ->
    Raw = #{<<"Grades">> => #{<<"L">> => [#{<<"N">> => <<"17">>}, #{<<"N">> => <<"39">>}, #{<<"N">> => <<"76">>}, #{<<"N">> => <<"27">>}]},
            <<"Student">> => #{<<"S">> => <<"Quentin">>},
            <<"Subject">> => #{<<"S">> => <<"Science">>}},
    Expected = #{ <<"Grades">> => {list, [17, 39, 76, 27]},
                  <<"Student">> => <<"Quentin">>,
                  <<"Subject">> => <<"Science">> },
    ?assertEqual(Expected, to_map(Raw)).

clean_map_test() ->
    Raw = #{<<"Grades">> => #{<<"L">> => [#{<<"N">> => <<"17">>}, #{<<"N">> => <<"39">>}, #{<<"N">> => <<"76">>}, #{<<"N">> => <<"27">>}]},
            <<"Student">> => #{<<"S">> => <<"Quentin">>},
            <<"Subject">> => #{<<"S">> => <<"Science">>}},
    Expected = #{ <<"Grades">> => [17, 39, 76, 27],
                  <<"Student">> => <<"Quentin">>,
                  <<"Subject">> => <<"Science">> },
    ?assertEqual(Expected, clean_map(to_map(Raw))).
-endif.
