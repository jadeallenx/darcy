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

-define(TIMEOUT, 5000). % 5 seconds
-define(BIG_TIMEOUT, 5*60*1000). % 5 minutes
-define(BATCH_SIZE, 100). % how many items to split into a batch
-define(BATCH_MAX, 10). % maximum number of batches to process in a single go
-define(INITIAL_ERROR_DELAY, 500). % initial throughput retry delay in milliseconds
-define(MAX_ERROR_DELAY, 32000). % maximum millseconds of delay before terminating operation
-define(INITIAL_ERROR_STATE, #error_delay{}).

%% This is a record because, later, I might try to extend this to reduce the sleep
%% time based on the number of successful operations. So for now it only has one
%% field, but it may have two or more in the future.
-record(error_delay, {
          delay = 0 :: non_neg_integer()
}).

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
    make_global_table_if_not_exists/3,
    describe_table/2,
    describe_global_table/2,
    delete_table/2,
    get_item/3,
    batch_get_items/3,
    put_item/3,
    put_item/4,
    batch_write_items/3,
    query/3,
    query/4,
    scan/3,
    scan_all/3,
    scan_all/4,
    scan_parallel/5,
    scan_parallel/6,
    scan_parallel/7
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
                                TableSpec :: map() ) -> ok | {error, Error :: term()}.
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

%% @doc Make a global table if it doesn't already exist.
-spec make_global_table_if_not_exists(Client :: darcy_client:aws_client(),
                                      TableSpec :: map(),
                                      Regions :: [ binary() ]) -> ok | {error, Error :: term()}.
make_global_table_if_not_exists(#{ region := Region } = Client,
                                #{ <<"TableName">> := TableName } = Spec, Regions) ->
    case lists:member(Region, Regions) of
        false -> {error, {bad_region_spec, [Region, Regions]}};
        true ->
            case describe_global_table(Client, TableName) of
                {ok, _Result} -> ok;
                {error, _} ->
                    ok = global_table_setup(Client, Spec, Regions),
                    attempt_make_global_table(Client, TableName, Regions)
            end
    end.

all_ok(ok) -> true;
all_ok(_) -> false.

global_table_setup(Client, Spec, Regions) ->
    true = lists:all(fun all_ok/1,
              pmap(fun(R) -> do_table_creation(Client, Spec, R) end, Regions)
                    ),
    ok.

do_table_creation(Client, Spec, Region) ->
    NewClient = darcy_client:switch_region(Client, Region),
    NewSpec = enable_global_streams(Spec),
    make_table_if_not_exists(NewClient, NewSpec).

enable_global_streams(Spec) ->
    Streams = #{ <<"StreamSpecification">> =>
                 #{ <<"StreamEnabled">> => true,
                    <<"StreamViewType">> => <<"NEW_AND_OLD_IMAGES">> } },
    maps:merge(Spec, Streams).

attempt_make_global_table(Client, TableName, Regions) ->
    Req = #{ <<"GlobalTableName">> => TableName,
             <<"ReplicationGroup">> => [ #{ <<"RegionName">> => R } || R <- Regions ] },
    %% this is a long-running operation, make the response receive timeout 60 seconds
    case darcy_ddb_api:create_global_table(Client, Req, [{recv_timeout, 60*1000}]) of
        {ok, _Result, {200, _Headers, _Client}} -> ok;
        {error, Error, {Status, _Headers, _Client}} -> {error, {global_table_creation_failed, {Status, Error}}}
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

%% @doc This returns a map representing the current state of the
%% given Dynamo global table.
-spec describe_global_table( Client :: darcy_client:aws_client(),
                             TableName :: binary() ) -> {ok, TableDesc :: map()} |
                                                        {error, Error :: term()}.
describe_global_table(Client, TableName) ->
    case darcy_ddb_api:describe_global_table(Client, #{ <<"GlobalTableName">> => TableName }) of
         {ok, Result, _Details                   } -> {ok, Result};
      {error,  Error, {Status, _Headers, _Client}} -> {error, {table_description_error, {Status, Error}}}
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

%% @doc Retrieve a set of records given a list of keys.
%%
%% The underlying API supports a maximum of 100 keys per
%% request, so this call batches keys into sets of up
%% to 100 and folds across these sets into an
%% accumulator for all keys.
%%
%% Items which are not found in the table will not be part
%% of the result set but will consume provisioned read
%% capacity.
%%
%% <B>N.B.</B>: Dynamo and this client will not return your items
%% in any particular order!
-spec batch_get_items(    Client :: darcy_client:aws_client(),
                       TableName :: binary(),
                           Items :: [ map() ] ) -> {ok, [ Result :: term() ]} |
                                                   {error, Error :: term() }.
batch_get_items(Client, TableName, Keys) ->
    make_batch_get(Client, TableName, Keys, []).

make_batch_get(_Client, _TableName, [], Acc) -> {ok, lists:flatten(Acc)};
make_batch_get(Client, TableName, Keys, Acc) ->
    {Request, Rest} = format_batch_get(TableName, Keys),
    Result = execute_batch_get(Client, TableName, Request, ?RETRIES, []),
    make_batch_get(Client, TableName, Rest, [ Result | Acc ]).

format_batch_get(TableName, Keys) ->
    {Current, Rest} = maybe_split_keys(Keys),
    {format_batch_get_request(TableName, Current), Rest}.

maybe_split_keys(Keys) when length(Keys) =< 100 -> {Keys, []};
maybe_split_keys(Keys) -> lists:split(100, Keys).

format_batch_get_request(TableName, Keys) ->
    #{ <<"RequestItems">> =>
       #{ TableName =>
          #{ <<"Keys">> => [ to_ddb(K) || K <- Keys ] }
        }
     }.

execute_batch_get(_Client, _TableName, Request, 0, Acc) -> {error, {retries_exceeded, Request, Acc}};
execute_batch_get(Client, TableName, Request, Retries, Acc) ->
    case darcy_ddb_api:batch_get_item(Client, Request) of
        {ok, Raw, _Details} -> return_batch_get_results(Raw, Client, TableName, Retries, Acc);
        {error, Error, {Code, Headers, _Client}} -> {error, {Error, [Code, Headers]}}
    end.

%% happy path: no unprocessed keys, no accumulator
return_batch_get_results(#{ <<"Responses">> := R,
                            <<"UnprocessedKeys">> := U }, _Client, TableName,
                                                          _Retries, [])
                                                   when map_size(U) == 0 ->
    batch_get_results(TableName, R);

%% not as happy path: no unprocessed keys, accumulator has partial results
return_batch_get_results(#{ <<"Responses">> := R,
                            <<"UnprocessedKeys">> := U }, _Client, TableName,
                                                          _Retries, Acc)
                                                   when map_size(U) == 0 ->
    lists:flatten([ batch_get_results(TableName, R) | Acc ]);

%% not very happy path: unprocessed keys
return_batch_get_results(#{ <<"Responses">> := R,
                            <<"UnprocessedKeys">> := U }, Client, TableName,
                                                          Retries, Acc) ->
    retry_sleep(Retries),
    execute_batch_get(Client, TableName, U,
                      Retries - 1, [ batch_get_results(TableName, R) | Acc ]).

batch_get_results(TableName, Responses) ->
    [ clean_map(to_map(I)) || I <- maps:get(TableName, Responses) ].

%% PUT ITEM

%% @doc Put a single item into the given dynamo table, with conditions!
-spec put_item( Client :: darcy_client:aws_client(),
                TableName :: binary(),
                Item :: map(),
                ConditionMap :: map() ) -> ok | {error, Error :: term()}.
put_item(Client, TableName, Item, ConditionMap) ->
    ConditionExpression = maps:get(condition_expression, ConditionMap, undefined),
    ExpressionAttributeNames = maps:get(expression_attribute_names, ConditionMap, undefined),
    ExpressionAttributeValues = maps:get(expression_attribute_values, ConditionMap, undefined),
    Request = make_put_request(TableName,
                               Item,
                               ConditionExpression,
                               ExpressionAttributeNames,
                               ExpressionAttributeValues),
    case darcy_ddb_api:put_item(Client, Request) of
         {ok, #{}, {200, _Headers, _Client}} -> ok;
    {error, Error, {Code, Headers, _Client}} -> {error, {Error, [Code, Headers]}}
    end.

make_put_request(TableName, Item, undefined, undefined, undefined) ->
    #{ <<"TableName">> => TableName,
       <<"Item">> => to_ddb(Item) };
make_put_request(TableName, Item,
                 ConditionExpression,
                 ExpressionAttributeNames,
                 ExpressionAttributeValues)
  when is_binary(ConditionExpression) and
       is_map(ExpressionAttributeNames) and
       is_map(ExpressionAttributeValues) ->
    #{ <<"TableName">> => TableName,
       <<"ConditionExpression">> => ConditionExpression,
       <<"ExpressionAttributeNames">> => ExpressionAttributeNames,
       <<"ExpressionAttributeValues">> => to_ddb(ExpressionAttributeValues),
       <<"Item">> => to_ddb(Item) }.

%% @doc Put a single item into the given dynamo table.
-spec put_item( Client :: darcy_client:aws_client(),
                TableName :: binary(),
                Item :: map() ) -> ok | {error, Error :: term()}.
put_item(Client, TableName, Item) ->
    put_item(Client, TableName, Item, #{}).

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
    retry_sleep(N),
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

%% SCAN

%% @doc This function executes a sequential table scan (no parallelism here)
%% and returns results synchronously to the caller. If you want to scan a table
%% with parallel workers, look at `scan_parallel'.
%%
%%
%% Scans return automatically when 1 MB of data has accumulated. If more data
%% is available, the atom `partial' will be returned instead of `ok'.
%%
%% If you want to continue your scanning activities, you must add the
%% `LastEvaluatedKey' as the `ExclusiveStartKey' in the next call to
%% this function's expression map. (See the official API docs for
%% further details about continuing scans:
%%
%% https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_ResponseSyntax.)
%%
%% If a table has no data, the atom `empty_table' will be returned.
%%
%% If a table has data, but a filter expression has filtered all results,
%% the atom `no_results' will be returned.
-spec scan( Client :: darcy_request:aws_client(),
            TableName :: binary(),
            Expr :: map() ) -> {ok, Result :: map()} |
                               {ok, empty_table} |
                               {ok, no_results} |
                               {partial, Result :: map()} |
                               {error, Reason :: term()}.
scan(Client, TableName, Expr) ->
    execute_scan(Client, make_scan_request([Expr, table_name(TableName)])).


%% @doc This function executes a sequential scan over an entire table. In other
%% words, it will continue to make new calls until no `LastEvaluatedKey' field
%% is returned from Dynamo.
%%
%% The items will be accumulated into a list and returned.  This call is
%% syntactic sugar for `scan_all/4' with a function of `same(X) -> X' passed
%% in.
scan_all(Client, TableName, Expr) ->
    scan_all(Client, TableName, Expr, fun same/1).

same(X) -> X.

%% @doc This function executes a sequential scan over an entire table. In other
%% words, it will continue to make new calls until no `LastEvaluatedKey' field
%% is returned from Dynamo.
%%
%% For each item returned, the function `Fun' will be executed and the results
%% accumulated and returned when all valid rows from the scan query expression
%% have been processed. (You may or may not care about these results if you're
%% doing something in your function for the side effect.)
-spec scan_all( Client :: darcy_request:aws_client(),
             TableName :: binary(),
                  Expr :: map(),
                  Fun  :: function() ) -> {ok, [ term() ]} |
                                          {error, { Reason :: term(), Acc :: [ map() ]}}.
scan_all(Client, TableName, Expr, Fun) ->
    Request = make_scan_request([Expr, table_name(TableName)]),
    do_scan_all(Client, Request, execute_scan(Client, Request), Fun, [], ?INITIAL_ERROR_STATE).

do_scan_all(_Client, _Req, {ok, empty_table}, _Fun, _Acc, _ErrDelay) -> {ok, []};
do_scan_all(_Client, _Req, {ok, no_results}, _Fun, Acc, _ErrDelay) -> {ok, flatten(Acc)};
do_scan_all(_Client, _Req, {ok, #{ <<"Items">> := I }}, Fun, Acc, _ErrDelay) ->
    {ok, flatten([ batch_pmap(Fun, I) | Acc ])};

do_scan_all(Client, Req, {error, {EType, _ETxt}=Error}, Fun, Acc, EDelay) ->
    case maybe_retry_scan_op(get_error_type(EType), EDelay) of
      {true, NewErrDelay} ->
        error_logger:warning_msg("Got error ~p. Will retry request.", [Error]),
        maybe_sleep(NewErrDelay),
        do_scan_all(Client, Req, execute_scan(Client, Req), Fun, Acc, NewErrDelay);
      _ ->
        error_logger:error_msg("Error executing scan_all: ~p request: ~p", [Error, Req]),
        {error, {Error, Acc}}
    end;

%% TODO? Maybe throttle back up if we get "a lot" of successes.
do_scan_all(Client, Req, {partial, #{ <<"LastEvaluatedKey">> := LEK, <<"Items">> := I }}, Fun, Acc, ErrDelay) ->
    NewRequest = make_scan_request([Req, #{ <<"ExclusiveStartKey">> => LEK }]),
    maybe_sleep(ErrDelay),
    do_scan_all(Client, NewRequest, execute_scan(Client, NewRequest), Fun, [ batch_pmap(Fun, I) | Acc], ErrDelay).

maybe_sleep(#error_delay{ delay = 0 }) ->
    ok;
maybe_sleep(#error_delay{ delay = D }) ->
    error_logger:warning_msg("THROTTLE: Pid ~p sleeping for ~p ms before next request...", [self(), D]),
    timer:sleep(D).

maybe_retry_scan_op(<<"ProvisionedThroughputExceededException">>, #error_delay{ delay = 0 } = ErrState) ->
    {true, ErrState#error_delay{ delay = ?INITIAL_ERROR_DELAY }};

maybe_retry_scan_op(<<"ProvisionedThroughputExceededException">>, #error_delay{ delay = D } = ErrState) when D > ?MAX_ERROR_DELAY ->
    {false, ErrState};

maybe_retry_scan_op(<<"ProvisionedThroughputExceededException">>, #error_delay{ delay = D } = ErrState) ->
    {true, ErrState#error_delay{ delay = D*2 }};

maybe_retry_scan_op(_Type, ErrorState) ->
    {false, ErrorState}.

get_error_type(Type) ->
    [_, Exception] = binary:split(<<"#">>, Type),
    Exception.

%% @doc Scan a table in parallel using the given expression. This
%% function is equivalent to `scan_parallel/7' with a timeout value of 60000
%% milliseconds.
%%
%% Instead of returning the coordinator pid to the caller, this
%% function blocks and waits for the return values from the
%% coordinator.
scan_parallel(Client, TableName, Expr, Fun, SegmentCount) ->
    scan_parallel(Client, TableName, Expr, Fun, ?BIG_TIMEOUT, SegmentCount).

scan_parallel(Client, TableName, Expr, Fun, Timeout, SegmentCount) ->
    {Ref, _Pid} = scan_parallel(Client, TableName, Expr, Fun, Timeout, SegmentCount, self()),

    receive
        {Ref, {Results, []}} -> {ok, Results};
        {Ref, {Partial, Errors}} -> {error, {Errors, Partial}};
        Other -> Other
    after Timeout ->
        {error, scan_timeout}
    end.

%% @doc Scan a table in parallel using the given expression.
%%
%% DynamoDB supports parallel scans using a partitioning technique described in the Developer Guide.
%%
%% https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html
%%
%% This function spawns a coordinator process which in turn spawns a set of
%% workers, one per segment count which executes the `scan_all/4' function.
%%
%% <B>N.B.</B>: This operation can consume a lot of read capacity. It is a good
%% idea to limit the number of segments used in a scan operation.
%%
%% The coordinator that is spawned here is <B>linked</B> to the caller. If you
%% want more robust error handling, you should trap exit messages by the
%% caller's process.
%%
%% Since this function returns the "raw" results, you will have to
%% handle them appropriately within your own receive block.
%%
%% They will be in the form of `{Ref, {Results, Errors}}' where
%% `Ref' matches the reference returned by this call.  The coordinator
%% pid is the process which is coordinating the parallel workers.
%% (You may or may not care about those.)
-spec scan_parallel(
        Client :: darcy_request:aws_client(),
        TableName :: binary(),
        Expr :: map(),
        Fun :: function(),
        Timeout :: pos_integer(),
        SegmentCount :: pos_integer(),
        ReplyPid :: pid()) -> {Ref :: reference(),
                               Coordinator :: pid()}.
scan_parallel(Client, TableName, Expr, Fun, Timeout, SegmentCount, ReplyPid) ->
    Reqs = [ make_scan_request([Expr, table_name(TableName), make_segments(N, SegmentCount)]) ||
            N <- lists:seq(0, SegmentCount - 1) ],

    Ref = make_ref(),
    Pid = spawn_link(fun() -> start_coordinator(Client, Ref, Timeout, Fun, Reqs, ReplyPid) end),
    {Ref, Pid}.

results_ok({ok, _}) -> true;
results_ok(_) -> false.

start_coordinator(Client, Ref, Timeout, Fun, Reqs, Reply) ->
    L = pmap(fun(Req) -> worker_scan_all(Client, Req, Fun) end, Reqs,
                   Timeout, {Ref, {[], scan_timeout}}),
    ReplyMsg = make_reply_msg(Ref, L),
    Reply ! {Ref, ReplyMsg}.

make_reply_msg(Ref, {Ref, {[], scan_timeout}}) -> {[], {error, scan_timeout}};
make_reply_msg(_Ref, Results) ->
    {Res, Err} = lists:partition(fun results_ok/1, Results),
    {lists:flatten([ R || {ok, R} <- Res ]), Err}.

worker_scan_all(Client, Request, Fun) ->
     do_scan_all(Client, Request, execute_scan(Client, Request), Fun, [], ?INITIAL_ERROR_STATE).

make_segments(N, Count) ->
    #{ <<"Segment">> => N,
       <<"TotalSegments">> => Count }.

make_scan_request(Ops) ->
    lists:foldl(fun(M, Acc) -> maps:merge(M, Acc) end, #{}, Ops).

execute_scan(Client, Request) ->
    case darcy_ddb_api:scan(Client, Request) of
         {ok, Result, _Details                  } -> process_scan_result(Result);
      {error,  Error, {Status, Headers, _Client}} -> {error, {Error, {Status, Headers}}}
    end.

process_scan_result(#{ <<"Count">> := 0, <<"ScannedCount">> := 0 }) -> {ok, empty_table};
process_scan_result(#{ <<"Count">> := 0, <<"ScannedCount">> := _SC }) -> {ok, no_results};
process_scan_result(#{ <<"Items">> := Items, <<"LastEvaluatedKey">> := _LEK } = M) ->
    NewItems = [ clean_map(to_map(I)) || I <- Items ],
    {partial, maps:put(<<"Items">>, NewItems, M)};

process_scan_result(#{ <<"Items">> := Items } = M) ->
    NewItems = [ clean_map(to_map(I)) || I <- Items ],
    {ok, maps:put(<<"Items">>, NewItems, M)};

process_scan_result(Other) ->
    {error, {scan_error, Other}}.


flatten(L) when is_list(L) -> lists:flatten(lists:reverse(L)).

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
unddt(#{ <<"BOOL">> := true }) -> true;
unddt(#{ <<"BOOL">> := <<"false">> }) -> false;
unddt(#{ <<"BOOL">> := false }) -> false;
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
%% see http://boto3.readthedocs.io/en/latest/_modules/boto3/dynamodb/types.html
ddt(undefined) -> #{ <<"NULL">> => true };
ddt(null) -> #{ <<"NULL">> => true };
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

%% sleep for RETRIES - N * 1000 milliseconds before retrying an operation.
%% Current RETRIES value is 5. Always sleeps for <i>at least</i> 1000
%% milliseconds.
%% N = 5 => 1000 ms sleep
%% N = 4 => 1000 ms sleep
%% N = 3 => 2000 ms sleep
%% N = 2 => 3000 ms sleep
%% N = 1 => 4000 ms sleep
retry_sleep(N) ->
    S = max(1000, (?RETRIES-N) * 1000),
    timer:sleep(S).

%% split the big list into smaller batches and execute them in parallel.
batch_pmap(F, List) when length(List) =< ?BATCH_SIZE -> pmap(F, List);
batch_pmap(F, BigList) ->
    Len = length(BigList),
    I = items_per_batch(Len),
    PC = lists:seq(1, Len, I),
    Batches = make_batches(BigList, I, PC, []),
    pmap(fun(E) -> lists:map(F, E) end, Batches).

items_per_batch(Len) ->
    case Len div ?BATCH_SIZE of
        I when I =< ?BATCH_MAX -> ?BATCH_SIZE;
        _ -> Len div ?BATCH_MAX
    end.

make_batches(_, _, [], Acc) -> lists:reverse(Acc);
make_batches(L, Len, [H|T], Acc) ->
    make_batches(L, Len, T, [ lists:sublist(L, H, Len) | Acc ]).

%% parallel map
%% http://erlang.org/pipermail/erlang-questions/2009-January/041214.html
%%
%% TODO: Maybe we do not care about the order messages are received
pmap(F, Arglist) ->
    pmap(F, Arglist, ?TIMEOUT, pmap_timeout).

pmap(F, Arglist, Timeout, TimeoutError) ->
    S = self(),
    TaskID = make_ref(),
    Workers = lists:map( fun(X) ->
                                 spawn_link(fun() -> do_F(S, TaskID, F, X) end)
                         end, Arglist),
    gather(Workers, TaskID, Timeout, TimeoutError).

do_F(Caller, TaskID, F, X) ->
    Caller ! {self(), TaskID, catch(F(X))}.

gather([], _, _, _) -> [];
gather([W|R], TaskID, Timeout, TimeoutError) ->
    receive
        {W, TaskID, Val} ->
            [Val | gather(R, TaskID, Timeout, TimeoutError)]
    after Timeout ->
        TimeoutError
    end.


%% Tests
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

make_put_request_test() ->
    TableName = <<"foo">>,
    Item = #{ <<"Student">> => <<"Foo">>,
              <<"Version">> => <<"totally_a_uuid">>
            },
    ConditionExpression = <<"#version = :old_version OR attribute_not_exists(#version)">>,
    ExpressionAttributeNames = #{<<"#version">> => <<"Version">>},
    ExpressionAttributeValues = #{<<":old_version">> => <<"totally_a_uuid">>},

    Request = make_put_request(TableName, Item,
                               ConditionExpression,
                               ExpressionAttributeNames,
                               ExpressionAttributeValues),
    Expected = #{<<"ConditionExpression">> =>
                     <<"#version = :old_version OR attribute_not_exists(#version)">>,
                 <<"ExpressionAttributeNames">> =>
                     #{<<"#version">> => <<"Version">>},
                 <<"ExpressionAttributeValues">> =>
                     #{<<":old_version">> =>
                           #{<<"S">> => <<"totally_a_uuid">>}},
                 <<"Item">> =>
                     #{<<"Student">> => #{<<"S">> => <<"Foo">>},
                       <<"Version">> => #{<<"S">> => <<"totally_a_uuid">>}},
                 <<"TableName">> => <<"foo">>},
    ?assertEqual(Expected, Request).

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
