-module(darcy_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

make_item(Name, Subject) ->
    Grades = [ rand:uniform(100) || _ <- lists:seq(1, rand:uniform(5)) ],
    Avg = lists:sum(Grades) / length(Grades),
    Bool = choose_bool(rand:uniform()),
    #{ <<"Student">> => Name,
       <<"Subject">> => Subject,
       <<"Grades">> => {list, Grades},
       <<"Average">> => Avg,
       <<"hasTextbook">> => Bool
     }.

choose_bool(X) when X > 0.50 -> true;
choose_bool(_) -> false.

names() -> [<<"Alice">>, <<"Bob">>, <<"Carol">>, <<"Dave">>, <<"Ethan">>,
            <<"Francine">>, <<"Gemma">>, <<"Hugh">>, <<"Ichabod">>,
            <<"Julie">>, <<"Kayla">>, <<"Lawrence">>, <<"Mickey">>,
            <<"Nora">>, <<"Ophelia">>, <<"Porter">>, <<"Quentin">>,
            <<"Roger">>, <<"Stewie">>, <<"Terry">>, <<"Ursula">>,
            <<"Wanda">>, <<"Xavier">>, <<"Yolanda">>, <<"Zach">>].

subjects() -> [<<"Math">>, <<"English">>, <<"Handwriting">>, <<"Science">>,
	       <<"Geography">>, <<"Civics">>, <<"Literature">>, <<"Knitting">>].

one_of(L) ->
    lists:nth(rand:uniform(length(L)), L).

deduplicate(Students) ->
    R = to_map(Students, #{}),
    lists:flatten(maps:fold(fun(_K, V, Acc) -> [ V | Acc ] end,
              [],
              R)).

to_map([], Acc) -> Acc;
to_map([H|T], Acc) ->
    Name = maps:get(<<"Student">>, H),
    Subj = maps:get(<<"Subject">>, H),
    Key = << Name/binary, Subj/binary >>,
    to_map(T, maps:put(Key, H, Acc)).

lookup_item(Records) ->
    R = one_of(Records),
    {maps:get(<<"Student">>, R), maps:get(<<"Subject">>, R)}.

make_table_name() ->
    Id = integer_to_binary(rand:uniform(100000)),
    << <<"Grades">>/binary, Id/binary >>.

find_student(Name, L) ->
    lists:filter(fun(E) -> has_name(Name, E) end, L).

has_name(Name, M) ->
    case maps:get(<<"Student">>, M) of
        Name -> true;
        _ -> false
    end.

same_count(Expected, #{ <<"Table">> := D }) ->
    #{ <<"ItemCount">> := C } = D,
    Expected == C.

make_random_query(Records) ->
    R = one_of(Records),
    Name = maps:get(<<"Student">>, R),
    KV = <<"Student = :sn">>,
    EV = darcy:to_ddb(#{ <<":sn">> => Name }),
    {#{ <<"KeyConditionExpression">> => KV,
       <<"ExpressionAttributeValues">> => EV },
     find_student(Name, Records)}.

cas_success_test() ->
    _ = darcy:start(),
    Client = darcy_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }],
    Keys = [<<"Student">>, <<"Subject">>],
    TableId = make_table_name(),
    TableSpec = darcy:make_table_spec(TableId, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),

    GradesV1 = #{ <<"Student">> => <<"Foo">>,
                  <<"Subject">> => <<"Bar">>,
                  <<"Grades">> => {list, [75,80,90]},
                  <<"Average">> => 81.66666666666667,
                  <<"Version">> => <<"totally_a_uuid">>
                },
    ok = darcy:put_item(Client, TableId, GradesV1),

    GradesV2 = #{ <<"Student">> => <<"Foo">>,
                  <<"Subject">> => <<"Bar">>,
                  <<"Grades">> => {list, [75,80,90]},
                  <<"Average">> => 90.0,
                  <<"Version">> => <<"the_new_uuid">>
                },
    ConditionMap = #{condition_expression => <<"#version = :old_version OR attribute_not_exists(#version)">>,
                     expression_attribute_names => #{<<"#version">> => <<"Version">>},
                     expression_attribute_values => #{<<":old_version">> => <<"totally_a_uuid">>}
                    },
    ok = darcy:put_item(Client, TableId, GradesV2, ConditionMap),

    {ok, Result} = darcy:get_item(Client, TableId, #{ <<"Student">> => <<"Foo">>,
                                                      <<"Subject">> => <<"Bar">> }),
    Expected = #{<<"Average">> => 90,
                 <<"Grades">> => "KPZ",
                 <<"Student">> => <<"Foo">>,
                 <<"Subject">> => <<"Bar">>,
                 <<"Version">> => <<"the_new_uuid">>},
    ?assertEqual(Expected, Result),

    _ = darcy:delete_table(Client, TableId).

cas_failure_test() ->
    _ = darcy:start(),
    Client = darcy_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }],
    Keys = [<<"Student">>, <<"Subject">>],
    TableId = make_table_name(),
    TableSpec = darcy:make_table_spec(TableId, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),

    GradesV1 = #{ <<"Student">> => <<"Foo">>,
                  <<"Subject">> => <<"Bar">>,
                  <<"Grades">> => {list, [75,80,90]},
                  <<"Average">> => 81.66666666666667,
                  <<"Version">> => <<"totally_a_uuid">>
                },
    ok = darcy:put_item(Client, TableId, GradesV1),

    GradesV2 = #{ <<"Student">> => <<"Foo">>,
                  <<"Subject">> => <<"Bar">>,
                  <<"Grades">> => {list, [75,80,90]},
                  <<"Average">> => 90.0,
                  <<"Version">> => <<"the_new_uuid">>
                },
    ConditionMap = #{condition_expression => <<"#version = :old_version OR attribute_not_exists(#version)">>,
                     expression_attribute_names => #{<<"#version">> => <<"Version">>},
                     expression_attribute_values => #{<<":old_version">> => <<"not_the_same">>}
                    },
    {error, {{_, <<"The conditional request failed">>}, _}} = darcy:put_item(Client, TableId, GradesV2, ConditionMap),

    _ = darcy:delete_table(Client, TableId).

batch_test() ->
    _ = darcy:start(),
    Client = darcy_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> } ],
    Keys = [<<"Student">>, <<"Subject">>],
    TableId = make_table_name(),
    TableSpec = darcy:make_table_spec(TableId, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),
    Students = [ make_item(one_of(names()), one_of(subjects())) ||
                 _ <- lists:seq(1, 65) ],
    Records = deduplicate(Students),
    ok = darcy:batch_write_items(Client, TableId, Records),
    {ok, Desc} = darcy:describe_table(Client, TableId),
    ?assert(same_count(length(Records), Desc)),

    Lookups = ordsets:from_list([ lookup_item(Records) || _ <- lists:seq(1, 20) ]),
    Result1 = lists:map(fun({N, S}) ->
                                darcy:get_item(Client,
                                           TableId,
                                           #{ <<"Student">> => N, <<"Subject">> => S })
                        end,
                        ordsets:to_list(Lookups)),

    ?assert(lists:all(fun({ok, _}) -> true;
                              (_)  -> false
                      end,
                      Result1)),

    LookupKeys = [ #{ <<"Student">> => N, <<"Subject">> => S } || {N, S} <- ordsets:to_list(Lookups) ],
    {ok, Results} = darcy:batch_get_items(Client, TableId, LookupKeys),
    ?assert(length(Results) == length(ordsets:to_list(Lookups))),

    {Cond, Expected} = make_random_query(Records),
    {ok, #{ <<"Count">> := C, <<"Items">> := I } } = darcy:query(Client, TableId, Cond),
    ?assert(length(Expected) == C),
    ?assert(lists:sort([ darcy:clean_map(E) || E <- Expected ] ) == lists:sort(I)),

    %% gives "ACTIVE" status instead of "DELETING"
    _ = darcy:delete_table(Client, TableId).

example_test() ->
    _ = darcy:start(),
    Client = darcy_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }],
    Keys = [<<"Student">>, <<"Subject">>],
    Tid = make_table_name(),
    TableSpec = darcy:make_table_spec(Tid, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),
    Grades = #{ <<"Student">> => <<"Foo">>, <<"Subject">> => <<"Bar">>,
                <<"Grades">> => {list, [75,80,90]},
                <<"Average">> => 81.66666666666667
              },
    ok = darcy:put_item(Client, Tid, Grades),
    {ok, Result} = darcy:get_item(Client, Tid, #{ <<"Student">> => <<"Foo">>,
                                                           <<"Subject">> => <<"Bar">> }),
    ?assertEqual(darcy:clean_map(Grades), Result),
    %% gives "ACTIVE" status instead of "DELETING"
    _ = darcy:delete_table(Client, Tid).

scan_test() ->
    _ = darcy:start(),
    Client = darcy_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }],
    Keys = [<<"Student">>, <<"Subject">>],
    Tid = make_table_name(),
    TableSpec = darcy:make_table_spec(Tid, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),
    Students = [ make_item(one_of(names()), one_of(subjects())) ||
                 _ <- lists:seq(1, 500) ],
    Records = deduplicate(Students),
    ok = darcy:batch_write_items(Client, Tid, Records),
    {ok, Desc} = darcy:describe_table(Client, Tid),
    ?assert(same_count(length(Records), Desc)),

    {ok, Results} = darcy:scan_all(Client, Tid, #{}),
    ?assertEqual(length(Records), length(Results)),

    %% filter students without textbooks
    Expr = darcy:to_ddb(#{ <<":val">> => false }),
    Filter = #{ <<"FilterExpression">> => <<"hasTextbook = :val">>,
                <<"ExpressionAttributeValues">> => Expr
              },
    {ok, Filtered} = darcy:scan_all(Client, Tid, Filter),
    Expected = lists:filter(fun(#{<<"hasTextbook">> := false}) -> true; (_) -> false end, Records),
    ?assertEqual(length(Expected), length(Filtered)),


    %% try a parallel scan
    {ok, F0} = darcy:scan_parallel(Client, Tid, Filter, fun(X) -> X end, 2),
    ?assertEqual(length(Expected), length(F0)),

    %% gives "ACTIVE" status instead of "DELETING"
    _ = darcy:delete_table(Client, Tid).
