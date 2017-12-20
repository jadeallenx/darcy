-module(darcy_test).
-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

make_item(Name, Subject) ->
    #{ <<"Student">> => Name,
       <<"Subject">> => Subject,
       <<"Grades">> => {list, [ rand:uniform(100) || _ <- lists:seq(1, rand:uniform(5)) ]}
     }.

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

make_map(Students) ->
    to_map(Students, #{}).

to_map([], Acc) -> Acc;
to_map([H|T], Acc) ->
    Name = maps:get(<<"Student">>, H),
    Subj = maps:get(<<"Subject">>, H),
    NewAcc = case maps:is_key(Name, Acc) of
        true ->
            S = maps:get(Name, Acc),
            maps:put(Name, ordsets:add_element(Subj, S), Acc);
        false ->
            maps:put(Name, ordsets:add_element(Subj, ordsets:new()), Acc)
    end,
    to_map(T, NewAcc).

lookup_item(Records) ->
    Name = one_of(maps:keys(Records)),
    Subject = one_of(ordsets:to_list(maps:get(Name, Records))),
    {Name, Subject}.

batch_test() ->
    _ = darcy:start(),
    Client = aws_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }],
    Keys = [<<"Student">>, <<"Subject">>],
    TableSpec = darcy:make_table_spec(<<"Grades">>, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),
    Students = [ make_item(one_of(names()), one_of(subjects())) ||
                 _ <- lists:seq(1, 30) ],
    %?debugFmt("students:~p", [Students]),
    Records = make_map(Students),
    %?debugFmt("records: ~p", [Records]),
    _ = darcy:batch_write_items(Client, <<"Grades">>, Students),
    %?debugFmt("batch write result: ~p", [Result]),

    Lookups = [ lookup_item(Records) || _ <- lists:seq(1, 10) ],
    Result1 = lists:map(fun({N, S}) ->
                            darcy:get_item(Client,
                                           <<"Grades">>,
                                           #{ <<"Student">> => N, <<"Subject">> => S })
                        end,
                        Lookups),

    ?debugFmt("get item results: ~p", [Result1]),
    ok.

example_test() ->
    _ = darcy:start(),
    Client = aws_client:make_local_client(<<"access">>, <<"secret">>, <<"12000">>),
    Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }],
    Keys = [<<"Student">>, <<"Subject">>],
    TableSpec = darcy:make_table_spec(<<"Grades">>, Attributes, Keys),
    ok = darcy:make_table_if_not_exists(Client, TableSpec),
    Grades = #{ <<"Student">> => <<"Foo">>, <<"Subject">> => <<"Bar">>,
                <<"Grades">> => {list, [75,80,90]} },
    ok = darcy:put_item(Client, <<"Grades">>, Grades),
    {ok, Result} = darcy:get_item(Client, <<"Grades">>, #{ <<"Student">> => <<"Foo">>,
                                                           <<"Subject">> => <<"Bar">> }),
    ?assertEqual(darcy:clean_map(Grades), Result).

