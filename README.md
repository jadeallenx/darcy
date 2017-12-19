darcy
=====
Darcy aims to be an DynamoDB client for Erlang that doesn't suck too much.

**Q.** Good lord, why are you writing another #@%#$! dynamo client? Aren't there
enough in the world? 

**A.** Thanks for asking, self. Yes, there sure are a lot of Dynamo clients out
there in the world and they're all mostly sucky. Why?  Because they're old and
don't use OTP maps. They use proplists and nested deep proplists. Ugh. Ain't
nobody got time for *that*.

It's *Frankensteen!*
--------------------
This implementation rips the Dynamo and AWS implementation off from
[aws-erlang][1] and [jsone][2] for JSON encoding/decoding.  

Use
---
This library uses maps. Maps in, maps out. If you don't like maps or your
Erlang doesn't support maps, pick one of the 239,029 other Erlang libraries
that implement a Dynamo client without maps.

    1> Client = aws_client:local_client(<<"access">>, <<"secret">>, 12000).
    2> Key1 = darcy:to_ddb(#{ <<"Key1">> => <<"some string">> }).
    3> Key2 = darcy:to_ddb(#{ <<"Key2">> => 42 }).
    4> Req = #{              <<"TableName">> => <<"example">>, 
                                   <<"Key">> => maps:merge(Key1, Key2),
                <<"ReturnConsumedCapacity">> => "TOTAL" }.
    5> RawItem = darcy:get_item(Client, Req).
    6> M = darcy:clean_map(darcy:to_map(maps:get(<<"Item">>, RawItem))).

Build
-----

    $ rebar3 compile

Name
----
Yes, this library is named for the adorable hedgehog [Darcy][3] because she is
quite a bit cuter than a yak. Although [baby yaks are adorable][4].

[1]: https://github.com/jkakar/aws-erlang
[2]: https://github.com/sile/jsone
[3]: https://www.instagram.com/darcytheflyinghedgehog/
[4]: https://bing.com/images/search?q=baby+yaks
