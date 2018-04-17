[![Build Status](https://travis-ci.org/mrallen1/darcy.svg?branch=master)](https://travis-ci.org/mrallen1/darcy)

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
[aws-erlang][1] and [jsone][2] for JSON encoding/decoding. The HTTP
client used is [hackney][7].  At some future point, it may be
desirable to make the JSON library and/or HTTP client configurable.

At this point in time, you'll still have to know quite a bit about
how Dynamo does what it does. Sorry.  This library was more about
getting something working quickly than ironing out the finer nuances
of library API design.

That being said, I am definitely interested in making this library
easier and more robust to use, so pull requests are definitely welcome. 
I ask that before you send any giant pull requests, open an issue first to
discuss the proposal.

Use
---
This library uses maps. Maps in, maps out. If you don't like maps or your
Erlang doesn't support maps, pick one of the 239,029 other Erlang libraries
that implement a Dynamo client without maps.

You'll want to install [local DynamoDB][5] to run this example (or indeed
the unit test.)

    1> darcy:start().
    2> Client = darcy_client:local_client(<<"access">>, <<"secret">>, <<"12000">>).
    3> Attributes = [{ <<"Student">>, <<"S">> }, { <<"Subject">>, <<"S">> }].
    4> Keys = [<<"Student">>, <<"Subject">>].
    5> TableSpec = darcy:make_table_spec(<<"Grades">>, Attributes, Keys).
    6> ok = darcy:make_table_if_not_exists(Client, TableSpec).
    7> Grade = #{ <<"Student">> => <<"Alice">>, <<"Subject">> => <<"Math">>, <<"Grades">> => {list, [75, 80, 90], <<"Average">> => 81.66666666666667 } }.
    8> ok = darcy:put_item(Client, <<"Grades">>, Grade).
    9> {ok, Grade} = darcy:get_item(Client, <<"Grades">>, #{ <<"Student">> => <<"Alice">>, <<"Subject">> => <<"Math">> }).

### Dynamo Data Types and Erlang ###
DynamoDB requires that data sent to it have its data types explicitly marked as
part of serializing a request to the service. [Read more about that here][6].

In darcy, the functions `to_map` and `to_ddb` translate back and forth 
between Erlang-y data structures and a Dynamo encoded data structure.

Erlang has some ambiguities in its data model when it's serialized to JSON.  In
particular, it is difficult to distinguish between a list of elements, and a
string.  It is also difficult to tell if a binary string should be treated as
text or as a literal binary blob.  To address these ambiguities, darcy encodes
values using the following rules:

1. If a value is binary, it is interpreted as a string.
2. If a value is a number type (integer or float), it is interpreted as a number.
3. If a value is a map, it is interpreted as a map (recursively applying these
   rules to its values)
4. If a value is a list, attempt to interpret it as a string; if that fails, we
   treat it as a literal list.
5. The atoms `undefined` and `null` are treated as the DynamoDB `NULL` value.
6. The atoms `true` and `false` are treated as Dynamo boolean types.

For ambiguous cases, we use tagged tuples, which have the following meanings:

* `{list, [ term() ]}` is turned into a Dynamo list value, recursively applying
data type encoding rules to each list element.

* `{blob, Blob}` takes a literal binary blob and uses the `default_encode` and
`default_decode` functions to encode those values for storage inside Dynamo.
These default functions can be changed by providing MFA tuples in darcy's
application environment configuration. **NOTE** Dynamo requires that blobs use
base64 encoding; binary values that are not encoded will be rejected.

* `{number_set, Set}` recursively turns elements from Set into a Dynamo encoded
numeric set.

* `{string_set, Set}` recursively turns elements from Set into a Dynamo encoded
string set.

If you have an Erlang-y map using tagged tuples, you can call the function
`clean_map` which will return a "naked" map. This is useful for your 
non-Dynamo code which doesn't know or care about the ambiguity of Erlang
data type encoding.

#### Recommendations ####

I would recommend you try to isolate the code which interacts with Dynamo so
that this encoding issue can be dealt with in a single place in your application.
Elsewhere in the application, try to deal with only "pure" Erlang data structures.
That will help isolate the pieces of code which need to translate back and forth
between the two worlds.

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
[5]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
[6]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes
[7]: https://hexdocs.pm/hackney/
