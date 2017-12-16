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
This library uses (a fork of) [erliam][1] for credential management and request
signing, rips the Dynamo implementation off from [aws-erlang][2] (a specification 
that thankfully hasn't changed much since 2012), [lhttpc][4] for a HTTP client
and [jsone][3] for JSON encoding/decoding.  

Use
---
This library uses maps. Maps in, maps out. If you don't like maps or your
Erlang doesn't support maps, pick one of the 239,029 other Erlang libraries
that implement a Dynamo client without maps.

Build
-----

    $ rebar3 compile

Name
----
Yes, this library is named for the adorable hedgehog [Darcy][5] because she is
quite a bit cuter than a yak. Although [baby yaks are adorable][6].

[1]: https://github.com/mrallen1/erliam
[2]: https://github.com/jkakar/aws-erlang
[3]: https://github.com/sile/jsone
[4]: https://github.com/alertlogic/lhttpc
[5]: https://www.instagram.com/darcytheflyinghedgehog/
[6]: https://bing.com/images/search?q=baby+yaks
