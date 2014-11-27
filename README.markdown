ddar
====

ddar is a free de-duplicating archiver for Unix. Save space, bandwidth and time by storing duplicate regions of data only once.

For details, see [the wiki][1].

Installation
------------

Some [pre-built packages][1] are available.

Building from Source
--------------------

For Debian/Ubuntu packages, just install build dependencies (see
`debian/control`) and then run `debuild` as usual.

Other distributions: install [protobuf][2], run `make pydist` and then `python
setup.py install`.

Contributing
------------

Please submit contributions as pull requests on [github][3], following these
guidelines:

 * Maintain compatibility with Python 2.5.

 * Keep behaviour similar to `ar(1)` and `tar(1)` (principle of least surprise).
   Options, behaviour and formats that make sense to match should match as
   closely as possible.

 * The manpage should be the source of all knowledge. All options and behaviour
   should be documented there.


[1]: https://github.com/basak/ddar/wiki
[2]: http://code.google.com/p/protobuf/
[3]: https://github.com/basak/ddar
