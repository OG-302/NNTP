# An NNTP library

This is a Java implementation of the NNTP protocol defined by [RFC-3977](https://datatracker.ietf.org/doc/html/rfc3977) and consists of:
1) a __Server__ component which responds to NNTP Client requests (such as LISTGROUP, ARTICLE, POST, IHAVE, etc.), and
2) a __Peer__ component which connects to other NNTP Servers to exchange new newsgroups and articles.

The library is implemented as the Java package [org.anarplex.lib.nntp](https://github.com/OG-302/NNTP/tree/main/src/main/java/org/anarplex/lib/nntp).
It is a library and not a standalone application.  It is intended to be used by applications and thus has several extension points such as:
1. the ability to define a custom Persistence Store in which newsgroups and articles are stored (this could be based on a database or a file system),
2. the ability to define custom Policies under which to handle NNTP Client POSTs, etc based on many factors, and
3. the ability to define a custom Network Transport Layer (not restricted to TCP/IP).


