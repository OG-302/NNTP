# An NNTP library

This is a Java implementation of the NNTP protocol defined by [RFC-3977](https://datatracker.ietf.org/doc/html/rfc3977) and consists of:
1) a __Server__ component which responds to NNTP Client requests (such as LISTGROUP, ARTICLE, POST, IHAVE, etc.), and
2) a __Peer__ component which connects to other NNTP Servers to exchange new newsgroups and articles.

The library is implemented as the Java package ([org.anarplex.lib.nntp](https://github.com/OG-302/NNTP/tree/main/src/main/java/org/anarplex/lib/nntp)) and is intended to be used by applications needing NNTP server/peer capabilities.  

As a general-purpose Library, several implementation details are deferred for the  Application to implement.  Such details include:
1. Persistence Storage, where newsgroups, articles and various other domain entities are stored.  This Library uses an abstract domain-specific Entity Model ([PersistenceService](https://github.com/OG-302/NNTP/tree/main/src/main/java/org/anarplex/lib/nntp/PersistenceService.java)) that Applications will implement and could be realized as a relational database, File System, or other storage mechanism,
2. Policies for accepting Articles.  This library uses the abstract [PolicyService class](https://github.com/OG-302/NNTP/tree/main/src/main/java/org/anarplex/lib/nntp/PolicyService.java) to offer the Application an opportunity to review and determine which Articles are to be published, ignored, or banned. And,
3. Network Transport which is abstracted to [NetworkServices class](https://github.com/OG-302/NNTP/tree/main/src/main/java/org/anarplex/lib/nntp/NetworkService.java) to allow Applications to implement custom transport layers which could, for example, use TCP/IP sockets or other transport protocols.



