# BitCask Java

This is an implementation (in progress) of Basho's bitcask; a local 
high-performance key/value store which is the preferred backend for 
Basho Riak.

I'm doing this in order to

* Understand more of bitcask / riak
* Provide tools to be able to read (write?) bitcask files without using Erlang, 
  which could make people more comfortable with using the backend.

bitcask-java tries to e faithful to the original bitcask, so it should be interoperable
and be able to run concurrently with the original.

At this stage, you can create/open a bitcask and get/put data to it. For sample
usage see [BitCaskTest.java](https://github.com/krestenkrab/bitcask-java/blob/master/src/test/java/com/trifork/bitcask/BitCaskTest.java)

Merging is not implemented yet.

You're welcome to help!

Kresten
