# BitCask Java

This is an implementation (in progress) of Basho's bitcask; a local 
high-performance key/value store which is the preferred backend for 
Basho Riak.

I'm doing this in order to

* Understand more of bitcask / riak
* Provide tools to be able to read (write?) bitcask files without using Erlang, which could make people more comfortable with using the backend.

At this stage, we only have an implementation of the module used to 
read and write individual bitcask data files (and hint files).  

You're welcome to help!

Kresten
