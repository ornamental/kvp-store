# A simplistic embedded MVCC key-value storage

The storage organizes the data supplied to it in a log-like manner, writing
the records one after another until a file reaches maximum allowed size, after which
a new file is created and written to, and so on. The metadata is stored in an SQLite
database.

The storage allows any number of concurrent reading transactions and a single 
writing transaction (not blocked by the reading transactions). Each reading transactions
observes the storage snapshot which was relevant at the moment when the transaction
was started. The writing transaction operates on the most recent storage state.

If a record is deleted and there are no readers which observe the corresponding storage
state, it may be garbage-collected. The garbage collection does not affect
a file unless the records ready to be collected occupy sufficiently much space in it.
The garbage collection on a file moves all the remaining records to the end of the 'log' and
removes the file that has become redundant.

The storage handles very big writing transactions (up to terabytes) without much impact
on the RAM. It works best with shorter keys.

## Usage
The principal interface is `VersionedStorage` serving to begin
reading and modifying transactions on an MVCC storage. Read-only transactions are represented
by the `Reader` interface, modifying transactions are represented by the `Writer` interface,
both extending `java.lang.AutoCloseable` (in fact, `Writer` is a `Reader`).
The supplied concrete implementation of the `VersionedStorage` interface is the class 
`GroupingFileStorage`.

Keys and values supplied to the storage transaction methods are represented 
by the `Key` interface, whose implementations must be able to return a byte array 
representing a key, and the `Value` interface, whose implementations must be aware 
of the size of data they hold and (in its most general form) be able to return a 
`ReadableByteChannel` as the data supplier.

Note that the `Key` implementations **must** override `equals(Object)` and `hashCode()` 
methods so that instances returning equal binary representations are equal. The default
behaviour is implemented in the `AbstractKey` class.

There exist ready implementations of the `Value` interface wrapping `byte[]` and `InputStream`
instances (`ByteArrayValue` and `InputStreamValue`, respectively).

## Build prerequisites

The project depends on another GitHub project `@ornamental/sqlite-statements`. 
Build and install the corresponding artifact into local Maven repository before building
this project.
