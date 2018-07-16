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

## Build prerequisites

The project depends on another GitHub project `@ornamental/sqlite-statements`. 
Build and install the corresponding artifact into local Maven repository before building
this project.