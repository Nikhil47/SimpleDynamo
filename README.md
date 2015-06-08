# SimpleDynamo
Chain Replicated Nodes providing Linearized storage

This is an implementation of a Dynamo-style key-value storage. This assignment is about implementing a simplified version
of Dynamo. There are three main pieces which have been implemented: 
1) Partitioning, 
2) Replication, and 
3) Failure handling.

The main goal is to provide both availability and linearizability at the same time. In other words, this implementation always
performs read and write operations successfully even under failures. At the same time, a read operation always returns the most
recent value. 

Features:
 - Just as the original Dynamo, every node can know every other node. This means that each node knows all other nodes in the
 system and also knows exactly which partition belongs to which node; any node can forward a request to the correct node without
 using a ring-based routing.
 - Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor of the key), and the
 coordinator is in charge of serving read/write operations.
 - Uses Chain replication - a write operation always comes to the first partition; then it propagates to the next two partitions
 in sequence. The last partition returns the result of the write.
 - A read operation always comes to the last partition and reads the value from the last partition.
 
 Other Details:
 - Implemented in Android (Android Studio)
 - Tested for 5 parallel processes (AVDs)
