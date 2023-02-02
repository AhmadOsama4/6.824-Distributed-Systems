# 6.824-Distributed-Systems

Personal Solution to MIT's course 6.824-Distributed-Systems Lab, [Spring 2020](https://pdos.csail.mit.edu/6.824/schedule.html). Course Channel on Youtube: [link](https://www.youtube.com/channel/UC_7WrbZTCODu1o_kfUMq88g)

Implementation was run against tests provided in the lab that simulate a distributed environment (hosts failing, connection issues, ..etc) but it was not run on a real distributed environment.

## Course Labs
* Lab 1: MapReduce *[Done]*
* Lab 2: Raft *[Done]*
    * Part A: Leader Election
    * Part B: Append New Log Entries
    * Part C: State Persistence
* Lab 3: Fault-tolerant Key/Value Service *[Done]*
* Lab 4: Sharded Key/Value Service *[Incomplete - some tests failing]*

## Labs Details
### Lab 1: MapReduce
Simple implementation for [MapReduce](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf).

### Lab 2: Raft
An implementation of [Raft](http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf) consensus algorithm.    
Some issues that I encountered when implementing this Lab.
#### Part 2A

Tip #1
Do not check for a timeout using a sleep as this could lead to unexpected timeouts
```go
    timeoutTime := time.Now() + RANDOM_TIME

    for {
        if time.Now().Sub(timeoutTime) {
            // start new election
        }

        time.Sleep(sleepPeriod)
    }
```

### Lab 3
Using the implementation of Raft in Lab 2 to implement a fault tolerant Key/Value storage. Raft is used for electing a leader and for fault tolerance.  

![](https://github.com/AhmadOsama4/6.824-Distributed-Systems/blob/master/fault_tolerant_DB_diagram.jpeg)

Notes: 
* Number of nodes can be any odd number (to be able to form a quorum) not necessarily 5.
* Both read/write operation go through the leader, a quorum is to be formed before the leader responds with success. (Implementation does not handle eventually consistent reads) 
* Sharding the Data is in Lab 4

#### Part A
Why the read operation has to be replicated and not just return the value in the Leader KV Storage ?
As mentioned in Raft's paper section 8:
> Read-only operations can be handled without writing
anything into the log. However, with no additional measures, this would run the risk of returning stale data, since
the leader responding to the request might have been superseded by a newer leader of which it is unaware.

It is important to note that Distributed databases like [DynamoDB](https://docs.aws.amazon.com/dynamodb/index.html) allow both eventually consistent and strongly consistent reads, see: [Read Consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html). However the implementation in the assignment is only for strongly consistent reads.

### Lab 4
Extension to Lab 3, large data will not fit into 1 host so they are to be sharded across N Shards.

![](https://github.com/AhmadOsama4/6.824-Distributed-Systems/blob/master/Sharded_KV_DB.jpeg)

Notes: 
* Each shard has data replicated to its hosts.
* A client library determines which shard should be called to fetch a key.
* Hosts within the same shard use Raft for fault tolerance.
* A Shard responds with a success to read/write when leader forms a quorom.
