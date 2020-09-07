# 6.824-Distributed-Systems

Personal Solution to MIT's course 6.824-Distributed-Systems Lab, [Spring 2020](https://pdos.csail.mit.edu/6.824/schedule.html). Course Channel on Youtube: [link](https://www.youtube.com/channel/UC_7WrbZTCODu1o_kfUMq88g)

## Course Labs
* Lab 1: MapReduce *(Done)*
* Lab 2: Raft *(Done)*
    * Part A: Leader Election
    * Part B: Append New Log Entries
    * Part C: State Persistence
* Lab 3: Fault-tolerant Key/Value Service
* Lab 4: Sharded Key/Value Service


## Labs Details
### Lab 1: MapReduce
Simple implementation for [MapReduce](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf)

### Lab 2: Raft
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
#### Part A
Why the read operation has to be replicated and not just return the value in the Leader KV Storage ?
As mentioned in Raft's paper section 8:
> Read-only operations can be handled without writing
anything into the log. However, with no additional measures, this would run the risk of returning stale data, since
the leader responding to the request might have been superseded by a newer leader of which it is unaware.

