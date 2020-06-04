# MIT-6.824-Distributed-Systems
MIT 6.824 Distributed System course materials, labs, assignments.

## Progress

- [x] Lab 1: MapReduce
- [X] Lab 2: Raft
    - [X] Lab 2A
    - [X] Lab 2B
    - [X] Lab 2C
- [ ] Lab 3: KV Raft
    - [ ] Lab 3A
    - [ ] Lab 3B
- [ ] Lab 4: Shared KV
    - [ ] Lab 4A
    - [ ] Lab 4B
    - [ ] Challenge 1
    - [ ] Challenge 2

## Specification

### Lab 1: MapReduce

Implemented a distributed MapReduce in Go, which consists of two programs, the master and the worker.

#### Worker

In a real system the workers would run on a bunch of different machines, in this lab works are running on a single machine. 

Worker's will keep talking to the master via RPC until all tasks are done.

DoMap function in the worker reads input files, then call map function written by users and save intermediate keys and values to a json file.

DoReduce function in the worker reads intermediate keys and values from json file and then calls the reduce function, then saves final results to mr-out-* files.

#### Master

Mater is responsible for distributing tasks to workers and tracking worker's progress, and resending failed tasks to other workers.

The master has three different stages: map stage, reduce stage, and finished stage. In map stage and reduce stage, master will send workers map tasks and reduce tasks, in finished stage, master will tell all workers all tasks are done next time workers ask the master for a new task, and workers would be shut down.

The master keeps tracks unassigned tasks and tasks in progress, if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), master would remove the task from in-progress set and put it into unassigned tasks queue. If the master is in map or reduce stage and there's no unassigned tasks, the master would tell workers that there is no task available.

#### Fault Tolerance

The master can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. For this lab, the master will wait for ten seconds; after that the master should assume the worker has died (of course, it might not have).

To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. For this lab, I use ioutil.TempFile in Go to create a temporary file and os.Rename in Go to atomically rename it after a task is finished.

### Lab 2: Raft

#### Finished lab 2, will finish this specification later...