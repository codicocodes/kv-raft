https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks -- argument that main/mrcoordinator.go passes to MakeCoordinator(). So, each mapper needs to create nReduce intermediate files for consumption by the reduce tasks.
The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
A mr-out-X file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value. Have a look in main/mrsequential.go for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.

- [x] One way to get started is to modify mr/worker.go's Worker() to send an RPC to the coordinator asking for a task. 
- [x] Then modify the coordinator to respond with the file name of an as-yet-unstarted map task. 
- [x] Then modify the worker to read that file and call the application Map function, as in mrsequential.go.
The application Map and Reduce functions are loaded at run-time using the Go plugin package, from files whose names end in .so.

NOTE: probably don't need to change anything in mr/ directory
If you change anything in the mr/ directory, you will probably have to re-build any MapReduce plugins you use, with something like go build -race -buildmode=plugin ../mrapps/wc.go

This lab relies on the workers sharing a file system. That's straightforward when all workers run on the same machine, but would require a global filesystem like GFS if the workers ran on different machines.

A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.

The worker's map task code will need a way to store intermediate key/value pairs in files in a way that can be correctly read back during reduce tasks. One possibility is to use Go's encoding/json package. To write key/value pairs in JSON format to an open file:


- [x] map task number, and reduce task number
- [x] The map part of your worker can use the ihash(key) function (in worker.go) to pick the reduce task for a given key.
- [x] You can steal some code from mrsequential.go for reading Map input files
- [] for sorting intermedate key/value pairs between the Map and Reduce 
- [] and for storing Reduce output in files.
- [] The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data.
- [] Workers will sometimes need to wait, e.g. reduces can't start until the last map has finished. 
    - [] One possibility is for workers to periodically ask the coordinator for work, sleeping with time.Sleep() between each request. 
    - [] Another possibility is for the relevant RPC handler in the coordinator to have a loop that waits, either with time.Sleep() or sync.Cond. Go runs the handler for each RPC in its own thread, so the fact that one handler is waiting won't prevent the coordinator from processing other RPCs.
- [] The coordinator can't reliably distinguish between crashed workers, workers that are alive but have stalled for some reason, and workers that are executing but too slowly to be useful. The best you can do is have the coordinator wait for some amount of time, and then give up and re-issue the task to a different worker. For this lab, have the coordinator wait for ten seconds; after that the coordinator should assume the worker has died (of course, it might not have).
- [] To ensure that nobody observes partially written files in the presence of crashes, the MapReduce paper mentions the trick of using a temporary file and atomically renaming it once it is completely written. You can use ioutil.TempFile to create a temporary file and os.Rename to atomically rename it.


- [] The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the number of reduce tasks -- argument that main/mrcoordinator.go passes to MakeCoordinator(). 
- [] So, each mapper needs to create nReduce intermediate files for consumption by the reduce tasks.
