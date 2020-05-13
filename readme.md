# key value stores: performance comparison 

## multithreaded writes
Input: precomputed files with random bytes as input, see Setup.scala
Benchmarking machine: my 6 core (12 threads) laptop 
Raw results:

```
threads|mvstore|leveldb-java|plain|mapdb|xodus|lmdb-jni|rocksdb-jni
   1   | 13.3s |     19s    |31.1s| 18s | 22s |  slow  |    35s
   2   |  8.6s |     27s    |18.6s| 11s | 24s |        |    30s
   4   |  9.0s |     28s    |10.8s| 11s | 29s |        |    25s
   8   |  9.2s |     29s    | 7.8s| 11s | 30s |        |    22s
  12   |  9.3s |     36s    | 6.4s| 11s | 30s |        |    37s
```
  
* note: `plain` is the 'control group', i.e. it's simply writing the results to output files
* note: db files must be written to a normal dir, not the typically memory-mapped /tmp
* mapdb: final 'combine' of separate trees not included in measurement, i.e. final solution would be slower
* other candidates, not (yet) tested:
  * leveldb-jni
  * riak-jni
  * https://github.com/jordw/heftydb
  * cassandra sstable (embedded in same jvm!)
