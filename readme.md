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

### Single threaded writes -- plain vanilla benchmark

```
 export SBT_OPTS="-Xmx13G"
 sbt "common/runMain Stuff"
```
 yields:

```
============Run number 0
Generate data in 1064.0551 ms aka 0.4224977898231022 GB/s
gen scalaHash 112.733143 ms aka 0.00887050581034541 GE/s
serialize scalaHash in 2068.320914 ms aka 0.21735550076248952 GB/s, with overhead factor 1.0667325875793192
Write + delete scalaHash in 323.120703 ms aka 1.4841552631803974 GB/s
total scalaHash 2504.3250909999997 ms

gen THash 127.683233 ms aka 0.00783188188851703 GE/s
serialize THash in 899.788316 ms aka 0.4996296573382044 GB/s, with overhead factor 1.0444883568707288
Write + delete THash in 330.39939499999997 ms aka 1.4211925388059503 GB/s
total THash 1358.035308 ms

total mvstore 37241.979263 ms


....


============Run number 9
Generate data in 940.379794 ms aka 0.47808815849567265 GB/s
gen scalaHash 125.217188 ms aka 0.007986124077470898 GE/s
serialize scalaHash in 1599.572573 ms aka 0.2810653618277562 GB/s, with overhead factor 1.066729097059239
Write + delete scalaHash in 339.49242599999997 ms aka 1.4126524519283385 GB/s
total scalaHash 2064.408611 ms

gen THash 135.28029899999999 ms aka 0.007392059356699086 GE/s
serialize THash in 639.0214179999999 ms aka 0.7035514480987239 GB/s, with overhead factor 1.0444860298591647
Write + delete THash in 330.31037599999996 ms aka 1.421646745362913 GB/s
total THash 1104.9120269999999 ms

total mvstore 35013.563042 ms

```
We can conclude:
1. Graal jit-warmup does not significantly matter and is swamped by GC jitter (ok, I didn't know that -- I am amazed that the JVM engineers manage to sqeeze meaningful perf out of language as horrible as java/scala)
2. Don't use a database when you just want to store a smallish (1M keys, average 450 byte values) key-value map
3. Don't multithread or use fancy-pants packages until the plain vanilla solution has been evaluated
4. Always include units. Unitless times are meaningless. The correct metrics are GB/s (gigabyte per second throughput), latency (irrelevant for our use) and GE/s (giga-entries per second, or alternatively nanoseconds/entry); alternatively cycles-per-byte is a very clear unit that permits to quickly gauge whether we do something stupid.
5. Don't use benchmark frameworks before plain vanilla benchmarks. First vanilla benchmark, then fancy-pants framework -- if required.

In prod, we should also run the thing through zstandard or LZ4 (in fast mode, with option to disable for people running a sane filesystem like ZFS with LZ4). That is expected to even save time (as opposed to raw storage on an SSD on top of dm-crypt). Do not use stupid libraries/algorithms like libzip/deflate for this -- deflate is dog slow. This is not applicable to this test on random data -- we'd need to test compressibility on real data.

In prod, storing serialized (byte-array) nodes is also a bad idea. We need a stringpool, and dedup strings in the cpg.