import java.nio.file.{Files, Paths}

import PlainFileBenchmark.getClass
import org.h2.mvstore.{MVMap, MVStore}

object MVStoreBenchmark extends App {
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val mvstoreFile = Files.createTempFile(Paths.get("target"), getClass.getName, "bin")
  val mvstore = new MVStore.Builder()
    .fileName(mvstoreFile.toAbsolutePath.toString)
    .autoCommitBufferSize(1024 * 8) //best option
//    .autoCommitBufferSize(1024 * 8 * 8) //slower
//    .autoCommitDisabled() //also slower than 8*1024

//    .cacheConcurrency(8) // no difference
//    .cacheSize(65000) //slower than default
//    .cacheSize(65000 * 8) //slower than default
    .open()
  val mvMap: MVMap[Long, Array[Byte]] = mvstore.openMap("nodes")

  def writeEntry(id: Long, bytes: Array[Byte]): Unit = {
    mvMap.put(id, bytes)
  }

  Benchmark.run(
    threadCount,
    put = (id, bytes) => mvMap.put(id, bytes),
    closeStorage = () => {
      mvstore.close
      mvstoreFile.toFile.delete
    }
  )

}
