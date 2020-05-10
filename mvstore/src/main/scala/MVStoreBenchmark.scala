import java.nio.file.Files

import org.h2.mvstore.{MVMap, MVStore}

object MVStoreBenchmark extends App {

  val mvstoreFile = Files.createTempFile(getClass.getName, "bin")
  val mvstore = new MVStore.Builder()
    .fileName(mvstoreFile.toAbsolutePath.toString)
    .autoCommitBufferSize(1024 * 8)
    .open()
  val mvMap: MVMap[Long, Array[Byte]] = mvstore.openMap("nodes")

  def writeEntry(id: Long, bytes: Array[Byte]): Unit = {
    mvMap.put(id, bytes)
  }

  Benchmark.run(
    put = (id, bytes) => mvMap.put(id, bytes),
    closeStorage = mvstore.close
  )

}
