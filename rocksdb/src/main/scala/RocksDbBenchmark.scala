import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import org.rocksdb._
import org.rocksdb.util.SizeUnit

object RocksDbBenchmark extends App {
  import Setup._
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicLong(0)
  val executorService = Executors.newFixedThreadPool(threadCount)

  val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)
  RocksDB.loadLibrary()
  val options = new Options().setCreateIfMissing(true)
  val db = RocksDB.open(options, storageFile.toString)

  val start = System.currentTimeMillis
  val futures = 0.until(FileCount).map(fileName).map { fileName =>
//    val futures = 0.until(2).map(fileName).map { fileName =>
    executorService.submit(new Runnable {
      override def run = {
        val fileIn = new FileInputStream(fileName)
        var valueCount = 0
        val tmpArray = new Array[Byte](ValueByteCount)
        val byteUtils = new ByteUtils
        while (valueCount < ValueCountPerFile) {
          val bytesRead = fileIn.read(tmpArray)
          assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
          val key = byteUtils.longToBytes(currId.getAndIncrement)
          db.put(key, tmpArray)
          valueCount += 1
        }
        fileIn.close
      }
    })
  }

  println("awaiting all futures...")
  futures.foreach(_.get)
  println("finished all futures - closing storage now")
  db.close

  val elapsedTime = System.currentTimeMillis - start
  assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms. storage=$storageFile")
  executorService.shutdown()
}
