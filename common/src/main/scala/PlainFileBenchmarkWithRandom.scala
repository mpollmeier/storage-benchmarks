import java.io.{File, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import Setup._

import scala.util.Random

/* write a plain file to see the theoretical maximum */
object PlainFileBenchmarkWithRandom extends App {
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicInteger(0)
  val executorService = Executors.newFixedThreadPool(threadCount)
  val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)
  val rnd = new Random

  val start = System.currentTimeMillis
  val futures = 0.until(FileCount).map { inputFileIdx =>
    executorService.submit(new Runnable {
      override def run(): Unit = {
        var valueCount = 0
        val tmpArray = new Array[Byte](ValueByteCount)
        val byteUtils = new ByteUtils
        // TODO write to one file rather than $threadCount
        val fileOutStream = new FileOutputStream(new File(s"$storageFile/$inputFileIdx"))
        while (valueCount < ValueCountPerFile) {
          rnd.nextBytes(tmpArray)
          val idBytes = byteUtils.longToBytes(currId.getAndIncrement)
          fileOutStream.write(idBytes)
          fileOutStream.write(tmpArray)
          valueCount += 1
        }
      }
    })
  }

  println("awaiting all futures...")
  futures.foreach(_.get)
  println("finished all futures - closing storage now")

  storageFile.toFile.delete

  val elapsedTime = System.currentTimeMillis - start
  assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms")
  executorService.shutdown()
}
