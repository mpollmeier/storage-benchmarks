import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import Setup._

/* write a plain file to see the theoretical maximum */
object PlainFileBenchmark extends App {
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicInteger(0)
  val executorService = Executors.newFixedThreadPool(threadCount)
  val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)

  val start = System.currentTimeMillis
  val futures = 0.until(FileCount).map { inputFileIdx =>
    executorService.submit(new Runnable {
      override def run(): Unit = {
        val fileIn = new FileInputStream(fileName(inputFileIdx))
        var valueCount = 0
        val tmpArray = new Array[Byte](ValueByteCount)
        val byteUtils = new ByteUtils
        val fileOutStream = new FileOutputStream(new File(s"$storageFile/$inputFileIdx"))
        while (valueCount < ValueCountPerFile) {
          val bytesRead = fileIn.read(tmpArray)
          assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
          val idBytes = byteUtils.longToBytes(currId.getAndIncrement)
          fileOutStream.write(idBytes)
          fileOutStream.write(tmpArray)
          valueCount += 1
        }
        fileIn.close
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
