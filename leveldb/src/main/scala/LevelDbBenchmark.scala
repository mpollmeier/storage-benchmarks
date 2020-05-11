import java.io.FileInputStream
import java.nio.file.Files
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._

object LevelDbBenchmark extends App {
  import Setup._
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicInteger(0)
  val executorService = Executors.newFixedThreadPool(threadCount)

  val storageFile = Files.createTempDirectory(getClass.getName)
  val options = new Options()
    .createIfMissing(true)

  val db = factory.open(storageFile.toFile, options)

  val start = System.currentTimeMillis
//  val futures = 0.until(2).map(fileName).map { fileName =>
  val futures = 0.until(FileCount).map(fileName).map { fileName =>
    executorService.submit(new Runnable {
      override def run(): Unit = {
        val fileIn = new FileInputStream(fileName)
        var valueCount = 0
        val tmpArray = new Array[Byte](ValueByteCount)
        val byteUtils = new ByteUtils
        val batch = db.createWriteBatch
        while (valueCount < ValueCountPerFile) {
          val bytesRead = fileIn.read(tmpArray)
          assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
          val idBytes = byteUtils.longToBytes(currId.getAndIncrement)
//          db.put(idBytes, tmpArray)
          batch.put(idBytes, tmpArray)
          valueCount += 1
        }
        db.write(batch)
        batch.close()
        fileIn.close
      }
    })
  }

  println("awaiting all futures...")
  futures.foreach(_.get)
  println("finished all futures - closing storage now")

  db.close
  storageFile.toFile.delete

  val elapsedTime = System.currentTimeMillis - start
  assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms")
  executorService.shutdown()

}
