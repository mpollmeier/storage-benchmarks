import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.env._

object XodusBenchmark extends App {
  import Setup._
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicLong(0)
  val executorService = Executors.newFixedThreadPool(threadCount)

  val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)
  val env = Environments.newInstance(storageFile.toFile)

  val start = System.currentTimeMillis
  env.executeInTransaction { txn =>
    val store = env.openStore("nodes", StoreConfig.WITH_DUPLICATES, txn)
//    val futures = 0.until(2).map(fileName).map { fileName =>
      val futures = 0.until(FileCount).map(fileName).map { fileName =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val fileIn = new FileInputStream(fileName)
          var valueCount = 0
          val tmpArray = new Array[Byte](ValueByteCount)
          val byteUtils = new ByteUtils
          while (valueCount < ValueCountPerFile) {
            val bytesRead = fileIn.read(tmpArray)
            assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
            val idBytes = new ArrayByteIterable(byteUtils.longToBytes(currId.getAndIncrement))
            store.put(txn, idBytes, new ArrayByteIterable(tmpArray))
            valueCount += 1
          }
          fileIn.close
        }
      })
    }

    println("awaiting all futures...")
    futures.foreach(_.get)
    println("finished all futures - closing storage now")
    txn.commit()
    env.close
    storageFile.toFile.delete
  }
  val elapsedTime = System.currentTimeMillis - start
  //  assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms")
  executorService.shutdown()

}
