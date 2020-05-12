import java.io.{File, FileInputStream}
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
  val futures = 0.until(FileCount).map { fileIdx =>
    executorService.submit(new Runnable {
      override def run = {
        val fileIn = new FileInputStream(fileName(fileIdx))
        var valueCount = 0
        val tmpArray = new Array[Byte](ValueByteCount)
//        val byteUtils = new ByteUtils
        env.executeInTransaction { txn =>
          val store = env.openStore(s"nodes-$fileIdx", StoreConfig.WITHOUT_DUPLICATES, txn)
          while (valueCount < ValueCountPerFile) {
            val bytesRead = fileIn.read(tmpArray)
            assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
            val idBytes = new ArrayByteIterable(new ByteUtils().longToBytes(currId.getAndIncrement))
//            val idBytes = new ArrayByteIterable(byteUtils.longToBytes(currId.getAndIncrement))
//            store.put(txn, idBytes, new ArrayByteIterable(tmpArray))
            store.add(txn, idBytes, new ArrayByteIterable(tmpArray))
            valueCount += 1
          }
          txn.flush
          txn.commit()
//          println(store.getName + " " + store.count(txn))
          store.close
        }
        fileIn.close
      }
    })
  }

  println("awaiting all futures...")
  futures.foreach(_.get)
  println("finished all futures - closing storage now")
  env.close

  val elapsedTime = System.currentTimeMillis - start
    assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms. storage=$storageFile")
  executorService.shutdown()

//  Thread.sleep(10000)

}

object XodusRead extends App {
  val storageDir = new File("target/XodusBenchmark$637704148292244438")
  val env = Environments.newInstance(storageDir)
  env.executeInTransaction { txn =>
    0.until(Setup.FileCount).map { fileIdx =>
      val store = env.openStore(s"nodes-$fileIdx", StoreConfig.USE_EXISTING, txn)
      println(store.count(txn))
    }
  }
}

object XodusSimpleWrite extends App {
  val storageDir = new File("target/XodusSimple")
//  val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)
//  val env = Environments.newInstance(storageFile.toFile)
  val env = Environments.newInstance(storageDir)
  val txn = env.beginTransaction()
  val store = env.openStore("nodes", StoreConfig.WITHOUT_DUPLICATES, txn)
//  val store = env.openStore("nodes", StoreConfig.WITH_DUPLICATES, txn)

  (0).until(16).foreach { id =>
    val idBytes = new ByteUtils().longToBytes(id)
    println(idBytes.mkString(","))
    val idByteIterable = new ArrayByteIterable(idBytes)
    val value = new ArrayByteIterable(new ByteUtils().longToBytes(id + 100))
    store.put(txn, idByteIterable, value)
//    txn.flush
  }
  txn.flush
  println(store.count(txn))
  txn.commit()
  store.close()
  env.close()
}

object XodusSimpleRead extends App {
  val storageDir = new File("target/XodusSimple")
  val env = Environments.newInstance(storageDir)
  val txn = env.beginReadonlyTransaction()
  val store = env.openStore("nodes", StoreConfig.USE_EXISTING, txn)
  val byteUtils = new ByteUtils

//  val key = new ArrayByteIterable(byteUtils.longToBytes(1L))
//  val value = new ArrayByteIterable(byteUtils.longToBytes(2L))
//  store.put(txn, key, value)
  println(store.count(txn))

  txn.abort()
  store.close()
  env.close()
}

