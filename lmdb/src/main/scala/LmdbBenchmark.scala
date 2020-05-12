import java.io.FileInputStream
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer.allocateDirect

import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.DbiFlags.MDB_DUPSORT
import org.lmdbjava.DirectBufferProxy.PROXY_DB
import org.lmdbjava.Env
import org.lmdbjava.Env.create
import org.lmdbjava.GetOp.MDB_SET
import org.lmdbjava.SeekOp.MDB_FIRST
import org.lmdbjava.SeekOp.MDB_LAST
import org.lmdbjava.SeekOp.MDB_PREV

object LmdbBenchmark extends App {
  import Setup._
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicLong(0)
  val executorService = Executors.newFixedThreadPool(threadCount)

  val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)

  val env: Env[ByteBuffer] = create()
    // LMDB also needs to know how large our DB might be. Over-estimating is OK.
    .setMapSize(10_485_760)
    // LMDB also needs to know how many DBs (Dbi) we want to store in this Env.
    .setMaxDbs(1)
    // Now let's open the Env. The same path can be concurrently opened and
    // used in different processes, but do not open the same path twice in
    // the same process at the same time.
    .open(storageFile.toFile)

  // We need a Dbi for each DB. A Dbi roughly equates to a sorted map. The
  // MDB_CREATE flag causes the DB to be created if it doesn't already exist.
  val db = env.openDbi("nodes", MDB_CREATE)

  // We want to store some data, so we will need a direct ByteBuffer.
  // Note that LMDB keys cannot exceed maxKeySize bytes (511 bytes by default).
  // Values can be larger.

//  val key = allocateDirect(env.getMaxKeySize)
//  val value = allocateDirect(700)
//  key.put("greeting".getBytes(UTF_8)).flip();
//  value.put("Hello world".getBytes(UTF_8)).flip();
//  val valSize = value.remaining

  // Now store it. Dbi.put() internally begins and commits a transaction (Txn).
//  db.put(key, value)

  val start = System.currentTimeMillis
  val futures = 0.until(FileCount).map(fileName).map { fileName =>
//    val futures = 0.until(2).map(fileName).map { fileName =>
    executorService.submit(new Runnable {
      override def run = {
        val fileIn = new FileInputStream(fileName)
        var valueCount = 0
        val tmpArray = new Array[Byte](ValueByteCount)
        while (valueCount < ValueCountPerFile) {
          val bytesRead = fileIn.read(tmpArray)
          assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
          val key = allocateDirect(env.getMaxKeySize)
//          val key = ByteBuffer.allocateDirect(java.lang.Long.BYTES)
//          key.putLong(0, currId.getAndIncrement).flip
//          key.put("greeting".getBytes("UTF-8")).flip
          key.put(currId.toString.getBytes("UTF-8")).flip
          val value = allocateDirect(tmpArray.length)
          value.put(tmpArray).flip
          db.put(key, value)
          valueCount += 1
        }
        fileIn.close
      }
    })
  }

  println("awaiting all futures...")
  futures.foreach(_.get)
  println("finished all futures - closing storage now")
  env.close
//
  val elapsedTime = System.currentTimeMillis - start
    assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms. storage=$storageFile")
  executorService.shutdown()
}
