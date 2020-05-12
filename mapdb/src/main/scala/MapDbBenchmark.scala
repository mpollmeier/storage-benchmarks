import java.io.FileInputStream
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import kotlin.Pair
import org.mapdb._

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object MapDbBenchmark extends App {
  import Setup._
  val threadCount = args match {
    case Array(threadCount) => threadCount.toInt
    case _ => throw new AssertionError("expected exactly one argument: threadCount")
  }

  val currId = new AtomicLong(0)
  val executorService = Executors.newFixedThreadPool(threadCount)

  val storageFile = Files.createTempFile(Paths.get("target"), getClass.getName, "bin")
  storageFile.toFile.delete
  val db = DBMaker.fileDB(storageFile.toFile)
    .concurrencyScale(threadCount)
    .fileLockDisable()
    .fileChannelEnable()
//    .cleanerHackEnable()
//    .fileLockWait()
//    .fileMmapPreclearDisable()
//    .fileMmapEnable()
    .executorEnable()
    .make()

  val start = System.currentTimeMillis

//  val iter: java.util.Iterator[Pair[java.lang.Long, Array[Byte]]] =
//    0.until(FileCount).map(fileName).flatMap { fileName =>
//        val fileIn = new FileInputStream(fileName)
//        var valueCount = 0
//        val tmpArray = new Array[Byte](ValueByteCount)
//        val builder = ArrayBuffer.newBuilder[Pair[java.lang.Long, Array[Byte]]]
//        while (valueCount < ValueCountPerFile) {
//          val bytesRead = fileIn.read(tmpArray)
//          assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
//          builder.addOne(new Pair(currId.getAndIncrement, tmpArray))
//          valueCount += 1
//        }
//        fileIn.close
//        builder.result
//    }.iterator.asJava

//  val nodesTreeMap: BTreeMap[java.lang.Long, Array[Byte]] = db.treeMap("nodes")
//    .keySerializer(Serializer.LONG)
//    .valueSerializer(Serializer.BYTE_ARRAY)
//    .createFrom(iter)

  type TreeMap = BTreeMap[java.lang.Long, Array[Byte]]

  val trees = ArrayBuffer.newBuilder[TreeMap]

  val futures = 0.until(FileCount).map(fileName).map { fileName =>
//  val futures = 0.until(2).map(fileName).map { fileName =>
    executorService.submit(new Runnable {
      override def run() = {
        val iter: java.util.Iterator[Pair[java.lang.Long, Array[Byte]]] = {
          val fileIn = new FileInputStream(fileName)
          var valueCount = 0
          val tmpArray = new Array[Byte](ValueByteCount)
          val builder = ArrayBuffer.newBuilder[Pair[java.lang.Long, Array[Byte]]]
          while (valueCount < ValueCountPerFile) {
            val bytesRead = fileIn.read(tmpArray)
            assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
            builder.addOne(new Pair(currId.getAndIncrement, tmpArray))
            valueCount += 1
          }
          fileIn.close
          builder.result
        }.iterator.asJava

        val nodesTreeMap: TreeMap = db.treeMap(fileName)
          .keySerializer(Serializer.LONG)
          .valueSerializer(Serializer.BYTE_ARRAY)
          .createFrom(iter)
        // TODO only do at the very end?
//        db.commit
//        fileIn.close
        trees.addOne(nodesTreeMap)
      }
    })
  }

  println("awaiting all futures...")
  futures.foreach(_.get)
  println("finished all futures - closing storage now")

//  val b: TreeMap = ???
//  b.putAll(b)

//  trees.result.toList.reduce(_.merge())

  // TODO combine all trees

  db.commit
  db.close
  storageFile.toFile.delete

  val elapsedTime = System.currentTimeMillis - start
//  assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
  println(s"$threadCount threads: completed in ${elapsedTime}ms")
  executorService.shutdown()

}
