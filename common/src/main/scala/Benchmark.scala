import java.io.FileInputStream
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext

object Benchmark {

  def run(put: (Long, Array[Byte]) => Unit,
          closeStorage: () => Unit): Unit = {
    import Setup._

    val start = System.currentTimeMillis
    val currId = new AtomicInteger(0)
    val fileIdx = new AtomicInteger(0)

    val threadCount = 1
//    val executorService = Executors.newFixedThreadPool(threadCount)

    0.until(threadCount).foreach { threadId =>
      new Thread(new Runnable {
        def run = {
          println(s"starting thread with id=$threadId")
          while (fileIdx.get < FileCount) {
            val fileIn = new FileInputStream(fileName(fileIdx.get))
            var valueCount = 0
            val tmpArray = new Array[Byte](ValueByteCount)
            while (valueCount < ValueCountPerFile) {
              val bytesRead = fileIn.read(tmpArray)
              assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
              put(currId.getAndIncrement, tmpArray)
              valueCount += 1
            }
            fileIn.close
            fileIdx.incrementAndGet
          }
          println(s"finishing thread with id=$threadId")
        }
      }).start

//      while (fileIdx.get < FileCount) {
//        val fileIn = new FileInputStream(fileName(fileIdx.get))
//        var valueCount = 0
//        val tmpArray = new Array[Byte](ValueByteCount)
//        while (valueCount < ValueCountPerFile) {
//          val bytesRead = fileIn.read(tmpArray)
//          assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
//          put(currId.getAndIncrement, tmpArray)
//          valueCount += 1
//        }
//        fileIn.close
//        fileIdx.incrementAndGet
//      }
    }

    closeStorage()

    val elapsedTime = System.currentTimeMillis - start
    println(s"total time: ${elapsedTime}ms for $currId entries")
  }

}
