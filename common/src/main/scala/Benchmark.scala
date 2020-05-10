import java.io.FileInputStream
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

object Benchmark {

  def run(put: (Long, Array[Byte]) => Unit,
          closeStorage: () => Unit): Unit = {
    import Setup._

    val currId = new AtomicInteger(0)
    val executorService = Executors.newFixedThreadPool(1)

    val start = System.currentTimeMillis
    val futures = 0.until(FileCount).map(fileName).map { fileName =>
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val fileIn = new FileInputStream(fileName)
          var valueCount = 0
          val tmpArray = new Array[Byte](ValueByteCount)
          while (valueCount < ValueCountPerFile) {
            val bytesRead = fileIn.read(tmpArray)
            assert(bytesRead == ValueByteCount, s"expected $ValueByteCount bytes to be read, but only got $bytesRead")
            put(currId.getAndIncrement, tmpArray)
            valueCount += 1
          }
          fileIn.close
        }
      })
    }

    println("awaiting all futures...")
    futures.foreach(_.get)
    println("finished all futures - closing storage now")

    closeStorage()

    val elapsedTime = System.currentTimeMillis - start
    println(s"completed in ${elapsedTime}ms for $currId entries")
    executorService.shutdown()
  }

}
