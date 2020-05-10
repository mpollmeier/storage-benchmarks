import java.io.FileInputStream
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

object Benchmark {

  def run(threadCount: Int,
          put: (Long, Array[Byte]) => Unit,
          closeStorage: () => Unit): Unit = {
    import Setup._

    val currId = new AtomicInteger(0)
    val executorService = Executors.newFixedThreadPool(threadCount)

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
    assert(currId.get == 8192000, s"expected to have handled 8192000 entries, but actually handled $currId")
    println(s"$threadCount threads: completed in ${elapsedTime}ms")
    executorService.shutdown()
  }

}
