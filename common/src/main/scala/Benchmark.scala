import java.io.FileInputStream
import java.util.concurrent.atomic.AtomicInteger

object Benchmark {

  def run(put: (Long, Array[Byte]) => Unit,
          close: () => Unit): Unit = {
    import Setup._

    val start = System.currentTimeMillis
    val currId = new AtomicInteger(0)
    val fileIdx = new AtomicInteger(0)

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

    close()

    val elapsedTime = System.currentTimeMillis - start
    println(s"total time: ${elapsedTime}ms for $currId entries")
  }

}
