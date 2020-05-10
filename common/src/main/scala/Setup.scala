import java.io.{File, FileOutputStream}

import scala.util.Random

object Setup extends App {
  def ValueByteCount = 450
  def ValueCountPerFile = 8000
  def FileCount = 1024
  def RootDir = "/tmp/storage-benchmark-inputBytes"
  def bytesPerFile = ValueByteCount * ValueCountPerFile
  def fileName(fileIdx: Int) = s"$RootDir/$fileIdx"

  val rnd = new Random

  val start = System.currentTimeMillis
  new File(RootDir).mkdir
  0.until(FileCount).foreach { fileIdx =>
    val outFile = new FileOutputStream(fileName(fileIdx))
    outFile.write(rnd.nextBytes(bytesPerFile))
    outFile.close
  }

  val elapsedTime = System.currentTimeMillis - start
  println(s"created $FileCount files in $elapsedTime ms")
}

