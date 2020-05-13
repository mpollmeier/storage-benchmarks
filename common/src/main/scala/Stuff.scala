import java.io.{ByteArrayOutputStream, File, FileOutputStream, ObjectOutputStream}
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import org.h2.mvstore.{MVMap, MVStore}
import gnu.trove.map.hash


/* write a plain file to see the theoretical maximum */
object Stuff extends App {
  val szMin = 250
  val szMax = 650
  val nEntries = 1000*1000
  val rng = new scala.util.Random()
  var tic = java.lang.System.nanoTime()
  var toc = java.lang.System.nanoTime()
  var nBytes = 0
  val data = mutable.ArrayBuffer[Array[Byte]]()
  val dataidxs = mutable.ArrayBuffer[Long]()
  val usedIdxs = mutable.HashSet[Long]()
  for(n <- Range(0, 10)) {
    println(s"\n\n============Run number ${n}")
    tic = java.lang.System.nanoTime()

    genData()
    toc = java.lang.System.nanoTime()
    println(s"Generate data in ${(toc - tic) * 1e-6} ms aka ${nBytes * 1.0 / (toc - tic)} GB/s")
    tic = java.lang.System.nanoTime()

    // scala hash
    tic = java.lang.System.nanoTime()
    benchScalamap()
    toc = java.lang.System.nanoTime()
    println(s"total scalaHash ${(toc - tic) * 1e-6} ms\n")
    tic = java.lang.System.nanoTime()


    tic = java.lang.System.nanoTime()
    benchThashMap()
    toc = java.lang.System.nanoTime()
    println(s"total THash ${(toc - tic) * 1e-6} ms\n")
    tic = java.lang.System.nanoTime()

    tic = java.lang.System.nanoTime()
    benchMVStore()
    toc = java.lang.System.nanoTime()
    println(s"total mvstore ${(toc - tic) * 1e-6} ms\n")
    tic = java.lang.System.nanoTime()

  }



  def writeFile(bytes: Array[Byte]) = {
    val storageFile = Files.createTempDirectory(Paths.get("target"), getClass.getName)
    val fileOutStream = new FileOutputStream(new File(s"${storageFile}/tmp.bin"))
    fileOutStream.write(bytes)
    fileOutStream.close()
    storageFile.toFile.delete

  }

  def serializeData(obj: Any):Array[Byte] = {
    val stream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(obj)
    oos.close()
    stream.toByteArray
  }

  def genHash():mutable.HashMap[Long, Array[Byte]] = {
    val dataHash = mutable.HashMap[Long, Array[Byte]]()
    for(i <- Range(0, nEntries)){
      dataHash.put(dataidxs(i), data(i))
    }
    dataHash
  }
  def genTHash():gnu.trove.map.hash.TLongObjectHashMap[Array[Byte]] = {
    val dataHash = new gnu.trove.map.hash.TLongObjectHashMap[Array[Byte]]()
    for(i <- Range(0, nEntries)){
      dataHash.put(dataidxs(i), data(i))
    }
    dataHash
  }

  def genData():Unit = {
    data.clear
    dataidxs.clear
    usedIdxs.clear
    nBytes = 0
    for(i <- Range(0, nEntries)){
      dataidxs.append(genIdx())
      val pt = rng.nextBytes(rng.between(szMin, szMax))
      data.append(pt)
      nBytes += pt.size
    }
  }

  def genIdx(): Long = {
    var res = rng.nextLong()
    while(!usedIdxs.add(res)){
      res =  rng.nextLong()
    }
    res
  }

  def benchScalamap():Unit = {
    var tic = java.lang.System.nanoTime()
    var toc = java.lang.System.nanoTime()

    val dataHash = genHash()
    toc = java.lang.System.nanoTime()
    println(s"gen scalaHash ${(toc - tic) * 1e-6} ms aka ${nEntries * 1.0 / (toc - tic)} GE/s")
    tic = java.lang.System.nanoTime()

    val bytes = serializeData(dataHash)
    toc = java.lang.System.nanoTime()
    println(s"serialize scalaHash in ${(toc - tic) * 1e-6} ms aka ${nBytes * 1.0 / (toc - tic)} GB/s, with overhead factor ${bytes.size * 1.0 / nBytes}")
    tic = java.lang.System.nanoTime()

    writeFile(bytes)
    toc = java.lang.System.nanoTime()
    println(s"Write + delete scalaHash in ${(toc - tic) * 1e-6} ms aka ${bytes.size * 1.0 / (toc - tic)} GB/s")
  }

  def benchThashMap():Unit = {
    var tic = java.lang.System.nanoTime()
    var toc = java.lang.System.nanoTime()

    val dataHash = genTHash()
    toc = java.lang.System.nanoTime()
    println(s"gen THash ${(toc - tic) * 1e-6} ms aka ${nEntries * 1.0 / (toc - tic)} GE/s")
    tic = java.lang.System.nanoTime()

    val bytes = serializeData(dataHash)
    toc = java.lang.System.nanoTime()
    println(s"serialize THash in ${(toc - tic) * 1e-6} ms aka ${nBytes * 1.0 / (toc - tic)} GB/s, with overhead factor ${bytes.size * 1.0 / nBytes}")
    tic = java.lang.System.nanoTime()

    writeFile(bytes)
    toc = java.lang.System.nanoTime()
    println(s"Write + delete THash in ${(toc - tic) * 1e-6} ms aka ${bytes.size * 1.0 / (toc - tic)} GB/s")
  }


  def benchMVStore():Unit = {
    val mvstoreFile = Files.createTempFile(Paths.get("target"), getClass.getName, "bin")
    val mvstore = new MVStore.Builder()
      .fileName(mvstoreFile.toAbsolutePath.toString)
      .autoCommitBufferSize(1024 * 8) //best option
      //    .autoCommitBufferSize(1024 * 8 * 8) //slower
      //    .autoCommitDisabled() //also slower than 8*1024

      //    .cacheConcurrency(8) // no difference
      //    .cacheSize(65000) //slower than default
      //    .cacheSize(65000 * 8) //slower than default
      .open()
    val mvMap: MVMap[Long, Array[Byte]] = mvstore.openMap("nodes")
    for(i <- Range(0, nEntries)){
      mvMap.put(dataidxs(i), data(i))
    }

    mvstore.close
    mvstoreFile.toFile.delete


  }

}
