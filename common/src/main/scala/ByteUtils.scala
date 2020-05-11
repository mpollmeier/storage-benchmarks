
/* _NOT_ thread safe */
class ByteUtils {
  import java.nio.ByteBuffer
  private val buffer = ByteBuffer.allocate(java.lang.Long.BYTES)

  def longToBytes(l: Long) = {
    buffer.putLong(0, l)
    buffer.array
  }

  def bytesToLong(bytes: Array[Byte]) = {
    buffer.put(bytes, 0, bytes.length)
    buffer.flip
    buffer.getLong
  }
}

