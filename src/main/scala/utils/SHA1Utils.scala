package moe.brianhsu.fmmaster.util

import java.io._
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.security.DigestInputStream
import java.security.MessageDigest

object SHA1Utils {

  val DigestLower = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f')
  val BufferSize = 1024 * 4

  def encodeHexString(data: Array[Byte]): String = {
    def digit1(b: Byte) = DigestLower((0xF0 & b) >>> 4).toString
    def digit2(b: Byte) = DigestLower((0x0F & b)).toString

    // Each byte is present by two ASCII character in HEX mode.
    data.map(b => digit1(b) + digit2(b)).mkString
  }

  def sha1Checksum(file: String): String = sha1Checksum(new File(file))

  def sha1Checksum(file: File): String = {
    val fileInputStream = new BufferedInputStream(new FileInputStream(file))
    var messageDigest = MessageDigest.getInstance("SHA-1")
    val digestInputStream = new DigestInputStream(fileInputStream, messageDigest)
    val buffer = new Array[Byte](BufferSize)

    Iterator.
      continually(digestInputStream.read(buffer, 0, BufferSize)).
      zipWithIndex.
      takeWhile{case (byteCount, index) => byteCount != -1 && index <= 100000}.
      size
    
    val digest = digestInputStream.getMessageDigest().digest
    digestInputStream.close()
    encodeHexString(digest)
  }
}

