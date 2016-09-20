package org.shadowmask.web.utils

import java.security.MessageDigest

import org.apache.commons.codec.binary.Hex

abstract class Hasher {
  def hasher():MessageDigest
  def hashb2b(bytes:Array[Byte]):Array[Byte] = hasher.digest(bytes)
  def hashb2s(bytes:Array[Byte]):String = new String(Hex.encodeHex(hashb2b(bytes)))
  def hashs2b(str:String):Array[Byte] = hashb2b(str.getBytes)
  def hashs2s(str:String):String = hashb2s(str.getBytes())
}
class  MD5Hasher private extends Hasher {
  override def hasher(): MessageDigest = MessageDigest.getInstance("MD5")
}
object MD5Hasher{
  val instance = new MD5Hasher
  def apply(): MD5Hasher = instance
}