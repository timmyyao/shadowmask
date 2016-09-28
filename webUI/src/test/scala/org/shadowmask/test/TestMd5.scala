package org.shadowmask.test

import java.security.MessageDigest

import org.apache.commons.codec.binary.Hex
import org.junit.Test
import org.junit.Assert._

/**
  * Created by liyh on 16/9/20.
  */
class TestMd5 {

  @Test
  def testMd5() = {
    val md5 = MessageDigest.getInstance("MD5")
    val str = new String(Hex.encodeHex(md5.digest("shadowmask".getBytes)))
    assertTrue(str=="ad98bea499d76c4bccbaeb3e08e863d4")
  }
}
