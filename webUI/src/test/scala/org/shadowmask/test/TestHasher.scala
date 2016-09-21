package org.shadowmask.test

import org.junit.Test
import org.shadowmask.web.utils.MD5Hasher
import org.junit.Assert._
/**
  * Created by liyh on 16/9/20.
  */
class TestHasher {

  @Test
  def testMd5Hasher() = {
    val str = MD5Hasher().hashs2s("123456")
    val str1 = MD5Hasher().hashb2s("123456".getBytes())
    val bs = MD5Hasher().hashs2b("123456")
    val bs2 = MD5Hasher().hashb2b("123456".getBytes())
    assertArrayEquals(bs,bs2)
    assertEquals(str,str1)
    assertEquals(MD5Hasher().hashs2s("shadowmask"),"ad98bea499d76c4bccbaeb3e08e863d4")
  }
}
