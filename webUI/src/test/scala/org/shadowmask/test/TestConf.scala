package org.shadowmask.test

import org.junit.Test
import org.junit.Assert._
import org.shadowmask.web.common.user.{LdapProp, ShadowmaskProp}
/**
  * Created by liyh on 16/9/28.
  */
class TestConf {

  @Test
  def testLdapConf():Unit = {
    assertNotNull(LdapProp().url)
    println(LdapProp().url)
  }
  @Test
  def testShadowmaskProp():Unit  = {
    assertNotNull(ShadowmaskProp().authType)
    println(ShadowmaskProp().authType)
  }

}
