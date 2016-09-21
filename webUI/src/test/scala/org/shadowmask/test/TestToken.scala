package org.shadowmask.test

import authentikat.jwt.{JsonWebToken, JwtClaimsSet, JwtHeader}
import org.junit.Test
import org.junit.Assert._
import org.shadowmask.web.common.user.{PlainUserAuth, Token, User}
/**
  * Created by liyh on 16/9/20.
  */
class TestToken {

  @Test
  def testJsonWeb()={
    val header = JwtHeader("HS256")
    val claimsSet = JwtClaimsSet(Map("username" -> "shadowmask"))
    val token = JsonWebToken(header, claimsSet, "12313212")
    assertNotNull(token)
    val res =
    token match {
      case JsonWebToken(header, claimsSet, token)=>
        claimsSet.asSimpleMap.get.get("username")

    }
    assertEquals(res.get,"shadowmask")
  }

  @Test
  def testPlainAuth()= {
    val res = PlainUserAuth().auth(Some(User("shadowmask","shadowmask")))
    val res1 = PlainUserAuth().auth(Some(User("shadowmask","shadowmask1")))
    assertNotEquals(res,None)
    assertEquals(res1,None)
    val v1 = PlainUserAuth().verify(res)
    val v2 = PlainUserAuth().verify(Some(Token("afasdfa")))
    assertNotEquals(v1,None)
    assertEquals(v2,None)
  }
}
