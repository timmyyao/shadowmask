package org.shadowmask.web.common.user

import java.util.ResourceBundle

import authentikat.jwt.{JsonWebToken, JwtClaimsSet, JwtHeader}
import org.shadowmask.web.utils.MD5Hasher

/**
  * abstract parent class
  */
abstract class UserAuth {
  def auth(user: Option[User]): Option[Token]

  def verify(token: Option[Token]): Option[User]
}

/**
  * token
  *
  * @param token
  */
case class Token(token: String)

/**
  * user
  *
  * @param username
  * @param password
  */
case class User(username: String, password: String)

/*----------------------------------------------------------------
  Auth strategy
 ----------------------------------------------------------------*/


/**
  *
  */
class PlainUserAuth private extends UserAuth {
  /**
    * auth one user and return a token
    *
    * @param user
    * @return
    */
  override def auth(user: Option[User]): Option[Token] = if (user != None && ConfiguredUsers().verify(user.get.username, user.get.password)) {
    Some(Token(JsonWebToken(JwtHeader("HS256"), JwtClaimsSet(Map("username" -> user.get.username)), ConfiguredUsers().secret)))
  } else None

  /**
    * verify a user with a token
    *
    * @param token
    * @return
    */
  override def verify(token: Option[Token]): Option[User] = {
    if(JsonWebToken.validate(token.get.token,ConfiguredUsers().secret)){
      token.getOrElse(Token("")).token match {
        case JsonWebToken(header, value, key) => {
          Option(User(value.asSimpleMap.get.get("username").getOrElse(""), ""))
        }
        case _ => None
      }
    }else None

  }
}

object PlainUserAuth {
  val instance = new PlainUserAuth
  def apply (): PlainUserAuth = instance
}


class LdapServerAuth private extends UserAuth{
  override def auth(user: Option[User]): Option[Token] = ???
  override def verify(token: Option[Token]): Option[User] = ???
}

class MockLdapServerAuth private extends UserAuth{
  override def auth(user: Option[User]): Option[Token] = PlainUserAuth().auth(user)
  override def verify(token: Option[Token]): Option[User] = PlainUserAuth().verify(token)
}

object MockLdapServerAuth {
  val instance = new MockLdapServerAuth
  def apply (): MockLdapServerAuth = instance
}

/*----------------------------------------------------------------
  Auth strategy
 ----------------------------------------------------------------*/



/**
  * user auth provider
  */
trait AuthProvider{
  def getAuth:UserAuth
}
trait PlainAuthProvider extends AuthProvider{
  def getAuth() = PlainUserAuth()
}
trait ConfiguredAuthProvider extends AuthProvider{
  def getAuth() ={
    ShadowmaskProp().authType match {
      case "ldap" => MockLdapServerAuth()
      case "plain"=>  PlainUserAuth()
    }
  }
}




/**
  * users configured in admin.properties
  */
class ConfiguredUsers private {
  val (userMap, forbiddenUsers, secret) = {
    val resource = ResourceBundle.getBundle("admin")
    var usermap = Map[String, String]()
    var forbiddenSet = Set[String]()
    resource.getString("admin.users").split(";").foreach((s) => {
      val arr = s.split(":")
      usermap += (arr(0) -> arr(1))
    })
    resource.getString("admin.users.forbidden").split(";").foreach(s => forbiddenSet += s)
    (usermap, forbiddenSet, resource.getString("admin.users.secret"))
  }

  def verify(username: String, password: String): Boolean = username != null && password != null &&
    userMap.keySet.contains(username) && !forbiddenUsers.contains(username) && {
    val pwd = userMap.get(username).getOrElse("")
    pwd.equals(MD5Hasher().hashs2s(password))
  }
}

object ConfiguredUsers {
  val instance = new ConfiguredUsers
  def apply() = instance
}
///////////ldap properties ////
class LdapProp  private (val url:String)
object LdapProp{
  val instance = new LdapProp({
    val resource = ResourceBundle.getBundle("ldap")
    resource.getString("user.auth.ldap.ldapserver")
  })
  def apply(): LdapProp = instance
}

/////////////shadow mask properties /////////////
class ShadowmaskProp private (val authType:String)
object ShadowmaskProp{
  val instance = new ShadowmaskProp({
    val resource = ResourceBundle.getBundle("shadowmask")
    resource.getString("user.auth.method")
  })
  def apply(): ShadowmaskProp = instance
}




