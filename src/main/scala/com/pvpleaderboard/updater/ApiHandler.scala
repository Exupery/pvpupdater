package com.pvpleaderboard.updater

import java.io.IOException

import scala.io.Source
import scala.util.Try

import scalaj.http.Http

import org.slf4j.{ Logger, LoggerFactory }

import net.liftweb.json.{ JField, JValue, parse }
import net.liftweb.json.JsonParser.ParseException

/**
 * Send requests and receive responses to/from the Blizzard API
 */
class ApiHandler(val region: String) {
  private val BASE_URI: String = String.format("https://%s.api.blizzard.com", region)
  private val OAUTH_URI: String = "https://us.battle.net/oauth/token"
  private val CLIENT_ID: String = sys.env("BATTLE_NET_CLIENT_ID")
  private val SECRET: String = sys.env("BATTLE_NET_SECRET")
  private val TOKEN: String = createToken

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def getStatic(path: String): Option[JValue] = {
    val namespace: String = "static-" + region
    val dataPath = "/data/wow/" + path
    return get(dataPath, namespace)
  }

  def getDynamic(path: String): Option[JValue] = {
    val namespace: String = "dynamic-" + region
    val dataPath = "/data/wow/" + path
    return get(dataPath, namespace)
  }

  def getProfile(path: String): Option[JValue] = {
    val namespace: String = "profile-" + region
    val profilePath = "/profile/wow/character/" + path
    return get(profilePath, namespace)
  }

  private def get(path: String, namespace: String): Option[JValue] = {
    val requiredParams: String = String.format("?locale=en_US&access_token=%s&namespace=%s", TOKEN, namespace)
    val url: String = BASE_URI + path + requiredParams
    for (c <- 1 to 3) {
      try {
        val response: String = Source.fromURL(url).mkString

        return Option(parse(response))
      } catch {
        case p: ParseException => {
          logger.error("Unable to parse response: {}", p.toString())
          return Option.empty
        }
        case io: IOException => {
          /* Do not log for failures on character endpoints - many of
           * these 404 due to character transfer/rename/deletion/etc */
          if (c == 3 && !io.toString().contains("/character/")) {
            logger.error("GET failed: {}", io.toString())
          }
        }
      }
    }

    return Option.empty
  }

  private def createToken(): String = {
    val response = Http(OAUTH_URI)
      .postForm(Seq("grant_type" -> "client_credentials"))
      .auth(CLIENT_ID, SECRET)
      .asString
    if (response.isError) {
      throw new IllegalStateException(response.body)
    }
    val json: JValue = parse(response.body)
    val oauth: Option[JField] = json.findField(f => f.name.equals("access_token"))
    if (oauth.isEmpty) {
      throw new IllegalStateException("No access token present: " + response.body)
    }
    return oauth.get.value.values.toString
  }
}

object Region {
  val EU: String = "eu"
  val US: String = "us"
}
