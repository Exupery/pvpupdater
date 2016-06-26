package com.pvpleaderboard.updater

import java.io.IOException

import scala.io.Source
import scala.util.Try

import org.slf4j.{ Logger, LoggerFactory }

import play.api.libs.json.{ JsValue, Json }

/**
 * Send requests and receive responses to/from the Blizzard API
 */
class ApiHandler {
  private val DEFAULT_URI: String = "https://us.api.battle.net/wow/"
  private val BASE_URI: String = Try(sys.env("BATTLE_NET_API_URI")).getOrElse(DEFAULT_URI)
  private val API_KEY: String = sys.env("BATTLE_NET_API_KEY")

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def get(path: String, params: String = ""): Option[JsValue] = {
    val requiredParams: String = String.format("?locale=en_US&apikey=%s", API_KEY)
    val allParams: String = if (params.isEmpty()) {
      requiredParams
    } else {
      String.format("%s&%s", requiredParams, params)
    }

    try {
      val response: String = Source.fromURL(BASE_URI + path + allParams).mkString

      return Option(Json.parse(response))
    } catch {
      case io: IOException => logger.error("GET failed: " + io.toString())
    }

    return Option.empty
  }
}
