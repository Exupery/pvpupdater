package com.pvpleaderboard.updater

import java.io.IOException

import scala.io.Source
import scala.util.Try

import org.slf4j.{ Logger, LoggerFactory }

import net.liftweb.json.{ JValue, parse }
import net.liftweb.json.JsonParser.ParseException

/**
 * Send requests and receive responses to/from the Blizzard API
 */
class ApiHandler {
  private val DEFAULT_URI: String = "https://us.api.battle.net/wow/"
  private val BASE_URI: String = Try(sys.env("BATTLE_NET_API_URI")).getOrElse(DEFAULT_URI)
  private val API_KEY: String = sys.env("BATTLE_NET_API_KEY")

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def get(path: String, params: String = ""): Option[JValue] = {
    val requiredParams: String = String.format("?locale=en_US&apikey=%s", API_KEY)
    val allParams: String = if (params.isEmpty()) {
      requiredParams
    } else {
      String.format("%s&%s", requiredParams, params)
    }

    val url: String = BASE_URI + path + allParams
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
          if (c == 3) {
            logger.error("GET failed: {}", io.toString())
          }
        }
      }
    }

    return Option.empty
  }
}
