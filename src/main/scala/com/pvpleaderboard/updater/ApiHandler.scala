package com.pvpleaderboard.updater

/**
 * Send requests and receive responses to/from the Blizzard API
 */
class ApiHandler {
  private val BASE_URI: String = "https://us.api.battle.net/wow/"
  private val API_KEY: String = sys.env("BATTLE_NET_API_KEY")

  def get(path: String, params: String = ""): Option[String] = {
    val requiredParams: String = String.format("?locale=en_US&apikey=%s", API_KEY)
    val allParams: String = if (params.isEmpty()) {
      requiredParams
    } else {
      String.format("%s&%s", requiredParams, params)
    }
    val uri: String = BASE_URI + path + allParams
    println(uri) // TODO DELME
    return Option("TODO")
  }
}
