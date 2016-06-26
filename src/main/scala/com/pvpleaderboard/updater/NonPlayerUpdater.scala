package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }
import play.api.libs.json.JsValue

/**
 * Updates the non-player data, e.g. achievements, classes, factions,
 * races, realms, specializations, and talents.
 */
object NonPlayerUpdater {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val api: ApiHandler = new ApiHandler()

  def update(): Unit = {
    logger.info("Updating non-player data")
    importRealms()
  }

  private def importRealms(): Unit = {
    val response: Option[JsValue] = api.get("realm/status")
    if (response.isEmpty) {
      logger.warn("Skipping realms import")
      return ;
    }

    println(response.get) // TODO DELME
  }

}
