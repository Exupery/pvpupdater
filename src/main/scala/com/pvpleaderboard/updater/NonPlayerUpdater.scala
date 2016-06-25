package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

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
    val response: Option[String] = api.get("realm/status")
    println(response.get) // TODO DELME
  }

}
