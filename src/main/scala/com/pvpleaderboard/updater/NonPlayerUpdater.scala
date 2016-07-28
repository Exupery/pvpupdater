package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

import net.liftweb.json.{ DefaultFormats, JValue }

/**
 * Updates the non-player data, e.g. achievements, classes, factions,
 * races, realms, specializations, and talents.
 */
object NonPlayerUpdater {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val api: ApiHandler = new ApiHandler()
  private implicit val formats = DefaultFormats

  def update(): Unit = {
    logger.info("Updating non-player data")
    //    importRealms()
    //    importRaces()
    importFactions()
  }

  private def importRealms(): Unit = {
    val response: Option[JValue] = api.get("realm/status")
    if (response.isEmpty) {
      logger.warn("Skipping realms import")
      return
    }

    val realms: List[Realm] = response.get.extract[Realms].realms
    println(realms) // TODO DELME
    println(realms.size) // TODO DELME
  }

  private def importRaces(): Unit = {
    val response: Option[JValue] = api.get("data/character/races")
    if (response.isEmpty) {
      logger.warn("Skipping races import")
      return
    }

    val races: List[Race] = response.get.extract[Races].races
    println(races) // TODO DELME
    println(races.size) // TODO DELME
  }

  private def importFactions(): Unit = {
    /* Faction data not available via API */
    val factions: List[Faction] = List(Faction(0, "Alliance"), Faction(1, "Horde"))
    println(factions) // TODO DELME
    println(factions.size) // TODO DELME
  }

}

case class Faction(id: Int, name: String)

case class Realms(realms: List[Realm])
case class Realm(slug: String, name: String, battlegroup: String, timezone: String, `type`: String)

case class Races(races: List[Race])
case class Race(id: Int, name: String, side: String)
