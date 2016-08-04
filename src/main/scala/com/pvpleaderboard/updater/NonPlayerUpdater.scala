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
    importRealms()
    importRaces()
    importFactions()
    importAchievements()
    val classes: Map[String, PlayerClass] = importClasses()
    importTalentsAndSpecs(classes)
    importPvPTalents()
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

  private def importAchievements(): Unit = {
    val response: Option[JValue] = api.get("data/character/achievements")
    if (response.isEmpty) {
      logger.warn("Skipping achievements import")
      return
    }

    val achievementGroups: List[AchievementGroup] = response.get.extract[Achievements].achievements
    val achievements: List[Achievement] = achievementGroups
      .filter(_.name.contains("Player vs. Player"))
      .map(_.achievements)
      .flatten
    println(achievementGroups.size) // TODO DELME
    println(achievementGroups.map(_.name)) // TODO DELME
    println(achievements.size) // TODO DELME
  }

  private def slugify(str: String): String = {
    return str.toLowerCase().replaceAll(" ", "-")
  }

  private def importClasses(): Map[String, PlayerClass] = {
    val response: Option[JValue] = api.get("data/character/classes")
    if (response.isEmpty) {
      logger.warn("Skipping classes import")
      return Map.empty
    }

    val classes: List[PlayerClass] = response.get.extract[Classes].classes
    println(classes) // TODO DELME
    println(classes.size) // TODO DELME
    return classes.map(c => slugify(c.name) -> c).toMap
  }

  private def importTalentsAndSpecs(classes: Map[String, PlayerClass]): Unit = {
    val response: Option[JValue] = api.get("data/talents")
    if (response.isEmpty) {
      logger.warn("Skipping talent and spec import")
      return
    }

    val talentsAndSpecs: List[TalentsAndSpecs] =
      response.get.children.map(_.extract[TalentsAndSpecs])
    println(talentsAndSpecs.size) // TODO DELME
    if (classes.size != talentsAndSpecs.size) {
      logger.error("Found {} classes, expected {} for talent and spec import, skipping",
        classes.size, talentsAndSpecs.size)
      return
    }
    talentsAndSpecs.foreach { x => println(x.`class`) }
    val talents: List[Talent] = talentsAndSpecs.head.talents.flatten.flatten
    println(talents.size) // TODO DELME
    val specs: List[Spec] = talentsAndSpecs.head.specs
    println(specs.size) // TODO DELME
  }

  private def importPvPTalents(): Unit = {
    // TODO PVP TALENTS NOT YET AVAILABLE VIA BATTLE.NET API
  }

}

case class Faction(id: Int, name: String)

case class Realms(realms: List[Realm])
case class Realm(slug: String, name: String, battlegroup: String, timezone: String, `type`: String)

case class Races(races: List[Race])
case class Race(id: Int, name: String, side: String)

case class Achievements(achievements: List[AchievementGroup])
case class AchievementGroup(name: String, achievements: List[Achievement])
case class Achievement(id: Int, title: String, description: String, icon: String, points: Int)

case class Classes(classes: List[PlayerClass])
case class PlayerClass(id: Int, name: String)

case class TalentsAndSpecs(talents: List[List[List[Talent]]], `class`: String, specs: List[Spec])
case class Talent(tier: Int, column: Int, spell: TalentSpell)
case class TalentSpell(id: Int, name: String, description: String, icon: String)
case class Spec(name: String, role: String, description: String, backgroundImage: String, icon: String)
