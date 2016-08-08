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
  private val db: DbHandler = new DbHandler()
  private implicit val formats = DefaultFormats

  def update(): Unit = {
    logger.info("Updating non-player data")
    importFactions()
    importRealms()
    importRaces()
    importAchievements()
    //    val classes: Map[String, PlayerClass] = importClasses()
    //    importTalentsAndSpecs(classes)
    importPvPTalents()
  }

  private def importFactions(): Unit = {
    val factions: List[Faction] = NonApiData.factions
    val rows = factions.foldLeft(List[List[Any]]()) { (l, f) =>
      l.:+(List(f.id, f.name))
    }
    db.insertDoNothing("factions", List("id", "name"), rows)
  }

  private def importRealms(): Unit = {
    val response: Option[JValue] = api.get("realm/status")
    if (response.isEmpty) {
      logger.warn("Skipping realms import")
      return
    }

    val realms: List[Realm] = response.get.extract[Realms].realms
    val columns: List[String] = List("slug", "name", "battlegroup", "timezone", "type")
    val rows = realms.foldLeft(List[List[Any]]()) { (l, r) =>
      l.:+(List(r.slug, r.name, r.battlegroup, r.timezone, r.`type`))
    }
    db.insertDoNothing("realms", columns, rows)
  }

  private def importRaces(): Unit = {
    val response: Option[JValue] = api.get("data/character/races")
    if (response.isEmpty) {
      logger.warn("Skipping races import")
      return
    }

    val races: List[Race] = response.get.extract[Races].races
    val columns: List[String] = List("id", "name", "side")
    val rows = races.foldLeft(List[List[Any]]()) { (l, r) =>
      l.:+(List(r.id, r.name, r.side))
    }
    db.insertDoNothing("races", columns, rows)
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
      .map(_.categories)
      .flatten
      .filter(c => {
        c.name.equalsIgnoreCase("Rated Battleground") || c.name.equalsIgnoreCase("Arena")
      })
      .map(_.achievements)
      .flatten
    val columns: List[String] = List("id", "name", "description", "icon", "points")
    val rows = achievements.foldLeft(List[List[Any]]()) { (l, a) =>
      l.:+(List(a.id, a.title, a.description, a.icon, a.points))
    }
    db.insertDoNothing("achievements", columns, rows)
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
    if (classes.size != talentsAndSpecs.size) {
      logger.error("Found {} classes, expected {} for talent and spec import, skipping",
        classes.size, talentsAndSpecs.size)
      return
    }
    talentsAndSpecs.foreach { x => println(x.`class`) }
    talentsAndSpecs.foreach { x =>
      println(x.`class`)
      println(x.talents.flatten.flatten.size)
    }
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
case class AchievementGroup(name: String, categories: List[AchievementCategory])
case class AchievementCategory(name: String, achievements: List[Achievement])
case class Achievement(id: Int, title: String, description: String, icon: String, points: Int)

case class Classes(classes: List[PlayerClass])
case class PlayerClass(id: Int, name: String)

case class TalentsAndSpecs(talents: List[List[List[Talent]]], `class`: String, specs: List[Spec])
case class Talent(tier: Int, column: Int, spell: TalentSpell, spec: TalentSpec)
case class TalentSpell(id: Int, name: String, description: String, icon: String)
case class TalentSpec(name: Option[String])
case class Spec(name: String, role: String, description: String, backgroundImage: String, icon: String)
