package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

import com.pvpleaderboard.updater.NonApiData.slugify

import net.liftweb.json.{ DefaultFormats, JValue }

/**
 * Updates the non-player data, e.g. achievements, classes, factions,
 * races, realms, specializations, and talents.
 */
object NonPlayerUpdater {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val apis: List[ApiHandler] = List(new ApiHandler(Region.US), new ApiHandler(Region.EU))
  private val db: DbHandler = new DbHandler()
  private implicit val formats = DefaultFormats

  def update(): Unit = {
    logger.info("Updating non-player data")
    importFactions()
    apis.foreach(importRealms)
    importRaces()
    importAchievements()
    val classes: Map[String, PlayerClass] = importClasses()
    importTalentsAndSpecs(classes)
    importPvPTalents()
  }

  private def importFactions(): Unit = {
    val factions: List[Faction] = NonApiData.factions
    logger.debug("Found {} factions", factions.size)
    val rows = factions.foldLeft(List[List[Any]]()) { (l, f) =>
      l.:+(List(f.id, f.name))
    }
    db.upsert("factions", List("id", "name"), rows)
  }

  private def importRealms(api: ApiHandler): Unit = {
    val response: Option[JValue] = api.get("realm/status")
    if (response.isEmpty) {
      logger.warn("Skipping realms import")
      return
    }

    val realms: List[Realm] = response.get.extract[Realms].realms
    val region: String = api.region.toUpperCase()
    logger.debug("Found {} {} realms", realms.size, region)
    val columns: List[String] = List("slug", "name", "region", "battlegroup", "timezone", "type")
    val rows = realms.foldLeft(List[List[Any]]()) { (l, r) =>
      l.:+(List(r.slug, r.name, region, r.battlegroup, r.timezone, r.`type`))
    }
    db.upsert("realms", columns, rows, Option("realms_slug_region_key"))
  }

  private def importRaces(): Unit = {
    val response: Option[JValue] = apis.head.get("data/character/races")
    if (response.isEmpty) {
      logger.warn("Skipping races import")
      return
    }

    val races: List[Race] = response.get.extract[Races].races
    logger.debug("Found {} races", races.size)
    val columns: List[String] = List("id", "name", "side")
    val rows = races.foldLeft(List[List[Any]]()) { (l, r) =>
      l.:+(List(r.id, r.name, r.side))
    }
    db.upsert("races", columns, rows)
  }

  private def importAchievements(): Unit = {
    val response: Option[JValue] = apis.head.get("data/character/achievements")
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
    logger.debug("Found {} achievements", achievements.size)
    val columns: List[String] = List("id", "name", "description", "icon", "points")
    val rows = achievements.foldLeft(List[List[Any]]()) { (l, a) =>
      l.:+(List(a.id, a.title, a.description, a.icon, a.points))
    }
    db.upsert("achievements", columns, rows)
  }

  private def importClasses(): Map[String, PlayerClass] = {
    val response: Option[JValue] = apis.head.get("data/character/classes")
    if (response.isEmpty) {
      logger.warn("Skipping classes import")
      return Map.empty
    }

    val classes: List[PlayerClass] = response.get.extract[Classes].classes
    logger.debug("Found {} classes", classes.size)
    val columns: List[String] = List("id", "name")
    val rows = classes.foldLeft(List[List[Any]]()) { (l, c) =>
      l.:+(List(c.id, c.name))
    }
    db.upsert("classes", columns, rows)
    return classes.map(c => slugify(c.name) -> c).toMap
  }

  private def importTalentsAndSpecs(classes: Map[String, PlayerClass]): Unit = {
    val response: Option[JValue] = apis.head.get("data/talents")
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

    insertSpecs(talentsAndSpecs.map(tas => tas.`class` -> tas.specs).toMap, classes)
    insertTalents(talentsAndSpecs, classes)
  }

  private def insertSpecs(specs: Map[String, List[Spec]],
    classes: Map[String, PlayerClass]): Unit = {

    val columns: List[String] = List(
      "id",
      "class_id",
      "name",
      "role",
      "description",
      "background_image",
      "icon")
    val rows = specs.foldLeft(List[List[List[Any]]]()) { (l, s) =>
      val className: String = s._1
      val classId: Int = classes(className).id
      l.:+(s._2.foldLeft(List[List[Any]]()) { (list, spec) =>
        list.:+(List(
          NonApiData.specIds(className + spec.name),
          classId,
          spec.name,
          spec.role,
          spec.description,
          spec.backgroundImage,
          spec.icon))
      })
    }.flatten
    logger.debug("Found {} specs", rows.size)
    db.upsert("specs", columns, rows)
  }

  private def insertTalents(talentsAndSpecs: List[TalentsAndSpecs],
    classes: Map[String, PlayerClass]): Unit = {

    val columns: List[String] = List(
      "spell_id",
      "class_id",
      "spec_id",
      "name",
      "description",
      "icon",
      "tier",
      "col")
    val rows = talentsAndSpecs.foldLeft(List[List[List[Any]]]()) { (l, t) =>
      val className: String = t.`class`
      val classId: Int = classes(className).id
      val talents: List[Talent] = t.talents.flatten.flatten

      l.:+(talents.foldLeft(List[List[Any]]()) { (list, talent) =>
        val specId: Option[Int] = if (talent.spec.name.isDefined) {
          Option(NonApiData.specIds(className + talent.spec.name.get))
        } else {
          Option.empty
        }
        list.:+(List(
          talent.spell.id,
          classId,
          specId.getOrElse(0),
          talent.spell.name,
          talent.spell.description,
          talent.spell.icon,
          talent.tier,
          talent.column))
      })
    }.flatten
    logger.debug("Found {} talents", rows.size)
    db.insertTalents(rows)
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
