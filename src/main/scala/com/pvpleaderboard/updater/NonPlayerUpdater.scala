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

  private val db: DbHandler = new DbHandler()
  private implicit val formats = DefaultFormats

  def update(apis: List[ApiHandler]): Unit = {
    val sharedApi: ApiHandler = apis.head // Only realms differ per handler (i.e. region)
    logger.info("Updating non-player data")
    importFactions()
    apis.foreach(importRealms)
    importRaces(sharedApi)
    importAchievements(sharedApi)
    importClasses(sharedApi)
    importTalentsAndSpecs(sharedApi)
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
    val response: Option[JValue] = api.getDynamic("realm/index")
    if (response.isEmpty) {
      logger.warn("Skipping realms import")
      return
    }

    val realms: List[Realm] = response.get.extract[Realms].realms
    val region: String = api.region.toUpperCase()
    logger.debug("Found {} {} realms", realms.size, region)
    val columns: List[String] = List("slug", "name", "region")
    val rows = realms.foldLeft(List[List[Any]]()) { (l, r) =>
      l.:+(List(r.slug, r.name.get, region))
    }
    db.upsert("realms", columns, rows, Option("realms_slug_region_key"))
  }

  private def importRaces(api: ApiHandler): Unit = {
    val response: Option[JValue] = api.getStatic("playable-race/index")
    if (response.isEmpty) {
      logger.warn("Skipping races import")
      return
    }

    val races: List[Race] = response.get.extract[Races].races
    logger.debug("Found {} races", races.size)
    val columns: List[String] = List("id", "name", "side")
    val rows = races.foldLeft(List[List[Any]]()) { (l, r) =>
      val res: JValue = api.getStatic("playable-race/" + r.id).get
      val side = (res \ "faction" \ "name").extract[String]
      l.:+(List(r.id, r.name, side.toLowerCase))
    }
    db.upsert("races", columns, rows)
  }

  private def importAchievements(api: ApiHandler): Unit = {
    val achievementIds: Set[Int] = NonApiData.getAchievementsIds()

    val achievements: List[Achievement] = achievementIds
      .map(id => api.getStatic("achievement/" + id).get.extract[Achievement])
      .toList
    logger.debug("Found {} of {} achievements", achievements.size, achievementIds.size)
    val columns: List[String] = List("id", "name", "description", "points")
    val rows = achievements.foldLeft(List[List[Any]]()) { (l, a) =>
      l.:+(List(a.id, a.name, a.description, a.points))
    }
    db.upsert("achievements", columns, rows)
  }

  private def importClasses(api: ApiHandler): Map[String, KeyedValue] = {
    val response: Option[JValue] = api.getStatic("playable-class/index")
    if (response.isEmpty) {
      logger.warn("Skipping classes import")
      return Map.empty
    }

    val classes: List[KeyedValue] = response.get.extract[Classes].classes
    logger.debug("Found {} classes", classes.size)
    val columns: List[String] = List("id", "name")
    val rows = classes.foldLeft(List[List[Any]]()) { (l, c) =>
      l.:+(List(c.id, c.name))
    }
    db.upsert("classes", columns, rows)
    return classes.map(c => slugify(c.name) -> c).toMap
  }

  private def importTalentsAndSpecs(api: ApiHandler): Unit = {
    val response: Option[JValue] = api.getStatic("playable-specialization/index")
    if (response.isEmpty) {
      logger.warn("Skipping talent and spec import")
      return
    }

    val specializations: List[Specialization] = response.get.extract[Specializations].character_specializations
      .map(_.id)
      .map(id => api.getStatic("playable-specialization/" + id).get.extract[Specialization])
      .toList

    insertSpecs(api, specializations)
    insertTalents(api, specializations)
  }

  private def insertSpecs(api: ApiHandler, specializations: List[Specialization]): Unit = {
    val columns: List[String] = List(
      "id",
      "class_id",
      "name",
      "role",
      "description",
      "icon")
    val rows = specializations.foldLeft(List[List[Any]]()) { (list, spec) =>
      val iconHref = api.getStatic("media/playable-specialization/" + spec.id).get.extract[Media]
        .assets
        .find(_.key.equals("icon"))
        .get
        .value
      val start = iconHref.lastIndexOf("/") + 1
      val end = iconHref.lastIndexOf(".")
      val icon = iconHref.substring(start, end)
      list.:+(List(
        spec.id,
        spec.playable_class.id,
        spec.name,
        spec.role("type"),
        spec.gender_description("male"),
        icon
      ))
    }
    logger.debug("Found {} specs", rows.size)
    db.upsert("specs", columns, rows)
  }

  private def insertTalents(api: ApiHandler, specializations: List[Specialization]): Unit = {
    val columns: List[String] = List(
      "id",
      "spell_id",
      "class_id",
      "spec_id",
      "name",
      "description",
      "icon",
      "tier",
      "col")
    val rows = specializations.foldLeft(List[List[List[Any]]]()) { (l, spec) =>
      val classId: Int = spec.playable_class.id
      val specId: Int = spec.id
      val talents: List[TalentListing] = spec.talent_tiers.map(_.talents).flatten

      l.:+(talents.foldLeft(List[List[Any]]()) { (list, talentListing) =>
        val talentId: Int = talentListing.talent.id
        val talent: Talent = api.getStatic("talent/" + talentId).get.extract[Talent]
        list.:+(List(
          talentId,
          talent.spell.id,
          classId,
          specId,
          talentListing.talent.name,
          talentListing.spell_tooltip.description,
          getSpellIcon(api, talent.spell.id),
          talent.tier_index,
          talent.column_index))
      })
    }.flatten
    logger.debug("Found {} talents", rows.size)
    db.insertTalents(rows)
  }

  private def getSpellIcon(api: ApiHandler, spellId: Int): String = {
    val media: Media = api.getMedia("spell/" + spellId).get.extract[Media]
    val icon: Asset = media.assets.filter(_.key.equals("icon")).head
    val start: Int = icon.value.lastIndexOf("/") + 1
    val end: Int = icon.value.lastIndexOf(".")
    return icon.value.substring(start, end)
  }

  private def importPvPTalents(): Unit = {
    // TODO PVP TALENTS NOT YET AVAILABLE VIA BATTLE.NET API
  }

}

case class KeyedValue(key: Key, name: String, id: Int)
case class Key(href: String)
case class Media(assets: List[Asset])
case class Asset(key: String, value: String)

case class Faction(id: Int, name: String)

case class Realms(realms: List[Realm])
case class Realm(slug: String, name: Option[String])

case class Races(races: List[Race])
case class Race(id: Int, name: String)

case class AchievementCategories(categories: List[KeyedValue])
case class AchievementKeys(name: String, id: Int, achievements: List[KeyedValue])
case class Achievement(id: Int, name: String, description: String, points: Int)

case class Classes(classes: List[KeyedValue])
case class Specializations(character_specializations: List[KeyedValue])
case class Specialization(id: Int, playable_class: KeyedValue, name: String, gender_description: Map[String, String],
  role: Map[String, String], talent_tiers: List[TalentTier])

case class TalentTier(level: Int, talents: List[TalentListing])
case class TalentListing(talent: KeyedValue, spell_tooltip: SpellTooltip)
case class SpellTooltip(spell: Option[KeyedValue], description: String, cast_time: String)
case class Talent(id: Int, tier_index: Int, column_index: Int, description: String, spell: KeyedValue, playable_class: KeyedValue)
case class Spell(id: Int, media: SpellMedia)
case class SpellMedia(id: Int, key: Key)
