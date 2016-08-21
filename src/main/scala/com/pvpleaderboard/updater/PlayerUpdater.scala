package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

import com.pvpleaderboard.updater.NonApiData.{ slugify, slugifyRealm }

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

/**
 * Updates the player.
 */
object PlayerUpdater {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val api: ApiHandler = new ApiHandler()
  private val db: DbHandler = new DbHandler()
  private implicit val formats = DefaultFormats

  private val BRACKETS: List[String] = List("2v2", "3v3", "rbg")

  private val maxPerBracket: Option[Int] = if (sys.env.isDefinedAt("MAX_PER_BRACKET")) {
    Option(sys.env("MAX_PER_BRACKET").toInt)
  } else {
    Option.empty
  }

  private lazy val classes: Map[Int, String] = getClasses()

  def update(): Unit = {
    logger.info("Updating player data")

    BRACKETS.foreach(importBracket)
  }

  private def importBracket(bracket: String): Unit = {
    logger.info("Importing {}", bracket)
    val response: Option[JValue] = api.get("leaderboard/" + bracket)
    if (response.isEmpty) {
      logger.warn("Skipping {} import", bracket)
      return
    }

    val leaderboard: List[LeaderboardEntry] = response.get.extract[Leaderboard].rows
    logger.debug("Found {} {} players", leaderboard.size, bracket)
    importPlayers(leaderboard.take(maxPerBracket.getOrElse(leaderboard.size)))
  }

  private def importPlayers(leaderboard: List[LeaderboardEntry]): Unit = {
    val path: String = "character/%s/%s?fields=talents,guild,achievements"
    val players: List[Player] = leaderboard.foldLeft(List[Player]()) { (list, entry) =>
      val response: Option[JValue] = api.get(String.format(path, entry.realmSlug, entry.name))
      if (response.isDefined) {
        val player: Player = response.get.extract[Player]
        list.:+(player)
      } else {
        list
      }
    }

    val columns: List[String] = List(
      "name",
      "class_id",
      "spec_id",
      "faction_id",
      "race_id",
      "realm_slug",
      "guild",
      "gender",
      "achievement_points",
      "honorable_kills",
      "thumbnail")
    val rows = players.foldLeft(List[List[Any]]()) { (l, p) =>
      val spec = getSpec(p)
      val realm = slugifyRealm(p.realm)
      val guild = if (p.guild.isDefined) Option(p.guild.get.name) else Option.empty

      l.:+(List(
        p.name,
        p.`class`,
        spec,
        p.faction,
        p.race,
        realm,
        guild,
        p.gender,
        p.achievementPoints,
        p.totalHonorableKills,
        p.thumbnail))
    }

    db.upsert("players", columns, rows, Option("players_name_realm_slug_key"))
    insertPlayersTalents(players)
  }

  private def getActiveTree(player: Player): Option[TalentTree] = {
    return player.talents.filter(_.selected.getOrElse(false)).headOption
  }

  private def insertPlayersTalents(players: List[Player]): Unit = {
    val columns: List[String] = List("name", "realm_slug", "spell_id")
    val rows = players.foldLeft(List[List[List[Any]]]()) { (l, p) =>
      val activeTree = getActiveTree(p)
      if (activeTree.isDefined) {
        val talents = activeTree.get.talents.filterNot(_ == null)
        l.:+(talents.map(t => List(p.name, slugifyRealm(p.realm), t.spell.id)))
      } else {
        l
      }
    }.flatten

    db.insertPlayersTalents(rows)
  }

  private def getSpec(player: Player): Option[Int] = {
    val activeTree = getActiveTree(player)

    return if (activeTree.isDefined && activeTree.get.spec.name.isDefined) {
      val specName: String = activeTree.get.spec.name.get
      val className: String = classes(player.`class`)
      return Option(NonApiData.specIds(className + specName))
    } else {
      Option.empty
    }
  }

  private def getClasses(): Map[Int, String] = {
    val response: Option[JValue] = api.get("data/character/classes")
    if (response.isEmpty) {
      throw new IllegalStateException("Unable to create class ID map")
    }

    return response.get.extract[Classes].classes
      .map(c => c.id -> slugify(c.name))
      .toMap
  }

}

case class Leaderboard(rows: List[LeaderboardEntry])
case class LeaderboardEntry(ranking: Int, rating: Int, name: String, realmSlug: String,
  specId: Int, seasonWins: Int, seasonLosses: Int)

case class Player(name: String, realm: String, `class`: Int, race: Int, gender: Int,
  achievementPoints: Int, thumbnail: String, faction: Int, totalHonorableKills: Int,
  guild: Option[Guild], achievements: CompletedAchievements, talents: List[TalentTree])
case class Guild(name: String)
case class CompletedAchievements(achievementsCompleted: List[Int],
  achievementsCompletedTimestamp: List[Long])
case class TalentTree(selected: Option[Boolean], talents: List[Talent], spec: TalentSpec)
