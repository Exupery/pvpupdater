package com.pvpleaderboard.updater

import scala.util.Try

import org.slf4j.{ Logger, LoggerFactory }

import com.pvpleaderboard.updater.NonApiData.{ slugify, slugifyRealm }

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

/**
 * Updates the player.
 */
object PlayerUpdater {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val apis: List[ApiHandler] = List(new ApiHandler(Region.US), new ApiHandler(Region.EU))
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

    apis.foreach { api =>
      BRACKETS.foreach(b => importBracket(b, api))
    }
    db.setUpdateTime()
  }

  private def importBracket(bracket: String, api: ApiHandler): Unit = {
    logger.info("Importing {}", bracket)
    val response: Option[JValue] = api.get("leaderboard/" + bracket)
    if (response.isEmpty) {
      logger.warn("Skipping {} import", bracket)
      return
    }

    val fullLeaderboard: List[LeaderboardEntry] = response.get.extract[Leaderboard].rows
    logger.debug("Found {} {} players", fullLeaderboard.size, bracket)
    val leaderboard: List[LeaderboardEntry] =
      fullLeaderboard.take(maxPerBracket.getOrElse(fullLeaderboard.size))

    importPlayers(leaderboard, api)
    updateLeaderboard(bracket, api.region, leaderboard)
  }

  private def importPlayers(leaderboard: List[LeaderboardEntry], api: ApiHandler): Unit = {
    val path: String = "character/%s/%s"
    val players: List[Player] = leaderboard.foldLeft(List[Player]()) { (list, entry) =>
      val response: Option[JValue] =
        api.get(String.format(path, entry.realmSlug, entry.name), "fields=talents,guild,achievements")
      Try(list.:+(response.get.extract[Player])).getOrElse(list)
    }
    val realmIds: Map[String, Int] = db.getRealmIds(api.region)

    val columns: List[String] = List(
      "name",
      "class_id",
      "spec_id",
      "faction_id",
      "race_id",
      "realm_id",
      "guild",
      "gender",
      "achievement_points",
      "honorable_kills",
      "thumbnail")
    val rows = players.foldLeft(List[List[Any]]()) { (l, p) =>
      val spec = getSpec(p)
      val realm: Int = realmIds(slugifyRealm(p.realm))
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

    db.upsert("players", columns, rows, Option("players_name_realm_id_key"))
    val playerIds: Map[String, Int] =
      db.getPlayerIds(players.map(p => (p.name, realmIds(slugifyRealm(p.realm)))))
    insertPlayersTalents(players, playerIds)
    players.grouped(1000).foreach(insertPlayersAchievements(_, playerIds))
  }

  private def getActiveTree(player: Player): Option[TalentTree] = {
    return player.talents.filter(_.selected.getOrElse(false)).headOption
  }

  private def insertPlayersTalents(players: List[Player], playerIds: Map[String, Int]): Unit = {
    val rows = players.foldLeft(List[List[(Int, Int)]]()) { (l, p) =>
      val activeTree = getActiveTree(p)
      val id = playerIds.getOrElse(p.name + p.realm, -1)
      if (activeTree.isDefined && id > -1) {
        val talents = activeTree.get.talents.filterNot(_ == null)
        l.:+(talents.map(t => (id, t.spell.id)))
      } else {
        l
      }
    }.flatten

    db.insertPlayersTalents(rows)
  }

  private def insertPlayersAchievements(players: List[Player],
    playerIds: Map[String, Int]): Unit = {
    val pvpIds: Set[Int] = db.getAchievementsIds()
    val rows = players.foldLeft(List[List[List[Any]]]()) { (l, p) =>
      val id = playerIds.getOrElse(p.name + p.realm, -1)
      if (id > -1) {
        val timestamps: List[Long] = p.achievements.achievementsCompletedTimestamp
        var idx: Int = -1
        l.:+(p.achievements.achievementsCompleted.map { achievementId =>
          idx += 1
          if (pvpIds.contains(achievementId)) {
            List(id, achievementId, timestamps(idx) / 1000)
          } else {
            List.empty
          }
        })
      } else {
        l
      }
    }.flatten.filter(!_.isEmpty)
    db.insertPlayersAchievements(rows)
  }

  private def updateLeaderboard(bracket: String, region: String,
    leaderboard: List[LeaderboardEntry]): Unit = {
    val rows = leaderboard.foldLeft(List[List[Any]]()) { (l, entry) =>
      l.:+(List(
        entry.ranking,
        entry.rating,
        entry.seasonWins,
        entry.seasonLosses,
        entry.name,
        entry.realmSlug))
    }

    db.updateBracket(bracket, region, rows)
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
    val response: Option[JValue] = apis.head.get("data/character/classes")
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
