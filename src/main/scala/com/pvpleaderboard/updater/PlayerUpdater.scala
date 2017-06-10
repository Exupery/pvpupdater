package com.pvpleaderboard.updater

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Try

import org.slf4j.{ Logger, LoggerFactory }

import com.pvpleaderboard.updater.NonApiData.slugify

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

  private val DEFAULT_NUM_THREADS: Int = 5;
  private val numThreads: Int = Try(sys.env("NUM_THREADS").toInt).getOrElse(DEFAULT_NUM_THREADS)

  private lazy val classes: Map[Int, String] = getClasses()

  def update(): Unit = {
    logger.info("Updating player data")

    apis.foreach { api =>
      BRACKETS.foreach(b => importBracket(b, api))
    }
    db.setUpdateTime()
  }

  private def importBracket(bracket: String, api: ApiHandler): Unit = {
    logger.info("Importing {} {}", api.region.toUpperCase(), bracket: Any)
    val response: Option[JValue] = api.get("leaderboard/" + bracket)
    if (response.isEmpty) {
      logger.warn("Skipping {} import", bracket)
      return
    }

    val fullLeaderboard: List[LeaderboardEntry] = response.get.extract[Leaderboard].rows
    logger.debug("Found {} {} players", fullLeaderboard.size, bracket)
    val leaderboard: List[LeaderboardEntry] = if (maxPerBracket.isDefined) {
      fullLeaderboard.take(maxPerBracket.get)
    } else {
      fullLeaderboard
    }

    if (leaderboard.isEmpty) {
      logger.warn("Empty {} leaderboard", bracket)
      return
    }

    importPlayers(leaderboard, api)
    updateLeaderboard(bracket, api.region, leaderboard)
  }

  private def importPlayers(leaderboard: List[LeaderboardEntry], api: ApiHandler): Unit = {
    logger.debug("Importing {} players", leaderboard.size)
    val players: List[Player] = getPlayers(leaderboard, api)

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
      val guild = if (p.guild.isDefined) Option(p.guild.get.name) else Option.empty

      l.:+(List(
        p.name,
        p.`class`,
        spec,
        p.faction,
        p.race,
        p.realmId,
        guild,
        p.gender,
        p.achievementPoints,
        p.totalHonorableKills,
        p.thumbnail))
    }

    db.upsert("players", columns, rows, Option("players_name_realm_id_key"))
    val playerIds: Map[String, Int] = db.getPlayerIds(players.map(p => (p.name, p.realmId)))

    val noId: Int = -1
    logger.debug("Mapping {} player IDs", playerIds.size)
    players.foreach(p => p.playerId = playerIds.getOrElse(p.name + p.realmId, noId))
    logger.debug("Mapped {} player IDs", players.filter(_.playerId > noId).size)

    insertPlayersTalents(players)
    players.grouped(1000).foreach(insertPlayersAchievements)
  }

  private def getPlayers(leaderboard: List[LeaderboardEntry], api: ApiHandler): List[Player] = {
    val groupSize: Int = leaderboard.size / numThreads
    val path: String = "character/%s/%s"
    val field: String = "fields=talents,guild,achievements"
    val futures = leaderboard.grouped(groupSize).map(group => {
      Future[List[Player]] {
        group.foldLeft(List[Player]()) { (list, entry) =>
          val response: Option[JValue] =
            api.get(String.format(path, entry.realmSlug, entry.name), field)
          Try(list.:+(response.get.extract[Player])).getOrElse(list)
        }
      }
    }).toList
    logger.debug("Waiting on {} futures", futures.size)
    val players = futures.map(Await.result[List[Player]](_, 12 hours)).flatten
    logger.debug("Found {} players", players.size)

    val realmIds: Map[String, Int] = db.getRealmIds(api.region, false)
    players.foreach(p => p.realmId = realmIds(p.realm))

    return players
  }

  private def getActiveTree(player: Player): Option[TalentTree] = {
    return player.talents.filter(_.selected.getOrElse(false)).headOption
  }

  private def insertPlayersTalents(players: List[Player]): Unit = {
    val rows = players.foldLeft(List[List[(Int, Int)]]()) { (l, p) =>
      val activeTree = getActiveTree(p)
      val id = p.playerId
      if (activeTree.isDefined && id > -1) {
        val talents = activeTree.get.talents.filterNot(_ == null)
        l.:+(talents.map(t => (id, t.spell.id)))
      } else {
        l
      }
    }.flatten

    db.insertPlayersTalents(rows)
  }

  private def insertPlayersAchievements(players: List[Player]): Unit = {
    val pvpIds: Set[Int] = db.getAchievementsIds()
    val rows = players.foldLeft(List[List[List[Any]]]()) { (l, p) =>
      val id = p.playerId
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
    val realmIds: Map[String, Int] = db.getRealmIds(region, true)
    val rows = leaderboard.foldLeft(List[List[Any]]()) { (l, entry) =>
      l.:+(List(
        entry.ranking,
        entry.rating,
        entry.seasonWins,
        entry.seasonLosses,
        entry.name,
        realmIds(entry.realmSlug)))
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
    guild: Option[Guild], achievements: CompletedAchievements, talents: List[TalentTree]) {
  var realmId: Int = -1
  var playerId: Int = -1
}
case class Guild(name: String)
case class CompletedAchievements(achievementsCompleted: List[Int],
  achievementsCompletedTimestamp: List[Long])
case class TalentTree(selected: Option[Boolean], talents: List[Talent], spec: TalentSpec)
