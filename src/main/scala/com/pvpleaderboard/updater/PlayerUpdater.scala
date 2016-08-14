package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

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
    println(leaderboard.size) // TODO DELME
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

    println(players.size) // TODO DELME
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
case class TalentTree(selected: Option[Boolean], talents: List[Talent])
