package com.pvpleaderboard.updater

import scala.collection.mutable.HashSet
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.math.max
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

  private val DEFAULT_NUM_THREADS: Int = 5
  private val numThreads: Int = Try(sys.env("NUM_THREADS").toInt).getOrElse(DEFAULT_NUM_THREADS)

  private val DEFAULT_ACHIEV_SIZE: Int = 500
  private val achievGroupSize: Int = Try(sys.env("ACHIEV_SIZE").toInt).getOrElse(DEFAULT_ACHIEV_SIZE)

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

    val leaderboard: Array[LeaderboardEntry] =
      response.get.extract[Leaderboard].rows.take(maxPerBracket.getOrElse(Int.MaxValue)).toArray

    if (leaderboard.isEmpty) {
      logger.warn("Empty {} leaderboard", bracket)
      return
    }

    importPlayers(leaderboard, api)
    updateLeaderboard(bracket, api.region, leaderboard)
  }

  private def importPlayers(leaderboard: Array[LeaderboardEntry], api: ApiHandler): Unit = {
    logger.debug("Importing {} players", leaderboard.size)
    val players: Array[Player] = getPlayers(leaderboard, api)

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
    insertPlayersStats(players)
    insertPlayersItems(players)
    insertItems(players)
    players.grouped(achievGroupSize).foreach(insertPlayersAchievements)
  }

  private def getPlayers(leaderboard: Array[LeaderboardEntry], api: ApiHandler): Array[Player] = {
    val groupSize: Int = max(leaderboard.size / numThreads, numThreads)
    val path: String = "character/%s/%s"
    val field: String = "fields=talents,guild,achievements,stats,items"
    val futures = leaderboard.grouped(groupSize).map(group => {
      Future[Array[Player]] {
        group.foldLeft(Array[Player]()) { (array, entry) =>
          val response: Option[JValue] =
            api.get(String.format(path, entry.realmSlug, entry.name), field)
          Try(array.:+(response.get.extract[Player])).getOrElse(array)
        }
      }
    }).toList
    logger.debug("Waiting on {} futures", futures.size)
    val players = futures.map(Await.result[Array[Player]](_, 12 hours)).flatten.toArray
    logger.debug("Found {} players", players.size)

    val realmIds: Map[String, Int] = db.getRealmIds(api.region, false)
    players.foreach(p => p.realmId = realmIds(p.realm))

    return players
  }

  private def getActiveTree(player: Player): Option[TalentTree] = {
    return player.talents.filter(_.selected.getOrElse(false)).headOption
  }

  private def insertPlayersTalents(players: Array[Player]): Unit = {
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

  private def insertPlayersStats(players: Array[Player]): Unit = {
    val columns: List[String] = List("player_id", "strength", "agility", "intellect", "stamina",
      "critical_strike", "haste", "mastery", "versatility", "leech", "dodge", "parry")
    val rows = players.foldLeft(List[List[Any]]()) { (l, p) =>
      val id = p.playerId
      val s = p.stats
      l.:+(List(id, s.str, s.agi, s.int, s.sta, s.critRating, s.hasteRating, s.masteryRating,
        s.versatility, s.leechRating, s.dodge, s.parry))
    }

    db.upsert("players_stats", columns, rows)
  }

  private def insertPlayersItems(players: Array[Player]): Unit = {
    val columns: List[String] = List("player_id", "average_item_level", "average_item_level_equipped",
      "head", "neck", "shoulder", "back", "chest", "shirt", "tabard", "wrist", "hands", "waist", "legs",
      "feet", "finger1", "finger2", "trinket1", "trinket2", "mainhand", "offhand")
    val rows = players.foldLeft(List[List[Any]]()) { (l, p) =>
      val id = p.playerId
      val i = p.items
      val shirtId = if (i.shirt.nonEmpty) i.shirt.get.id else null
      val tabardId = if (i.tabard.nonEmpty) i.tabard.get.id else null
      val offHandId = if (i.offHand.nonEmpty) i.offHand.get.id else null
      l.:+(List(id, i.averageItemLevel, i.averageItemLevelEquipped, i.head.id, i.neck.id, i.shoulder.id,
        i.back.id, i.chest.id, shirtId, tabardId, i.wrist.id, i.hands.id, i.waist.id, i.legs.id, i.feet.id,
        i.finger1.id, i.finger2.id, i.trinket1.id, i.trinket2.id, i.mainHand.id, offHandId))
    }

    db.upsert("players_items", columns, rows)
  }

  private def insertItems(players: Array[Player]): Unit = {
    val items = players.foldLeft(HashSet[Item]()) { (s, p) =>
      val i = p.items
      if (i.shirt.nonEmpty) {
        s.add(i.shirt.get)
      }
      if (i.tabard.nonEmpty) {
        s.add(i.tabard.get)
      }
      if (i.offHand.nonEmpty) {
        s.add(i.offHand.get)
      }
      s.add(i.head)
      s.add(i.neck)
      s.add(i.shoulder)
      s.add(i.back)
      s.add(i.chest)
      s.add(i.wrist)
      s.add(i.hands)
      s.add(i.waist)
      s.add(i.legs)
      s.add(i.feet)
      s.add(i.finger1)
      s.add(i.finger2)
      s.add(i.trinket1)
      s.add(i.trinket2)
      s.add(i.mainHand)

      s
    }

    val columns: List[String] = List("id", "name", "icon")
    val rows = items.foldLeft(List[List[Any]]()) { (l, i) =>
      l.:+(List(i.id, i.name, i.icon))
    }

    db.upsert("items", columns, rows)
  }

  private def insertPlayersAchievements(players: Array[Player]): Unit = {
    val pvpIds: Array[Int] = db.getAchievementsIds()
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
    leaderboard: Array[LeaderboardEntry]): Unit = {
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
    guild: Option[Guild], achievements: CompletedAchievements, talents: List[TalentTree],
    stats: Stats, items: Items) {
  var realmId: Int = -1
  var playerId: Int = -1
}
case class Guild(name: String)
case class CompletedAchievements(achievementsCompleted: List[Int],
  achievementsCompletedTimestamp: List[Long])
case class TalentTree(selected: Option[Boolean], talents: List[Talent], spec: TalentSpec)
case class Stats(str: Int, agi: Int, int: Int, sta: Int, critRating: Int, hasteRating: Int,
  masteryRating: Int, versatility: Int, leechRating: Double, dodge: Double, parry: Double)
case class Item(id: Int, name: String, icon: String)
case class Items(averageItemLevel: Int, averageItemLevelEquipped: Int, head: Item, neck: Item,
  shoulder: Item, back: Item, chest: Item, shirt: Option[Item], tabard: Option[Item], wrist: Item, hands: Item,
  waist: Item, legs: Item, feet: Item, finger1: Item, finger2: Item, trinket1: Item, trinket2: Item,
  mainHand: Item, offHand: Option[Item])
