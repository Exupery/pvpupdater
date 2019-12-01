package com.pvpleaderboard.updater

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant

import scala.collection.mutable.HashSet
import scala.concurrent.{ blocking, Await, Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.math.max
import scala.util.Try

import org.slf4j.{ Logger, LoggerFactory }

import com.pvpleaderboard.updater.NonApiData.slugify

import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

/**
 * Updates player info.
 */
object PlayerUpdater {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val db: DbHandler = new DbHandler()
  private implicit val formats = DefaultFormats

  private val maxPerBracket: Option[Int] = if (sys.env.isDefinedAt("MAX_PER_BRACKET")) {
    Option(sys.env("MAX_PER_BRACKET").toInt)
  } else {
    Option.empty
  }

  private val DEFAULT_NUM_THREADS: Int = 5
  private val numThreads: Int = Try(sys.env("NUM_THREADS").toInt).getOrElse(DEFAULT_NUM_THREADS)

  private val DEFAULT_ACHIEV_SIZE: Int = 500
  private val achievGroupSize: Int = Try(sys.env("ACHIEV_SIZE").toInt).getOrElse(DEFAULT_ACHIEV_SIZE)

  def update(apis: List[ApiHandler]): Unit = {
    logger.info("Updating player data")

    apis.foreach { api =>
      val season: Int = api.getDynamic("pvp-season/index").get.extract[Seasons].current_season.id
      if (season <= 0) {
        logger.info("No current PvP season")
        return
      }
      val brackets: Brackets = api.getDynamic("pvp-season/" + season + "/pvp-leaderboard/index").get.extract[Brackets]
      brackets.leaderboards.foreach(b => importBracket(season, b.name, api))
    }
    db.setUpdateTime()
  }

  private def importBracket(season: Int, bracket: String, api: ApiHandler): Unit = {
    logger.info("Importing {} {}", api.region.toUpperCase(), bracket: Any)
    val response: Option[JValue] = api.getDynamic("pvp-season/" + season + "/pvp-leaderboard/" + bracket)
    if (response.isEmpty) {
      logger.warn("Skipping {} import", bracket)
      return
    }

    val leaderboard: Array[LeaderboardEntry] =
      response.get.extract[Leaderboard].entries.take(maxPerBracket.getOrElse(Int.MaxValue)).toArray

    if (leaderboard.isEmpty) {
      logger.warn("Empty {} leaderboard", bracket)
      return
    }

    val groupSize: Int = max(leaderboard.size / numThreads, numThreads)
    val futures = leaderboard.grouped(groupSize).map(group => {
      Future {
        blocking {
          importPlayers(group, api)
        }
      }
    }).toList
    logger.debug("Waiting on {} futures", futures.size)
    futures.map(Await.result[Unit](_, 3 hours))
    logger.debug("All {} {} futures complete", api.region.toUpperCase(), bracket: Any)

    updateLeaderboard(bracket, leaderboard, api)
  }

  private def importPlayers(leaderboard: Array[LeaderboardEntry], api: ApiHandler): Unit = {
    logger.debug("Importing {} players", leaderboard.size)
    val players: Array[Player] = getPlayers(leaderboard, api)

    if (players.isEmpty) {
      logger.warn("No player data retrieved, expected {}", leaderboard.size)
      return
    }

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
      "last_update")
    val rows = players.foldLeft(List[List[Any]]()) { (l, p) =>
      val guild = if (p.guild.isDefined) Option(p.guild.get.name) else Option.empty
      val lastLogin = Timestamp.from(Instant.ofEpochMilli(p.last_login_timestamp))

      l.:+(List(
        p.name,
        p.character_class.id,
        p.active_spec.id,
        p.factionId,
        p.race.id,
        p.realmId,
        guild,
        p.genderId,
        p.achievement_points,
        lastLogin))
    }

    db.upsert("players", columns, rows, Option("players_name_realm_id_key"))
    val playerIds: Map[String, Int] = db.getPlayerIds(players.map(p => (p.name, p.realmId)))

    val noId: Int = -1
    logger.debug("Mapping {} player IDs", playerIds.size)
    players.foreach(p => p.playerId = playerIds.getOrElse(p.name + p.realmId, noId))
    logger.debug("Mapped {} player IDs", players.filter(_.playerId > noId).size)

    insertPlayersTalents(players, api)
    insertPlayersStats(players, api)
    insertPlayersItems(players, api)
    players.filter(_.playerId != noId).grouped(achievGroupSize).foreach(group => insertPlayersAchievements(group, api))
  }

  private def getPlayers(leaderboard: Array[LeaderboardEntry], api: ApiHandler): Array[Player] = {
    val path: String = "%s/%s"
    val players = leaderboard.foldLeft(Array[Player]()) { (array, entry) =>
      val response: Option[JValue] = api.getProfile(entry.character.charPath)
      Try(array.:+(response.get.extract[Player])).getOrElse(array)
    }
    logger.debug("Found {} players", players.size)

    val realmIds: Map[String, Int] = db.getRealmIds(api.region)
    players.foreach(p => p.realmId = realmIds(p.realm.slug))

    return players
  }

  private def insertPlayersTalents(players: Array[Player], api: ApiHandler): Unit = {
    val rows = players.foldLeft(List[List[(Int, Int)]]()) { (l, p) =>
      val response: Option[JValue] = api.getProfile(p.charPath + "/specializations")
      if (response.isDefined) {
        val specs: PlayerSpecializations = response.get.extract[PlayerSpecializations]
        val activeSpec: PlayerSpecialization =
          specs.specializations.find(_.specialization.id == p.active_spec.id).get
        l.:+(activeSpec.talents.map(t => (p.playerId, t.spell_tooltip.spell.get.id)))
      } else {
        l
      }
    }.flatten

    db.insertPlayersTalents(rows)
  }

  private def insertPlayersStats(players: Array[Player], api: ApiHandler): Unit = {
    val columns: List[String] = List("player_id", "strength", "agility", "intellect", "stamina",
      "critical_strike", "haste", "mastery", "versatility", "leech", "dodge", "parry")
    val rows = players.foldLeft(List[List[Any]]()) { (l, p) =>
      val response: Option[JValue] = api.getProfile(p.charPath + "/statistics")
      if (response.isDefined) {
        val s: Stats = response.get.extract[Stats]
        val critRating: Int = max(s.melee_crit.rating, s.spell_crit.rating)
        val hasteRating: Int = max(s.melee_haste.rating, s.spell_haste.rating)
        l.:+(List(p.playerId, s.strength.effective, s.agility.effective, s.intellect.effective,
          s.stamina.effective, critRating, hasteRating, s.mastery.rating, s.versatility,
          s.lifesteal.rating, s.dodge.rating, s.parry.rating))
      } else {
        l
      }
    }

    db.upsert("players_stats", columns, rows)
  }

  private def insertPlayersItems(players: Array[Player], api: ApiHandler): Unit = {
    val slots: List[String] = NonApiData.itemSlots
    val columns: List[String] = List("player_id", "average_item_level", "average_item_level_equipped") ++ slots
    val items: Map[Player, Items] = players.toList
      .map(p => p -> api.getProfile(p.charPath + "/equipment"))
      .filter(_._2.isDefined)
      .map(t => t._1 -> t._2.get.extract[Items])
      .toMap
    val rows = items.foldLeft(List[List[Any]]()) { (l, entry) =>
      val player: Player = entry._1
      val items: Items = entry._2
      val equippedItems: List[Any] = slots.map(slot => items.get(slot).getOrElse(null))
      l.:+(List(player.playerId, player.average_item_level, player.equipped_item_level) ++ equippedItems)
    }

    db.upsert("players_items", columns, rows)

    insertItems(items.values.toList)
  }

  private def insertItems(items: List[Items]): Unit = {
    val equippedItem: List[EquippedItem] = items.map(_.equipped_items).flatten
    val columns: List[String] = List("id", "name")
    val rows = equippedItem.toSet.foldLeft(List[List[Any]]()) { (list, item) =>
      list.:+(List(item.item.id, item.name))
    }

    db.upsert("items", columns, rows)
  }

  private def insertPlayersAchievements(players: Array[Player], api: ApiHandler): Unit = {
    val pvpIds: Set[Int] = NonApiData.getAchievementsIds()
    val rows = players.foldLeft(List[List[List[Any]]]()) { (l, p) =>
      val response: Option[JValue] = api.getProfile(p.charPath + "/achievements")
      if (response.isDefined) {
        val achievements: List[PlayerAchievement] = response.get.extract[Achievements].achievements
          .filter(a => pvpIds.contains(a.id))
          .filter(_.completed_timestamp.isDefined)
        l.:+(achievements.map(a => List(p.playerId, a.id, a.completed_timestamp.get / 1000)))
      } else {
        l
      }
    }.flatten

    db.insertPlayersAchievements(rows)
  }

  private def updateLeaderboard(bracket: String, leaderboard: Array[LeaderboardEntry], api: ApiHandler): Unit = {
    val realmIds: Map[String, Int] = db.getRealmIds(api.region)
    val rows = leaderboard.foldLeft(List[List[Any]]()) { (l, entry) =>
      l.:+(List(
        entry.rank,
        entry.rating,
        entry.season_match_statistics.won,
        entry.season_match_statistics.lost,
        entry.character.name,
        realmIds(entry.character.realm.slug)))
    }

    db.updateBracket(bracket, api.region, rows)
  }

}

case class Id(id: Int)

case class Seasons(seasons: List[Id], current_season: Id)
case class Brackets(season: Id, leaderboards: List[KeyedValue])
case class Leaderboard(season: Id, name: String, entries: List[LeaderboardEntry])
case class LeaderboardEntry(character: Character, rank: Int, rating: Int, season_match_statistics: SeasonStats)
case class SeasonStats(played: Int, won: Int, lost: Int)

abstract class HasCharPath(name: String, realm: Realm) {
  val charPath: String = realm.slug + "/" + URLEncoder.encode(name.toLowerCase, StandardCharsets.UTF_8.name)
}
case class Character(name: String, id: Int, realm: Realm) extends HasCharPath(name, realm)
case class Player(id: Int, name: String, gender: Gender, faction: PlayerFaction, race: KeyedValue,
  character_class: KeyedValue, active_spec: KeyedValue, realm: Realm, guild: Option[KeyedValue],
  achievement_points: Int, average_item_level: Int, equipped_item_level: Int, last_login_timestamp: Long)
  extends HasCharPath(name, realm) {
    var realmId: Int = -1
    var playerId: Int = -1
    val factionId: Int = if ("HORDE".equals(faction.`type`)) 1 else 0
    val genderId: Int = if ("FEMALE".equals(gender.`type`)) 1 else 0
}

case class PlayerFaction(`type`: String, name: String)
case class Gender(`type`: String, name: String)
case class Guild(name: String)

case class Achievements(total_quantity: Int, total_points: Int, achievements: List[PlayerAchievement],
  character: Character)
case class PlayerAchievement(id: Int, completed_timestamp: Option[Long])

case class PlayerSpecializations(specializations: List[PlayerSpecialization], active_specialization: KeyedValue,
  character: Character)
case class PlayerSpecialization(specialization: KeyedValue, talents: List[TalentListing], glyphs: List[KeyedValue],
  pvp_talent_slots: List[PvpTalentSlot])
case class PvpTalentSlot(selected: TalentListing, slot_number: Int)

case class Stats(health: Int, power: Int, power_type: KeyedValue, strength: Stat, agility: Stat, intellect: Stat,
  stamina: Stat, mastery: Rating, lifesteal: Rating, versatility: Int, avoidance: Rating, attack_power: Int,
  dodge: Rating, parry: Rating, melee_crit: Rating, spell_crit: Rating, ranged_crit: Rating, melee_haste: Rating,
  spell_haste: Rating, ranged_haste: Rating)
case class Stat(base: Int, effective: Int)
case class Rating(rating: Int, rating_bonus: Double, value: Option[Double])

case class Items(character: Character, equipped_items: List[EquippedItem]) {
  private val items: Map[String, EquippedItem] =
    equipped_items.map(item => item.slot.`type`.toLowerCase.replaceAll("_", "") -> item).toMap
  def get(slot: String): Option[Int] = {
    if (items.contains(slot)) {
      return Option(items.get(slot).get.item.id)
    } else {
      Option.empty
    }
  }
}
case class EquippedItem(item: Id, slot: Slot, name: String)
case class Slot(`type`: String, name: String)
