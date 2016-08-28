package com.pvpleaderboard.updater

import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet, SQLException }

import org.slf4j.{ Logger, LoggerFactory }

import com.pvpleaderboard.updater.NonApiData.slugifyRealm

/**
 * Handles database CRUD operations.
 */
class DbHandler {

  private val DB_URL: String = "jdbc:" + sys.env("DB_URL")

  private val INSERT: String = "INSERT INTO %s (%s) VALUES %s %s"
  private val DO_NOTHING: String = "ON CONFLICT DO NOTHING"
  private val DO_UPDATE: String = "ON CONFLICT ON CONSTRAINT %s DO UPDATE SET (%s)=(%s)"

  private val BATCH_SIZE: Int = if (sys.env.isDefinedAt("BATCH_SIZE")) {
    sys.env("BATCH_SIZE").toInt
  } else {
    1000
  }

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def execute(sql: String, rows: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    var count: Int = 0

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      rows.grouped(BATCH_SIZE).foreach { group =>
        group.foreach { row =>
          var idx: Int = 1
          row.foreach { value =>
            val v = if (value.isInstanceOf[Option[Any]]) {
              value.asInstanceOf[Option[Any]].getOrElse(null)
            } else {
              value
            }
            stmt.setObject(idx, v)
            idx += 1
          }
          stmt.addBatch()
        }
        count += stmt.executeBatch().foldLeft(0)(_ + _)
      }
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return count
  }

  /**
   * Inserts rows into {@code table}, on violation of the table's
   * primary key the conflicting row(s) will be updated using the
   * provided {@code values}.
   */
  def upsert(table: String, columns: List[String], values: List[List[Any]],
    conflictKey: Option[String] = Option.empty): Int = {
    if (columns.isEmpty || values.isEmpty) {
      return 0
    }
    val cols: String = columns.mkString(",")

    val valString = "(" + (List.fill(values.head.size)("?").mkString(",")) + ")"
    val updValString = columns.map("EXCLUDED." + _).mkString(",")
    val key: String = conflictKey.getOrElse(table + "_pkey")
    val updateString = String.format(DO_UPDATE, key, cols, updValString)

    val sql: String = String.format(INSERT, table, cols, valString, updateString)

    val inserted: Int = execute(sql, values)
    logger.debug(s"Inserted or updated ${inserted} rows in ${table}")

    return inserted
  }

  /**
   * Inserts rows into {@code table}, doing nothing on conflict.
   */
  def insertDoNothing(table: String, columns: List[String], values: List[List[Any]]): Int = {
    if (columns.isEmpty || values.isEmpty) {
      return 0
    }
    val cols: String = columns.mkString(",")
    val valString = "(" + (List.fill(values.head.size)("?").mkString(",")) + ")"
    val sql: String = String.format(INSERT, table, cols, valString, DO_NOTHING)

    val inserted: Int = execute(sql, values)
    logger.debug(s"Inserted ${inserted} rows in ${table}")

    return inserted
  }

  def insertPlayersTalents(ids: List[(Int, Int)]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val deleteSql: String = "DELETE FROM players_talents WHERE player_id=?"

    val sql: String = """
      INSERT INTO players_talents (player_id, talent_id)
      SELECT ?, talents.id FROM players JOIN talents ON players.class_id=talents.class_id
      WHERE players.id=? AND (players.spec_id=talents.spec_id OR talents.spec_id=0)
      AND talents.spell_id=?
    """ + DO_NOTHING

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleteStmt: PreparedStatement = db.prepareStatement(deleteSql)
      ids.foreach { t =>
        val playerId: Int = t._1

        stmt.setInt(1, playerId)
        stmt.setInt(2, playerId)
        stmt.setInt(3, t._2)
        deleteStmt.setInt(1, playerId)

        stmt.addBatch()
        deleteStmt.addBatch()
      }

      deleteStmt.executeBatch()
      val inserted: Int = stmt.executeBatch().foldLeft(0)((s, i) => s + i)
      db.commit()
      logger.debug(s"Inserted ${inserted} rows in players_talents")
      return inserted
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return 0
  }

  def updateBracket(bracket: String, values: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val deleteSql: String = s"TRUNCATE TABLE bracket_${bracket}"

    val sql: String = s"""
      INSERT INTO bracket_${bracket} (ranking, player_id, rating, season_wins, season_losses)
      SELECT ?, players.id, ?, ?, ? FROM players
      WHERE players.name=? AND players.realm_slug=?
    """ + DO_NOTHING

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleteStmt: PreparedStatement = db.prepareStatement(deleteSql)
      var idx: Int = 1
      values.foreach { row =>
        row.foreach { value =>
          stmt.setObject(idx, value)
          idx += 1
        }
        idx = 1
        stmt.addBatch()
      }

      deleteStmt.execute()
      val inserted: Int = stmt.executeBatch().foldLeft(0)((s, i) => s + i)
      db.commit()
      logger.debug(s"Populated bracket_${bracket} with ${inserted} rows")
      return inserted
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return 0
  }

  def insertPlayersAchievements(values: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)

    val sql: String = """
      INSERT INTO players_achievements (player_id, achievement_id, achieved_at)
      SELECT ?, ?, to_timestamp(?)
      WHERE EXISTS (SELECT 1 FROM achievements WHERE id=?)
    """ + DO_NOTHING

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      values.foreach { row =>
        stmt.setObject(1, row(0))
        stmt.setObject(2, row(1))
        stmt.setObject(3, row(2))
        stmt.setObject(4, row(1))
        stmt.addBatch()
      }

      val inserted: Int = stmt.executeBatch().foldLeft(0)((s, i) => s + i)
      logger.debug(s"Inserted ${inserted} rows in players_achievements")
      return inserted
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return 0
  }

  def setUpdateTime(): Unit = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    val sql: String = """
      INSERT INTO metadata (key, last_update) VALUES ('update_time', NOW())
      ON CONFLICT (key) DO UPDATE SET last_update=NOW()
    """
    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      stmt.executeUpdate()
      stmt.close()
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }
  }

  def getPlayerIds(players: List[(String, String)]): Map[String, Int] = {
    logger.debug("Getting player IDs for {} players", players.size)
    val sql: String = "SELECT id FROM players WHERE name=? AND realm_slug=?"
    val db: Connection = DriverManager.getConnection(DB_URL)

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val idMap = players.foldLeft(Map[String, Int]()) { (map, player) =>
        val name: String = player._1
        val realm: String = player._2
        stmt.setString(1, name)
        stmt.setString(2, slugifyRealm(realm))
        val rs: ResultSet = stmt.executeQuery()
        if (rs.next()) {
          val id = rs.getInt(1)
          map.+((name + realm) -> id)
        } else {
          logger.warn(s"${name} (${realm}) is not in players table")
          map
        }
      }

      logger.debug("Found {} player IDs", idMap.size)
      return idMap
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return Map.empty
  }

  def getAchievementsIds(): Set[Int] = {
    val sql: String = "SELECT id FROM achievements"
    val db: Connection = DriverManager.getConnection(DB_URL)

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val rs: ResultSet = stmt.executeQuery()

      return Iterator.continually(rs.next()).takeWhile(identity)
        .map(_ => rs.getInt(1)).toSet
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return Set.empty
  }

  private def logSqlException(sqle: SQLException): Unit = {
    logger.error("[SQLException] {}", sqle.getMessage)
    if (sqle.getNextException != null) {
      logger.error("Next exception: {}", sqle.getNextException.getMessage)
    }
  }

}
