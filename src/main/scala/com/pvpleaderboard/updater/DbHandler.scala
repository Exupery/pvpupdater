package com.pvpleaderboard.updater

import java.sql.{ Connection, DriverManager, PreparedStatement }

import org.slf4j.{ Logger, LoggerFactory }
import java.sql.SQLException

/**
 * Handles database CRUD operations.
 */
class DbHandler {

  private val DB_URL: String = "jdbc:" + sys.env("DB_URL")

  private val INSERT: String = "INSERT INTO %s (%s) VALUES %s %s"
  private val DO_NOTHING: String = "ON CONFLICT DO NOTHING"
  private val DO_UPDATE: String = "ON CONFLICT ON CONSTRAINT %s DO UPDATE SET (%s)=(%s)"

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def execute(sql: String, rows: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      var idx: Int = 1
      rows.foreach { row =>
        row.foreach { value =>
          val v = if (value.isInstanceOf[Option[Any]]) {
            value.asInstanceOf[Option[Any]].getOrElse(null)
          } else {
            value
          }
          stmt.setObject(idx, v)
          idx += 1
        }
      }

      return stmt.executeUpdate()
    } catch {
      case sqle: SQLException => {
        logger.error("[SQLException] {}", sqle.getMessage)
        return 0
      }
    } finally {
      db.close()
    }
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

    val valFormat = "(" + (List.fill(values.head.size)("?").mkString(",")) + ")"
    val valString = List.fill(values.size)(valFormat).mkString(",")

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
    val valFormat = "(" + (List.fill(values.head.size)("?").mkString(",")) + ")"
    val valString = List.fill(values.size)(valFormat).mkString(",")
    val sql: String = String.format(INSERT, table, cols, valString, DO_NOTHING)

    val inserted: Int = execute(sql, values)
    logger.debug(s"Inserted ${inserted} rows in ${table}")

    return inserted
  }

  def insertPlayersTalents(values: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val deleteSql: String = """DELETE FROM players_talents WHERE player_id=(
    SELECT players.id FROM players WHERE players.name=? AND players.realm_slug=?)
    """

    val sql: String = """
      INSERT INTO players_talents (player_id, talent_id)
      SELECT players.id, talents.id FROM players JOIN talents ON players.class_id=talents.class_id
      WHERE players.name=? AND players.realm_slug=? AND (players.spec_id=talents.spec_id OR talents.spec_id=0)
      AND talents.spell_id=? ON CONFLICT DO NOTHING
    """

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleteStmt: PreparedStatement = db.prepareStatement(deleteSql)
      var idx: Int = 1
      values.foreach { row =>
        row.foreach { value =>
          stmt.setObject(idx, value)
          if (idx < 3) {
            deleteStmt.setObject(idx, value)
          }
          idx += 1
        }
        idx = 1
        stmt.addBatch()
        deleteStmt.addBatch()
      }

      deleteStmt.executeBatch()
      val inserted: Int = stmt.executeBatch().foldLeft(0)((s, i) => s + i)
      db.commit()
      logger.debug(s"Inserted ${inserted} rows in players_talents")
      return inserted
    } catch {
      case sqle: SQLException => {
        logger.error("[SQLException] {}", sqle.getMessage)
        return 0
      }
    } finally {
      db.close()
    }
  }

  def updateBracket(bracket: String, values: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val deleteSql: String = s"TRUNCATE TABLE bracket_${bracket}"

    val sql: String = s"""
      INSERT INTO bracket_${bracket} (ranking, player_id, rating, season_wins, season_losses)
      SELECT ?, players.id, ?, ?, ? FROM players
      WHERE players.name=? AND players.realm_slug=?
    """

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
      case sqle: SQLException => {
        logger.error("[SQLException] {}", sqle.getMessage)
        return 0
      }
    } finally {
      db.close()
    }
  }

  def insertPlayersAchievements(values: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)

    val sql: String = """
      INSERT INTO players_achievements (player_id, achievement_id, achieved_at)
      SELECT players.id, ?, to_timestamp(?) FROM players
      WHERE players.name=? AND players.realm_slug=?
      AND EXISTS (SELECT 1 FROM achievements WHERE id=?)
      ON CONFLICT DO NOTHING
    """

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      var idx: Int = 1
      values.foreach { row =>
        row.foreach { value =>
          stmt.setObject(idx, value)
          idx += 1
        }
        idx = 1
        stmt.addBatch()
      }

      val inserted: Int = stmt.executeBatch().foldLeft(0)((s, i) => s + i)
      logger.debug(s"Inserted ${inserted} rows in players_achievements")
      return inserted
    } catch {
      case sqle: SQLException => {
        logger.error("[SQLException] {}", sqle.getMessage)
        return 0
      }
    } finally {
      db.close()
    }
  }

  def setUpdateTime(): Unit = {
    val sql: String = """
      INSERT INTO metadata (key, last_update) VALUES ('update_time', NOW())
      ON CONFLICT (key) DO UPDATE SET last_update=NOW()
    """

    execute(sql, List.empty)
  }

}
