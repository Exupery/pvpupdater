package com.pvpleaderboard.updater

import java.sql.{ Connection, DriverManager, PreparedStatement, ResultSet, SQLException }

import org.slf4j.{ Logger, LoggerFactory }

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
    logger.debug(s"Upserting ${values.size} rows")
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

  def insertTalents(values: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val sql: String = """
      INSERT INTO talents (id, spell_id, class_id, spec_id, name, description, icon, tier, col)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET spec_id=0"""
    val deleteSql: String = "TRUNCATE TABLE talents CASCADE"

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleteStmt: PreparedStatement = db.prepareStatement(deleteSql)
      values.foreach { row =>
        var idx: Int = 1
        row.foreach { v => stmt.setObject(idx, v); idx += 1 }
        stmt.addBatch()
      }

      deleteStmt.execute()
      val inserted: Int = stmt.executeBatch().foldLeft(0)((s, i) => s + i)
      db.commit()
      logger.debug(s"Inserted ${inserted} rows in talents")
      return inserted
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return 0
  }

  def insertPlayersTalents(ids: List[(Int, Int)]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val deleteSql: String = "DELETE FROM players_talents WHERE player_id=?"

    val sql: String = """
      INSERT INTO players_talents (player_id, talent_id) VALUES (?, ?)
    """ + DO_NOTHING

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleteStmt: PreparedStatement = db.prepareStatement(deleteSql)
      ids.foreach { t =>
        val playerId: Int = t._1

        stmt.setInt(1, playerId)
        stmt.setInt(2, t._2)
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

  def updateBracket(bracket: String, region: String, values: List[List[Any]]): Int = {
    val regionUpper: String = region.toUpperCase()
    logger.debug(s"Updating ${regionUpper} ${bracket} with ${values.size} rows")
    val db: Connection = DriverManager.getConnection(DB_URL)
    db.setAutoCommit(false)

    val deleteSql: String = "DELETE FROM leaderboards WHERE bracket=? AND region=?"

    val sql: String = s"""
      INSERT INTO leaderboards (bracket, region, ranking, player_id, rating, season_wins,
      season_losses) SELECT '${bracket}', '${regionUpper}', ?, players.id, ?, ?, ?
      FROM players WHERE players.name=? AND players.realm_id=?
    """ + DO_NOTHING

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleteStmt: PreparedStatement = db.prepareStatement(deleteSql)
      deleteStmt.setString(1, bracket)
      deleteStmt.setString(2, regionUpper)

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
      logger.debug(s"Populated ${regionUpper} ${bracket} with ${inserted} rows")
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
    """ + DO_NOTHING

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      values.foreach { row =>
        stmt.setObject(1, row(0))
        stmt.setObject(2, row(1))
        stmt.setObject(3, row(2))
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

  def purgeStale(): Unit = {
    purgeStalePlayerInfo()
    purgeStaleItems()
  }

  private def purgeStalePlayerInfo(): Unit = {
    logger.info("Deleting data for players no longer on a leaderboard")
    val db: Connection = DriverManager.getConnection(DB_URL)

    val sql: String = "DELETE FROM %s WHERE %s NOT IN (SELECT player_id FROM leaderboards)"
    val playerTables = List("players_achievements", "players_talents", "players_stats", "players_items", "players")

    try {
      playerTables.foreach { table =>
        val idCol: String = if (table.equals("players")) "id" else "player_id"
        val stmt: PreparedStatement = db.prepareStatement(String.format(sql, table, idCol))
        val deleted: Int = stmt.executeUpdate()
        logger.info("Deleted {} rows from {}", deleted, table)
        stmt.close()
      }
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }
  }

  private def purgeStaleItems(): Unit = {
    logger.info("Deleting items no longer equipped by any players")
    val db: Connection = DriverManager.getConnection(DB_URL)

    var sql: String = "DELETE FROM items WHERE id NOT IN ("

    try {
      var first: Boolean = true
      getEquippedItems.foreach { id =>
        if (!first) {
          sql += ","
        }
        sql += id
        first = false
      }
      sql += ")"

      val stmt: PreparedStatement = db.prepareStatement(sql)
      val deleted: Int = stmt.executeUpdate()
      logger.info("Deleted {} rows from items", deleted)
      stmt.close()
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }
  }

  private def getEquippedItems(): Set[Int] = {
    val columns: List[String] = NonApiData.itemSlots
    val sql: String = "SELECT DISTINCT(%1$s) FROM players_items WHERE %1$s IS NOT NULL"
    val db: Connection = DriverManager.getConnection(DB_URL)

    try {
      val ids = columns.map(col => {
        val stmt: PreparedStatement = db.prepareStatement(String.format(sql, col))
        val rs: ResultSet = stmt.executeQuery()
        Iterator.continually(rs.next()).takeWhile(identity).map(_ => rs.getInt(col))
      }).flatten.toSet
      return ids
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return Set.empty
  }

  def getPlayerIds(players: Array[(String, Int)]): Map[String, Int] = {
    logger.debug("Getting player IDs for {} players", players.size)
    val sql: String = "SELECT id FROM players WHERE name=? AND realm_id=?"
    val db: Connection = DriverManager.getConnection(DB_URL)

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      val idMap = players.foldLeft(Map[String, Int]()) { (map, player) =>
        val name: String = player._1
        val realm: Int = player._2
        stmt.setString(1, name)
        stmt.setInt(2, realm)
        val rs: ResultSet = stmt.executeQuery()
        if (rs.next()) {
          val id = rs.getInt("id")
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

  /**
   * Returns a map of realm IDs for the given {@code region} using
   * the realm slug as the key.
   */
  def getRealmIds(region: String): Map[String, Int] = {
    val sql: String = "SELECT name, slug, id FROM realms WHERE region=?"
    val db: Connection = DriverManager.getConnection(DB_URL)

    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      stmt.setString(1, region.toUpperCase())
      val rs: ResultSet = stmt.executeQuery()
      val col: String = "slug"
      return Iterator.continually(rs.next()).takeWhile(identity)
        .map(_ => rs.getString(col) -> rs.getInt("id")).toMap
    } catch {
      case sqle: SQLException => logSqlException(sqle)
    } finally {
      db.close()
    }

    return Map.empty
  }

  private def logSqlException(sqle: SQLException): Unit = {
    logger.error("[SQLException] {}", sqle.getMessage)
    if (sqle.getNextException != null) {
      logger.error("Next exception: {}", sqle.getNextException.getMessage)
    }
  }

}
