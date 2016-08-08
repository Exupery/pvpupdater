package com.pvpleaderboard.updater

import java.sql.{ Connection, DriverManager, PreparedStatement }

import org.slf4j.{ Logger, LoggerFactory }
import java.sql.SQLException

/**
 * Handles database CRUD operations.
 */
class DbHandler {

  private val DB_URL: String = "jdbc:" + sys.env("DATABASE_URL")

  private val INSERT: String = "INSERT INTO %s (%s) VALUES %s %s"
  private val DO_NOTHING: String = "ON CONFLICT DO NOTHING"

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def execute(sql: String, rows: List[List[Any]]): Int = {
    val db: Connection = DriverManager.getConnection(DB_URL)
    try {
      val stmt: PreparedStatement = db.prepareStatement(sql)
      var idx: Int = 1
      rows.foreach { row =>
        row.foreach { value =>
          stmt.setObject(idx, value)
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

}