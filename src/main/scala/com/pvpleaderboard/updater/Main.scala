package com.pvpleaderboard.updater

import java.sql.DriverManager

import org.slf4j.{ Logger, LoggerFactory }

object Main {
  private def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Updating PvPLeaderboard DB")
    // TODO DELME BEGIN
    val dbUrl = "jdbc:" + sys.env("DATABASE_URL")
    val db = DriverManager.getConnection(dbUrl)
    db.close()
    println(db.isClosed())
    // TODO DELME END
    logger.info("PvPLeaderboard DB update complete")
  }
}
