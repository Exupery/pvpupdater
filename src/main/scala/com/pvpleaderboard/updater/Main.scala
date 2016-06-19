package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

object Main {
  private def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis() / 1000
    logger.info("Updating PvPLeaderboard DB")
    // TODO
    val end = System.currentTimeMillis() / 1000
    val elapsed = end - start
    val durationMessage = if (elapsed < 180) {
      String.format("%s seconds", elapsed.toString())
    } else {
      String.format("%s minutes", (elapsed / 60).toString())
    }
    logger.info("DB update complete after {}", durationMessage)
  }
}
