package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

object Main {
  private def logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Updating PvPLeaderboard DB")
    // TODO
    logger.info("PvPLeaderboard DB update complete")
  }
}
