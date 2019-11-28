package com.pvpleaderboard.updater

import org.slf4j.{ Logger, LoggerFactory }

object Main {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis() / 1000
    logger.info("Updating PvPLeaderboard DB")
    val apis: List[ApiHandler] = List(new ApiHandler(Region.US), new ApiHandler(Region.EU))
    NonPlayerUpdater.update(apis)
    PlayerUpdater.update(apis)
    // TODO CLEANUP
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
