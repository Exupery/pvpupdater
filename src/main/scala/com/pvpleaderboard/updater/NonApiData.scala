package com.pvpleaderboard.updater

/**
 * Data that is not available via the battle.net API
 */
object NonApiData {
  val factions: List[Faction] = List(
    Faction(0, "Alliance"),
    Faction(1, "Horde"))
}
