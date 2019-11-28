package com.pvpleaderboard.updater

/**
 * Data that is not available via the battle.net API
 */
object NonApiData {
  val factions: List[Faction] = List(
    Faction(0, "Alliance"),
    Faction(1, "Horde"))

  def getAchievementsIds(): Set[Int] = {
    return Set(
      // Arena achievements
      401, 405, 404, 1159, 1160, 1161, 5266, 5267, 876, 2090, 2093, 2092, 2091,
      // RBG achievements
      5329, 5326, 5339, 5353, 5341, 5355, 5343, 5356, 6942, 6941
      )
  }

  def slugify(str: String): String = {
    return str.toLowerCase().replaceAll(" ", "-")
  }
}
