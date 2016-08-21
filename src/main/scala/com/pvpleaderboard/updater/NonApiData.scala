package com.pvpleaderboard.updater

/**
 * Data that is not available via the battle.net API
 */
object NonApiData {
  val factions: List[Faction] = List(
    Faction(0, "Alliance"),
    Faction(1, "Horde"))

  val specIds: Map[String, Int] = Map(
    "mageArcane" -> 62,
    "mageFire" -> 63,
    "mageFrost" -> 64,
    "paladinHoly" -> 65,
    "paladinProtection" -> 66,
    "paladinRetribution" -> 70,
    "warriorArms" -> 71,
    "warriorFury" -> 72,
    "warriorProtection" -> 73,
    "druidBalance" -> 102,
    "druidFeral" -> 103,
    "druidGuardian" -> 104,
    "druidRestoration" -> 105,
    "death-knightBlood" -> 250,
    "death-knightFrost" -> 251,
    "death-knightUnholy" -> 252,
    "hunterBeast Mastery" -> 253,
    "hunterMarksmanship" -> 254,
    "hunterSurvival" -> 255,
    "priestDiscipline" -> 256,
    "priestHoly" -> 257,
    "priestShadow" -> 258,
    "rogueAssassination" -> 259,
    "rogueOutlaw" -> 260,
    "rogueSubtlety" -> 261,
    "shamanElemental" -> 262,
    "shamanEnhancement" -> 263,
    "shamanRestoration" -> 264,
    "warlockAffliction" -> 265,
    "warlockDemonology" -> 266,
    "warlockDestruction" -> 267,
    "monkBrewmaster" -> 268,
    "monkWindwalker" -> 269,
    "monkMistweaver" -> 270,
    "demon-hunterHavoc" -> 577,
    "demon-hunterVengeance" -> 581)

  def slugify(str: String): String = {
    return str.toLowerCase().replaceAll(" ", "-")
  }

  def slugifyRealm(str: String): String = {
    return slugify(str).replaceAll("'", "")
  }
}
