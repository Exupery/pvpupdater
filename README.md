[![Build Status](https://travis-ci.org/Exupery/pvpupdater.svg)](https://travis-ci.org/Exupery/pvpupdater)
# PvPUpdater

Updates the [World of Warcraft](https://worldofwarcraft.com/en-us/) PvP leaderboard data consumed by [pvpleaderboard](https://github.com/Exupery/pvpleaderboard).

Environment variables:
* `DB_URL` the URL of the [PostgreSQL] database to use (required)
* `BATTLE_NET_CLIENT_ID` [battle.net](https://develop.battle.net/) Client ID (required)
* `BATTLE_NET_SECRET` [battle.net](https://develop.battle.net/) Secret (required)
* `NUM_THREADS` number of threads to use when retrieving player data (optional)
