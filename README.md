[![Build Status](https://travis-ci.org/Exupery/pvpupdater.svg)](https://travis-ci.org/Exupery/pvpupdater)
# PvPUpdater

Updates the [World of Warcraft](https://worldofwarcraft.com/en-us/) PvP leaderboard data consumed by [pvpleaderboard](https://github.com/Exupery/pvpleaderboard).

Environment variables:
* `DB_URL` the URL of the [PostgreSQL] database to use (required)
* `BATTLE_NET_API_KEY` [battle.net](https://dev.battle.net/) API key (required)
* `NUM_THREADS` number of threads to use when retrieving player data (optional)
