# PvPUpdater

Updates the [World of Warcraft](http://us.battle.net/wow/en/) PvP leaderboard data consumed by [pvpleaderboard](https://github.com/Exupery/pvpleaderboard).

Environment variables:
* `DATABASE_URL` the URL of the [PostgreSQL] database to use (required)
* `BATTLE_NET_API_KEY` [battle.net](https://dev.battle.net/) API key (required)
* `BATTLE_NET_API_URI` base URI of the API to use (optional, will default to `https://us.api.battle.net/wow/` if not provided)