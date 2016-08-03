CREATE TABLE realms (
  slug VARCHAR(64) PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  battlegroup VARCHAR(64),
  timezone VARCHAR(64),
  type VARCHAR(16),
  UNIQUE (name)
);

CREATE TABLE races (
  id INTEGER PRIMARY KEY,
  name VARCHAR(32) NOT NULL,
  side VARCHAR(32) NOT NULL,
  UNIQUE (name, side)
);

CREATE TABLE factions (
  id INTEGER PRIMARY KEY,
  name VARCHAR(32) NOT NULL,
  UNIQUE (name)
);

CREATE TABLE classes (
  id INTEGER PRIMARY KEY,
  name VARCHAR(32) NOT NULL,
  UNIQUE (name)
);

CREATE TABLE specs (
  id INTEGER PRIMARY KEY,
  class_id INTEGER NOT NULL REFERENCES classes (id),
  name VARCHAR(32) NOT NULL,
  role VARCHAR(32),
  description VARCHAR(1024),
  background_image VARCHAR(128),
  icon VARCHAR(128),
  UNIQUE (class_id, name)
);

CREATE TABLE talents (
  id INTEGER PRIMARY KEY,
  class_id INTEGER NOT NULL REFERENCES classes (id),
  name VARCHAR(128) NOT NULL,
  description VARCHAR(1024),
  icon VARCHAR(128),
  tier SMALLINT,
  col SMALLINT
);

CREATE INDEX ON talents (tier, col);
CREATE INDEX ON talents (class_id, name);

CREATE TABLE glyphs (
  id INTEGER PRIMARY KEY,
  class_id INTEGER NOT NULL REFERENCES classes (id),
  name VARCHAR(128) NOT NULL,
  icon VARCHAR(128),
  item_id INTEGER,
  type_id SMALLINT,
  spell_id INTEGER,
  UNIQUE (class_id, name)
);

CREATE TABLE players (
  id SERIAL PRIMARY KEY,
  name VARCHAR(32) NOT NULL,
  class_id INTEGER REFERENCES classes (id),
  spec_id INTEGER REFERENCES specs (id),
  faction_id INTEGER REFERENCES factions (id),
  race_id INTEGER REFERENCES races (id),
  realm_slug VARCHAR(64) NOT NULL REFERENCES realms (slug),
  guild VARCHAR(64),
  gender SMALLINT,
  achievement_points INTEGER,
  honorable_kills INTEGER,
  last_update TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE (name, realm_slug)
);

CREATE INDEX ON players (class_id, spec_id);
CREATE INDEX ON players (faction_id, race_id);
CREATE INDEX ON players (guild);
CREATE INDEX ON players (last_update DESC);

CREATE TABLE bracket_2v2 (
  ranking INTEGER PRIMARY KEY,
  player_id INTEGER NOT NULL REFERENCES players (id),
  rating SMALLINT NOT NULL,
  season_wins SMALLINT,
  season_losses SMALLINT,
  last_update TIMESTAMP DEFAULT NOW(),
  UNIQUE (player_id)
);

CREATE INDEX ON bracket_2v2 (rating);
CREATE INDEX ON bracket_2v2 (last_update DESC);

CREATE TABLE bracket_3v3 (
  ranking INTEGER PRIMARY KEY,
  player_id INTEGER NOT NULL REFERENCES players (id),
  rating SMALLINT NOT NULL,
  season_wins SMALLINT,
  season_losses SMALLINT,
  last_update TIMESTAMP DEFAULT NOW(),
  UNIQUE (player_id)
);

CREATE INDEX ON bracket_3v3 (rating);
CREATE INDEX ON bracket_3v3 (last_update DESC);

CREATE TABLE bracket_5v5 (
  ranking INTEGER PRIMARY KEY,
  player_id INTEGER NOT NULL REFERENCES players (id),
  rating SMALLINT NOT NULL,
  season_wins SMALLINT,
  season_losses SMALLINT,
  last_update TIMESTAMP DEFAULT NOW(),
  UNIQUE (player_id)
);

CREATE INDEX ON bracket_5v5 (rating);
CREATE INDEX ON bracket_5v5 (last_update DESC);

CREATE TABLE bracket_rbg (
  ranking INTEGER PRIMARY KEY,
  player_id INTEGER NOT NULL REFERENCES players (id),
  rating SMALLINT NOT NULL,
  season_wins SMALLINT,
  season_losses SMALLINT,
  last_update TIMESTAMP DEFAULT NOW(),
  UNIQUE (player_id)
);

CREATE INDEX ON bracket_rbg (rating);
CREATE INDEX ON bracket_rbg (last_update DESC);

CREATE TABLE achievements (
  id INTEGER PRIMARY KEY,
  name VARCHAR(128),
  description VARCHAR(1024),
  icon VARCHAR(128),
  points SMALLINT
);

CREATE INDEX ON achievements (name);

CREATE TABLE players_achievements (
  player_id INTEGER NOT NULL REFERENCES players (id),
  achievement_id INTEGER NOT NULL REFERENCES achievements (id),
  achieved_at TIMESTAMP,
  PRIMARY KEY (player_id, achievement_id)
);

CREATE TABLE players_talents (
  player_id INTEGER NOT NULL REFERENCES players (id),
  talent_id INTEGER NOT NULL REFERENCES talents (id),
  PRIMARY KEY (player_id, talent_id)
);

CREATE TABLE players_glyphs (
  player_id INTEGER NOT NULL REFERENCES players (id),
  glyph_id INTEGER NOT NULL REFERENCES glyphs (id),
  PRIMARY KEY (player_id, glyph_id)
);

CREATE TABLE players_stats (
  player_id INTEGER PRIMARY KEY REFERENCES players (id),
  strength INTEGER,
  agility INTEGER,
  intellect INTEGER,
  stamina INTEGER,
  spirit INTEGER,
  critical_strike INTEGER,
  haste INTEGER,
  attack_power INTEGER,
  mastery INTEGER,
  multistrike INTEGER,
  versatility INTEGER,
  leech INTEGER,
  dodge INTEGER,
  parry INTEGER
);

CREATE VIEW player_ids_all_brackets AS
  SELECT player_id FROM bracket_2v2 UNION
  SELECT player_id FROM bracket_3v3 UNION
  SELECT player_id FROM bracket_5v5 UNION
  SELECT player_id FROM bracket_rbg;

CREATE TABLE items (
  id INTEGER PRIMARY KEY,
  name VARCHAR(128),
  icon VARCHAR(128),
  item_level INTEGER
);

CREATE TABLE players_items (
  player_id INTEGER PRIMARY KEY REFERENCES players (id),
  average_item_level INTEGER,
  average_item_level_equipped INTEGER,
  head INTEGER,
  neck INTEGER,
  shoulder INTEGER,
  back INTEGER,
  chest INTEGER,
  shirt INTEGER,
  tabard INTEGER,
  wrist INTEGER,
  hands INTEGER,
  waist INTEGER,
  legs INTEGER,
  feet INTEGER,
  finger1 INTEGER,
  finger2 INTEGER,
  trinket1 INTEGER,
  trinket2 INTEGER,
  mainhand INTEGER,
  offhand INTEGER
);

CREATE TABLE metadata (
  key VARCHAR(32) PRIMARY KEY,
  value VARCHAR(512) NOT NULL DEFAULT '',
  last_update TIMESTAMP DEFAULT NOW()
);
