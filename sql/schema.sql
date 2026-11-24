-- SQLite3 schema for the processed USGS earthquake events.
-- event_id is the USGS event identifier -> natural PRIMARY KEY (prevents duplicates).
CREATE TABLE IF NOT EXISTS earthquakes (
    event_id              TEXT    PRIMARY KEY,
    event_time            TEXT    NOT NULL,              -- ISO-8601, UTC
    updated_time          TEXT    NOT NULL,              -- ISO-8601, UTC
    event_date            TEXT    NOT NULL,              -- YYYY-MM-DD
    event_hour            INTEGER NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    day_of_week           TEXT    NOT NULL,
    magnitude             REAL    NOT NULL CHECK (magnitude BETWEEN -1 AND 10),
    mag_type              TEXT    NOT NULL,
    magnitude_class       TEXT    NOT NULL,
    depth_km              REAL    NOT NULL,
    depth_class           TEXT    NOT NULL CHECK (depth_class IN ('shallow', 'intermediate', 'deep')),
    latitude              REAL    NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude             REAL    NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    place                 TEXT,
    region                TEXT    NOT NULL,
    event_type            TEXT    NOT NULL,
    status                TEXT    NOT NULL CHECK (status IN ('automatic', 'reviewed')),
    network               TEXT    NOT NULL,
    alert_level           TEXT    NOT NULL,
    tsunami_flag          INTEGER NOT NULL CHECK (tsunami_flag IN (0, 1)),
    significance          INTEGER NOT NULL CHECK (significance >= 0),
    felt_reports          INTEGER NOT NULL CHECK (felt_reports >= 0),
    station_count         INTEGER NOT NULL,
    azimuthal_gap         REAL,
    rms                   REAL,
    min_station_distance  REAL,
    energy_joules         REAL    NOT NULL,
    log10_energy          REAL    NOT NULL,
    distance_to_almaty_km REAL    NOT NULL,
    url                   TEXT
);

CREATE INDEX IF NOT EXISTS idx_earthquakes_region ON earthquakes(region);
CREATE INDEX IF NOT EXISTS idx_earthquakes_date   ON earthquakes(event_date);
