"""
schema_data.py
==============
CIS 3120 · MP02 — SQL and Database
Author 1 module — schema creation and seed data

CONTRACT SUMMARY
----------------
Implement build_database(conn) and seed_database(conn) exactly as specified.
The Integrator's main.py and Author 2's queries.py depend on the table names
and column names defined here.  Do not rename any column.

REQUIRED (graded):
    ✓ build_database(conn)   — creates four tables; PRAGMA foreign_keys = ON first
    ✓ seed_database(conn)    — populates all four tables with executemany; commits
    ✓ IntegrityError demo    — in __main__ block; catches a bad artist_id insert
    ✓ .backup() to music.db  — in __main__ block; prints confirmation
    ✓ INSERT OR IGNORE        — used in all INSERT statements in seed_database()
    ✓ Isolation               — this module must NOT import from queries.py or main.py
"""

import sqlite3
import os


# ─────────────────────────────────────────────────────────────────────────────
# PART 1 — Schema creation
# ─────────────────────────────────────────────────────────────────────────────

def build_database(conn):
    """Create the four-table music schema in the database referenced by conn.

    Requirements (all graded):
      - Call conn.execute("PRAGMA foreign_keys = ON;") as the FIRST statement.
      - Use CREATE TABLE IF NOT EXISTS for every table.
      - Create tables in dependency order so foreign key references resolve:
            Artist  →  Track  →  Playlist  →  PlaylistTrack
      - PlaylistTrack must declare a composite PRIMARY KEY (playlist_id, track_id).
      - Call conn.commit() at the end.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open SQLite connection.  May be :memory: or a file-backed database.

    Returns
    -------
    None
    """
    # Step 1 — enable foreign key enforcement  (DO NOT REMOVE THIS LINE)
    conn.execute("PRAGMA foreign_keys = ON;")

    # Step 2 — Artist table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Artist (
            artist_id    INTEGER PRIMARY KEY,
            name         TEXT    NOT NULL,
            genre        TEXT    NOT NULL,
            origin_city  TEXT
        )
    """)

    # Step 3 — Track table  (references Artist)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Track (
            track_id         INTEGER PRIMARY KEY,
            title            TEXT    NOT NULL,
            duration_seconds INTEGER NOT NULL,
            artist_id        INTEGER NOT NULL
                REFERENCES Artist(artist_id)
        )
    """)

    # Step 4 — Playlist table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS Playlist (
            playlist_id    INTEGER PRIMARY KEY,
            playlist_name  TEXT    NOT NULL,
            owner_name     TEXT    NOT NULL
        )
    """)

    # Step 5 — PlaylistTrack junction table  (references both Playlist and Track)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS PlaylistTrack (
            playlist_id  INTEGER NOT NULL REFERENCES Playlist(playlist_id),
            track_id     INTEGER NOT NULL REFERENCES Track(track_id),
            position     INTEGER NOT NULL,
            PRIMARY KEY (playlist_id, track_id)
        )
    """)

    conn.commit()
    print("build_database: schema created successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# PART 2 — Seed data
# ─────────────────────────────────────────────────────────────────────────────

def seed_database(conn):
    """Populate all four tables with realistic music data.

    Requirements (all graded):
      - Use conn.executemany() for every table — no individual execute() inserts.
      - Use INSERT OR IGNORE so this function can be called more than once
        without raising IntegrityError on duplicate primary keys.
      - Insert at minimum:
            6  artists
            18 tracks     (each referencing a valid artist_id)
            4  playlists
            20 PlaylistTrack assignments
      - At least one artist must have three or more tracks assigned to playlists.
      - Call conn.commit() after all inserts.

    Parameters
    ----------
    conn : sqlite3.Connection
        An open, schema-ready SQLite connection.

    Returns
    -------
    None
    """

    # ── Artists ──────────────────────────────────────────────────────────────
    # Columns: artist_id, name, genre, origin_city
    # Theme: classic and modern hip-hop artists

    artists = [
        # (artist_id, name, genre, origin_city)
        (1, "Kendrick Lamar",      "Hip-Hop",                "Compton"),
        (2, "J. Cole",             "Hip-Hop",                "Fayetteville"),
        (3, "Drake",               "Hip-Hop/R&B",            "Toronto"),
        (4, "Tyler the Creator",   "Alternative Hip-Hop",    "Los Angeles"),
        (5, "Nas",                 "East Coast Hip-Hop",     "Queens"),
        (6, "Lauryn Hill",         "Neo-Soul/Hip-Hop",       "Newark"),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO Artist VALUES (?, ?, ?, ?)",
        artists
    )

    # ── Tracks ───────────────────────────────────────────────────────────────
    # Columns: track_id, title, duration_seconds, artist_id
    # Kendrick Lamar (artist_id=1) has 5 tracks — satisfies the >=3 requirement

    tracks = [
        # (track_id, title, duration_seconds, artist_id)
        # Kendrick Lamar — 5 tracks
        (1,  "HUMBLE.",                177, 1),
        (2,  "DNA.",                   185, 1),
        (3,  "Alright",               219, 1),
        (4,  "Money Trees",            386, 1),
        (5,  "Swimming Pools",         313, 1),
        # J. Cole — 4 tracks
        (6,  "No Role Modelz",         293, 2),
        (7,  "Love Yourz",             233, 2),
        (8,  "Middle Child",           218, 2),
        (9,  "Power Trip",             268, 2),
        # Drake — 3 tracks
        (10, "God's Plan",             198, 3),
        (11, "Hotline Bling",          267, 3),
        (12, "One Dance",              174, 3),
        # Tyler the Creator — 3 tracks
        (13, "See You Again",          239, 4),
        (14, "EARFQUAKE",              191, 4),
        (15, "NEW MAGIC WAND",         197, 4),
        # Nas — 3 tracks
        (16, "N.Y. State of Mind",     298, 5),
        (17, "If I Ruled the World",   280, 5),
        (18, "One Love",               355, 5),
        # Lauryn Hill — 2 tracks
        (19, "Ex-Factor",              307, 6),
        (20, "Doo Wop (That Thing)",   235, 6),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO Track VALUES (?, ?, ?, ?)",
        tracks
    )

    # ── Playlists ────────────────────────────────────────────────────────────
    # Columns: playlist_id, playlist_name, owner_name

    playlists = [
        # (playlist_id, playlist_name, owner_name)
        (1, "Late Night Vibes",  "Alex"),
        (2, "Workout Anthems",   "Jordan"),
        (3, "90s Classics",      "Morgan"),
        (4, "Chill Session",     "Riley"),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO Playlist VALUES (?, ?, ?)",
        playlists
    )

    # ── PlaylistTrack ─────────────────────────────────────────────────────────
    # Columns: playlist_id, track_id, position
    # Kendrick Lamar tracks (1-5) appear across multiple playlists

    playlist_tracks = [
        # (playlist_id, track_id, position)
        # Late Night Vibes (playlist_id=1) — 6 tracks
        (1,  4,  1),   # Money Trees        — Kendrick Lamar
        (1,  7,  2),   # Love Yourz         — J. Cole
        (1, 19,  3),   # Ex-Factor          — Lauryn Hill
        (1, 18,  4),   # One Love           — Nas
        (1,  5,  5),   # Swimming Pools     — Kendrick Lamar
        (1, 13,  6),   # See You Again      — Tyler the Creator
        # Workout Anthems (playlist_id=2) — 6 tracks
        (2,  1,  1),   # HUMBLE.            — Kendrick Lamar
        (2,  2,  2),   # DNA.               — Kendrick Lamar
        (2,  6,  3),   # No Role Modelz     — J. Cole
        (2,  8,  4),   # Middle Child       — J. Cole
        (2, 15,  5),   # NEW MAGIC WAND     — Tyler the Creator
        (2, 16,  6),   # N.Y. State of Mind — Nas
        # 90s Classics (playlist_id=3) — 5 tracks
        (3, 16,  1),   # N.Y. State of Mind — Nas
        (3, 17,  2),   # If I Ruled the World — Nas
        (3, 20,  3),   # Doo Wop (That Thing) — Lauryn Hill
        (3, 19,  4),   # Ex-Factor          — Lauryn Hill
        (3, 18,  5),   # One Love           — Nas
        # Chill Session (playlist_id=4) — 6 tracks
        (4,  3,  1),   # Alright            — Kendrick Lamar
        (4,  9,  2),   # Power Trip         — J. Cole
        (4, 10,  3),   # God's Plan         — Drake
        (4, 12,  4),   # One Dance          — Drake
        (4, 14,  5),   # EARFQUAKE          — Tyler the Creator
        (4, 17,  6),   # If I Ruled the World — Nas
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO PlaylistTrack VALUES (?, ?, ?)",
        playlist_tracks
    )

    conn.commit()
    print("seed_database: data inserted successfully.")


# ─────────────────────────────────────────────────────────────────────────────
# PART 3 — Standalone demonstration  (run:  python schema_data.py)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # 3a — Build and seed a RAM-only database
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON;")
    build_database(conn)
    seed_database(conn)

    # ── Quick sanity check ────────────────────────────────────────────────────
    row_counts = {
        "Artist":        conn.execute("SELECT COUNT(*) FROM Artist").fetchone()[0],
        "Track":         conn.execute("SELECT COUNT(*) FROM Track").fetchone()[0],
        "Playlist":      conn.execute("SELECT COUNT(*) FROM Playlist").fetchone()[0],
        "PlaylistTrack": conn.execute("SELECT COUNT(*) FROM PlaylistTrack").fetchone()[0],
    }
    print("\nRow counts after seeding:")
    for table, count in row_counts.items():
        print(f"  {table:<16} {count:>3} rows")

    # ── 3b — IntegrityError demonstration ────────────────────────────────────
    # Attempts to insert a Track row whose artist_id does NOT exist in Artist.
    # artist_id = 9999 was never inserted, so SQLite raises IntegrityError
    # because PRAGMA foreign_keys = ON is active.
    print("\nIntegrityError demonstration:")
    try:
        conn.execute("INSERT INTO Track VALUES (999, 'Ghost Track', 210, 9999)")
        print("  Insert succeeded — did you enable PRAGMA foreign_keys = ON?")
    except sqlite3.IntegrityError as e:
        print(f"  IntegrityError caught: {e}")
        print("  --> artist_id 9999 does not exist in Artist; referential integrity violated.")
        print("  This error confirms that foreign key enforcement is active.")

    # ── 3c — Persist the RAM database to disk with .backup() ─────────────────
    # Opens a connection to music.db and backs up the in-memory database to it.
    print("\nPersisting database to music.db ...")
    DB_PATH = "music.db"
    target_conn = sqlite3.connect(DB_PATH)
    conn.backup(target_conn)
    target_conn.close()
    conn.close()
    print(f"  Backup complete.  File size: {os.path.getsize(DB_PATH):,} bytes")
    print(f"  Reopen with:  sqlite3.connect('{DB_PATH}')")
