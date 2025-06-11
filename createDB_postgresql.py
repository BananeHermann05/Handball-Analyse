import psycopg2
import os
import logging
from typing import Optional
from dotenv import load_dotenv #

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s') #
logger = logging.getLogger(__name__)

# --- Lade Umgebungsvariablen (aus .env oder database.env) ---
# Diese Methode wird beibehalten, da createDB meist lokal ausgeführt wird.
dotenv_path_specific = os.path.join(os.path.dirname(__file__), 'database.env') #
dotenv_path_default = os.path.join(os.getcwd(), '.env') #
loaded_env_file = None

if os.path.exists(dotenv_path_specific): #
    if load_dotenv(dotenv_path_specific): #
        loaded_env_file = dotenv_path_specific #
elif os.path.exists(dotenv_path_default): #
    if load_dotenv(dotenv_path_default): #
        loaded_env_file = dotenv_path_default #

if loaded_env_file: #
    logger.info(f"Umgebungsvariablen aus {loaded_env_file} geladen für createDB_postgresql.") #
else:
    logger.warning(f"Keine .env oder database.env Datei gefunden. Umgebungsvariablen müssen anderweitig gesetzt sein für createDB_postgresql.") #


# --- Datenbank-Verbindungsdetails (aus Umgebungsvariablen laden) ---
DB_NAME = os.environ.get("PG_DB_NAME") #
DB_USER = os.environ.get("PG_DB_USER") #
DB_PASSWORD = os.environ.get("PG_DB_PASSWORD") #
DB_HOST = os.environ.get("PG_DB_HOST") #
DB_PORT = os.environ.get("PG_DB_PORT", "5432") #


SQL_SCHEMA_POSTGRESQL: str = """
-- SQL-Anweisungen zur Erstellung der HandballAnalyseDB für PostgreSQL
-- Alle Tabellen- und Spaltennamen in Anführungszeichen, um Groß-/Kleinschreibung beizubehalten.

CREATE TABLE IF NOT EXISTS "Ligen" (
    "Liga_ID" TEXT PRIMARY KEY, "Name" TEXT NOT NULL, "Akronym" TEXT,
    "Saison" TEXT NOT NULL, "Altersgruppe" TEXT, "Typ" TEXT
);
CREATE TABLE IF NOT EXISTS "Teams" (
    "Team_ID" TEXT PRIMARY KEY, "Name" TEXT NOT NULL, "Akronym" TEXT, "Logo_URL" TEXT
);
CREATE TABLE IF NOT EXISTS "Spieler" (
    "Spieler_ID" TEXT PRIMARY KEY, "Vorname" TEXT, "Nachname" TEXT,
    "Ist_NN" INTEGER NOT NULL DEFAULT 0, "Ist_Offizieller" INTEGER NOT NULL DEFAULT 0,
    "Position" TEXT, "Spitzname" TEXT, "Bild_URL" TEXT
);
CREATE TABLE IF NOT EXISTS "Hallen" (
    "Hallen_ID" TEXT PRIMARY KEY, "Name" TEXT NOT NULL, "Stadt" TEXT, "Hallen_Nummer" TEXT
);
CREATE TABLE IF NOT EXISTS "Vereine" (
    "Verein_ID" SERIAL PRIMARY KEY, -- Eine einfache, fortlaufende ID
    "Name" TEXT NOT NULL UNIQUE,     -- Der offizielle, aggregierte Name des Vereins
    "Logo_URL" TEXT -- Platzhalter für ein Vereinslogo
);
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_attribute WHERE attrelid = '"Teams"'::regclass AND attname = 'Verein_ID') THEN
        ALTER TABLE "Teams" ADD COLUMN "Verein_ID" INTEGER REFERENCES "Vereine"("Verein_ID") ON DELETE SET NULL;
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS "Spiele" (
    "Spiel_ID" TEXT PRIMARY KEY,
    "Liga_ID" TEXT NOT NULL REFERENCES "Ligen"("Liga_ID") ON DELETE CASCADE,
    "Phase_ID" TEXT, "Hallen_ID" TEXT REFERENCES "Hallen"("Hallen_ID") ON DELETE SET NULL,
    "Spiel_Nummer" TEXT, "Start_Zeit" BIGINT NOT NULL, 
    "Heim_Team_ID" TEXT NOT NULL REFERENCES "Teams"("Team_ID") ON DELETE CASCADE,
    "Gast_Team_ID" TEXT NOT NULL REFERENCES "Teams"("Team_ID") ON DELETE CASCADE,
    "Tore_Heim" INTEGER, "Tore_Gast" INTEGER, "Tore_Heim_HZ" INTEGER, "Tore_Gast_HZ" INTEGER,
    "Status" TEXT, "PDF_URL" TEXT, "SchiedsrichterInfo" TEXT,
    "Punkte_Heim_Offiziell" INTEGER, "Punkte_Gast_Offiziell" INTEGER
);
CREATE TABLE IF NOT EXISTS "Spiel_Kader_Statistiken" (
    "Kader_Eintrag_ID" SERIAL PRIMARY KEY,
    "Spiel_ID" TEXT NOT NULL REFERENCES "Spiele"("Spiel_ID") ON DELETE CASCADE,
    "Spieler_ID" TEXT NOT NULL REFERENCES "Spieler"("Spieler_ID") ON DELETE CASCADE,
    "Team_ID" TEXT NOT NULL REFERENCES "Teams"("Team_ID") ON DELETE CASCADE,
    "Rueckennummer" INTEGER, "Tore_Gesamt" INTEGER NOT NULL DEFAULT 0,
    "Tore_7m" INTEGER NOT NULL DEFAULT 0, "Fehlwurf_7m" INTEGER NOT NULL DEFAULT 0,
    "Gelbe_Karten" INTEGER NOT NULL DEFAULT 0, "Rote_Karten" INTEGER NOT NULL DEFAULT 0,
    "Blaue_Karten" INTEGER NOT NULL DEFAULT 0, "Zwei_Minuten_Strafen" INTEGER NOT NULL DEFAULT 0,
    UNIQUE ("Spiel_ID", "Spieler_ID")
);
CREATE TABLE IF NOT EXISTS "Ereignisse" (
    "Ereignis_Auto_ID" SERIAL PRIMARY KEY, "H4A_Ereignis_ID" INTEGER NOT NULL,
    "Spiel_ID" TEXT NOT NULL REFERENCES "Spiele"("Spiel_ID") ON DELETE CASCADE,
    "Zeitstempel" BIGINT NOT NULL, "Spiel_Minute" TEXT NOT NULL, "Typ" TEXT NOT NULL,
    "Score_Heim" INTEGER, "Score_Gast" INTEGER, "Team_Seite" TEXT, "Nachricht" TEXT,
    "Referenz_Spieler_ID" TEXT REFERENCES "Spieler"("Spieler_ID") ON DELETE SET NULL,
    UNIQUE("Spiel_ID", "H4A_Ereignis_ID")
);
CREATE INDEX IF NOT EXISTS idx_spiele_liga ON "Spiele" ("Liga_ID");
CREATE INDEX IF NOT EXISTS idx_spiele_datum ON "Spiele" ("Start_Zeit");
CREATE INDEX IF NOT EXISTS idx_kader_spieler ON "Spiel_Kader_Statistiken" ("Spieler_ID");
CREATE INDEX IF NOT EXISTS idx_kader_spiel ON "Spiel_Kader_Statistiken" ("Spiel_ID");
CREATE INDEX IF NOT EXISTS idx_ereignisse_spiel ON "Ereignisse" ("Spiel_ID");
CREATE INDEX IF NOT EXISTS idx_ereignisse_typ ON "Ereignisse" ("Typ");
CREATE INDEX IF NOT EXISTS idx_ereignisse_spieler ON "Ereignisse" ("Referenz_Spieler_ID");
"""

def get_postgresql_connection() -> Optional[psycopg2.extensions.connection]:
    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT]):
        logger.error("PG-Verbindungsinformationen unvollständig.")
        return None
    try:
        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        logger.info(f"Verbunden mit PostgreSQL DB '{DB_NAME}' auf {DB_HOST}.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Fehler bei PG-Verbindung: {e}")
        return None

def erstelle_postgres_datenbank_schema() -> None:
    conn: Optional[psycopg2.extensions.connection] = None
    cursor: Optional[psycopg2.extensions.cursor] = None # type: ignore
    try:
        conn = get_postgresql_connection()
        if conn is None: 
            logger.error("Konnte keine Datenbankverbindung herstellen. Schemaerstellung abgebrochen.")
            return
        cursor = conn.cursor()
        logger.info("Führe PostgreSQL-Schema aus...")
        cursor.execute(SQL_SCHEMA_POSTGRESQL)
        conn.commit()
        logger.info("PostgreSQL-Schema erfolgreich erstellt/aktualisiert.")
    except psycopg2.Error as e:
        logger.error(f"PG-Fehler bei Schemaerstellung: {e}")
        if conn: conn.rollback()
    except Exception as e_gen:
        logger.error(f"Allgemeiner Fehler bei Schemaerstellung: {e_gen}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        logger.info("PG-Verbindung für Schemaerstellung geschlossen.")

if __name__ == "__main__":
    # Stelle sicher, dass die Variablen korrekt geladen sind für direkte Ausführung
    DB_NAME = os.environ.get("PG_DB_NAME")
    DB_USER = os.environ.get("PG_DB_USER")
    DB_PASSWORD = os.environ.get("PG_DB_PASSWORD")
    DB_HOST = os.environ.get("PG_DB_HOST")
    DB_PORT = os.environ.get("PG_DB_PORT", "5432")

    if not all([DB_NAME, DB_USER, DB_PASSWORD, DB_HOST]):
        print("FEHLER: Bitte setze die Umgebungsvariablen: PG_DB_NAME, PG_DB_USER, PG_DB_PASSWORD, PG_DB_HOST in deiner .env oder database.env Datei.")
    else:
        logger.info("Starte die Erstellung des PostgreSQL-Datenbankschemas...")
        # erstelle_postgres_datenbank_schema() # Ausführen der Funktion