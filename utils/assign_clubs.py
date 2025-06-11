import os
import sys
import re
import logging

# Fügt das Hauptverzeichnis zum Suchpfad hinzu, damit wir db_queries_refactored importieren können
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db_queries_refactored as db_queries

# --- Logging Konfiguration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_base_club_name(team_name: str) -> str:
    """
    Entfernt eine am Ende stehende Zahl oder römische Ziffer,
    um einen einheitlichen Vereinsnamen zu erhalten.
    Beispiel: "SG Handball Steinfurt 2" -> "SG Handball Steinfurt"
             "Teamname III" -> "Teamname"
    """
    if not team_name:
        return ""
    
    # Dieser Pattern sucht nach:
    # \s+      - einem oder mehreren Leerzeichen
    # (        - Beginn einer Gruppe
    #  \d+     - eine oder mehrere Ziffern (für 1, 2, 3...)
    #  |       - ODER
    #  [IVX]+  - einer oder mehreren römischen Ziffern (für I, II, III, IV...)
    # )        - Ende der Gruppe
    # $        - am Ende des Strings
    pattern = r'\s+(\d+|[IVX]+)$'
    
    base_name = re.sub(pattern, '', team_name, flags=re.IGNORECASE)
    return base_name.strip()


def assign_clubs_to_teams():
    """
    Hauptfunktion: Geht alle Teams durch, weist sie einem Verein zu
    und aktualisiert die Datenbank.
    """
    conn = None
    try:
        conn = db_queries.get_db_connection()
        if not conn:
            logger.error("Keine Datenbankverbindung möglich. Skript wird beendet.")
            return

        cursor = conn.cursor()

        # 1. Alle Teams aus der Datenbank holen
        logger.info("Lade alle Teams aus der Datenbank...")
        teams_df = db_queries.fetch_all_teams_simple()
        if teams_df.empty:
            logger.warning("Keine Teams in der Datenbank gefunden.")
            return

        logger.info(f"{len(teams_df)} Teams gefunden. Starte die Vereinszuordnung...")
        processed_count = 0

        # 2. Durch jedes Team iterieren
        for index, team_row in teams_df.iterrows():
            team_id = team_row["Team_ID"]
            team_name = team_row["Name"]

            # 3. Basis-Vereinsnamen ermitteln
            base_club_name = get_base_club_name(team_name)

            if not base_club_name:
                logger.warning(f"Konnte keinen Basis-Namen für Team '{team_name}' (ID: {team_id}) ermitteln. Überspringe.")
                continue

            # 4. Prüfen, ob der Verein schon existiert, sonst neu anlegen
            cursor.execute('SELECT "Verein_ID" FROM "Vereine" WHERE "Name" = %s', (base_club_name,))
            club_result = cursor.fetchone()

            if club_result:
                club_id = club_result[0]
            else:
                # Verein neu anlegen und ID zurückgeben
                cursor.execute('INSERT INTO "Vereine" ("Name") VALUES (%s) RETURNING "Verein_ID"', (base_club_name,))
                club_id = cursor.fetchone()[0]
                logger.info(f"Neuer Verein '{base_club_name}' mit ID {club_id} angelegt.")

            # 5. Team in der Datenbank aktualisieren und die Verein_ID zuweisen
            cursor.execute('UPDATE "Teams" SET "Verein_ID" = %s WHERE "Team_ID" = %s', (club_id, team_id))

            processed_count += 1
            if processed_count % 50 == 0:
                logger.info(f"{processed_count}/{len(teams_df)} Teams verarbeitet...")


        # 6. Änderungen in der Datenbank speichern
        conn.commit()
        logger.info(f"Zuordnung erfolgreich abgeschlossen! {processed_count} Teams wurden einem Verein zugeordnet.")

    except Exception as e:
        logger.error(f"Ein Fehler ist aufgetreten: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()
            logger.info("Datenbankverbindung geschlossen.")

# --- Skript ausführen ---
if __name__ == "__main__":
    assign_clubs_to_teams()