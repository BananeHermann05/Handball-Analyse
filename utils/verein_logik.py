# In utils/verein_logik.py
import re
import logging

logger = logging.getLogger(__name__)

def assign_club_to_team(cursor, team_id: str, team_name: str):
    """
    Stellt sicher, dass ein Team einem Verein zugeordnet ist.
    Nutzt Namens-Analyse, um den Verein zu erraten, und legt ihn bei Bedarf an.
    """
    if not team_name or not team_id:
        return

    try:
        # 1. Prüfen, ob das Team bereits einem Verein zugeordnet ist
        cursor.execute('SELECT "Verein_ID" FROM "Teams" WHERE "Team_ID" = %s', (team_id,))
        result = cursor.fetchone()
        if result and result[0] is not None:
            return # Bereits erledigt

        # 2. Heuristik: Errate den Vereinsnamen aus dem Mannschaftsnamen
        # Entfernt Zahlen, römische Ziffern und übliche Mannschafts-Endungen
        verein_name_guess = re.sub(
            r'\s+[0-9]+$|\s+[IVX]+$|\s+\(A\)$|\s+II$|\s+III$|\s+IV$|\s+V$|\s+VI$',
            '',
            team_name.strip(),
            flags=re.IGNORECASE
        ).strip()
        
        if not verein_name_guess:
             verein_name_guess = team_name.strip()

        # 3. Verein in der DB anlegen oder finden
        cursor.execute(
            """
            INSERT INTO "Vereine" ("Name") VALUES (%s)
            ON CONFLICT ("Name") DO UPDATE SET "Name" = EXCLUDED."Name"
            RETURNING "Verein_ID"
            """,
            (verein_name_guess,)
        )
        db_verein_id = cursor.fetchone()[0]

        # 4. Das Team mit dem gefundenen/erstellten Verein verknüpfen
        cursor.execute(
            'UPDATE "Teams" SET "Verein_ID" = %s WHERE "Team_ID" = %s',
            (db_verein_id, team_id)
        )
        logger.info(f"Team '{team_name}' wurde dem Verein '{verein_name_guess}' (ID: {db_verein_id}) zugeordnet.")

    except Exception as e:
        logger.error(f"Fehler bei der Zuordnung von Team '{team_name}' zu einem Verein: {e}", exc_info=True)