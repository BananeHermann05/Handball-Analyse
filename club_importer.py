import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import logging
from typing import List, Set, Tuple
from urllib.parse import urljoin
import db_queries_refactored as db_queries # Import für DB-Zugriff

# Wiederverwendung der bestehenden Funktionen
from fetch_html_game_ids import fetch_game_ids_from_html_page, REQUEST_HEADERS

logger = logging.getLogger(__name__)

def _get_or_create_verein(cursor, club_url: str) -> Tuple[int, str]:
    """
    Extrahiert die Vereins-ID aus der URL, legt den Verein in der DB an (falls nicht vorhanden)
    und gibt die interne DB-ID zurück.
    """
    # 1. Extrahiere Vereins-API-ID und den Namen von der Seite
    verein_api_id_match = re.search(r"/vereine/([^/]+)", club_url)
    if not verein_api_id_match:
        raise ValueError("Die Vereins-URL hat nicht das erwartete Format '.../vereine/...'")
    
    verein_api_id = verein_api_id_match.group(1)

    response = requests.get(club_url, headers=REQUEST_HEADERS, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Versuche den Vereinsnamen aus dem <title> oder einem <h1> Tag zu extrahieren
    verein_name = soup.find('h1').get_text(strip=True) if soup.find('h1') else verein_api_id
    
    # 2. Füge Verein in DB ein oder finde ihn und gib die interne ID zurück
    cursor.execute(
        'INSERT INTO "Vereine" ("Name") VALUES (%s) ON CONFLICT ("Name") DO UPDATE SET "Name" = EXCLUDED."Name" RETURNING "Verein_ID"',
        (verein_name,)
    )
    db_verein_id = cursor.fetchone()[0]
    return db_verein_id, verein_name


def _link_teams_to_verein(cursor, club_url: str, db_verein_id: int):
    """Sucht alle Teams auf der Vereinsseite und verknüpft sie in der DB mit der Vereins-ID."""
    soup = BeautifulSoup(requests.get(club_url, headers=REQUEST_HEADERS).text, 'html.parser')
    team_link_pattern = re.compile(r"/mannschaften/(handball4all\.[^/]+)")
    all_team_links = soup.find_all('a', href=team_link_pattern)
    
    updated_teams = 0
    for link in all_team_links:
        match = team_link_pattern.search(link.get('href'))
        if match:
            team_api_id = match.group(1)
            cursor.execute(
                'UPDATE "Teams" SET "Verein_ID" = %s WHERE "Team_ID" = %s',
                (db_verein_id, team_api_id)
            )
            updated_teams += cursor.rowcount # Zählt, ob eine Zeile aktualisiert wurde
    
    logger.info(f"{updated_teams} Teams wurden dem Verein mit ID {db_verein_id} zugeordnet.")
    st.info(f"{len(all_team_links)} Teams auf der Seite gefunden und {updated_teams} davon in der DB verknüpft.")


def get_all_game_ids_for_club(club_url: str, id_prefix: str) -> List[str]:
    """
    Hauptfunktion: Orchestriert das Anlegen/Verknüpfen des Vereins und das Sammeln aller Spiel-IDs.
    """
    conn = None
    try:
        conn = db_queries.get_db_connection()
        if not conn:
            st.error("Keine Datenbankverbindung möglich.")
            return []
        
        with conn.cursor() as cursor:
            # Schritt A: Verein anlegen/finden und verknüpfen
            with st.spinner("Analysiere Verein und verknüpfe Teams..."):
                db_verein_id, verein_name = _get_or_create_verein(cursor, club_url)
                st.success(f"Verein '{verein_name}' (ID: {db_verein_id}) in Datenbank registriert.")
                _link_teams_to_verein(cursor, club_url, db_verein_id)
            conn.commit()

        # Schritt B: Alle Spiel-IDs für den Import sammeln (wie bisher, aber mit sauberer Grundlage)
        # Die Logik zum Sammeln der Spiel-IDs bleibt gleich
        soup = BeautifulSoup(requests.get(club_url, headers=REQUEST_HEADERS).text, 'html.parser')
        link_pattern = re.compile(r"/ligen/handball4all\..*/spielplan")
        all_links = soup.find_all('a', href=link_pattern)
        
        all_game_ids: Set[str] = set()
        progress_bar = st.progress(0.0, text="Sammle Spiel-IDs aus den Ligen...")

        for i, link_tag in enumerate(all_links):
            href = link_tag.get('href')
            if href:
                full_url = urljoin("https://www.handball.net/", href)
                full_league_schedule_url = full_url.replace("/spielplan", "/liga-spielplan")
                game_ids_of_league = fetch_game_ids_from_html_page(full_league_schedule_url, id_prefix)
                all_game_ids.update(game_ids_of_league)
            progress_bar.progress((i + 1) / len(all_links), text=f"Liga {i+1}/{len(all_links)} verarbeitet")
        
        final_ids = sorted(list(all_game_ids))
        if final_ids:
            st.success(f"Insgesamt {len(final_ids)} eindeutige Spiel-IDs für den Import vorbereitet.")
        else:
            st.warning("Keine Spiele zum Importieren für diesen Verein gefunden.")
        return final_ids

    except Exception as e:
        if conn: conn.rollback()
        st.error(f"Ein Fehler ist aufgetreten: {e}")
        logger.error("Fehler im Vereins-Import-Workflow", exc_info=True)
        return []
    finally:
        if conn: conn.close()