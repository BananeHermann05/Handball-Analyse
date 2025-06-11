import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import logging
from typing import List, Set, Tuple
from urllib.parse import urljoin
import db_queries_refactored as db_queries

from fetch_html_game_ids import fetch_game_ids_from_html_page, REQUEST_HEADERS

logger = logging.getLogger(__name__)

def _get_or_create_verein(cursor, club_url: str) -> Tuple[int, str]:
    """
    Extrahiert die Vereins-API-ID aus der URL, legt den Verein in der DB an (falls nicht vorhanden)
    und gibt die interne DB-ID zurück.
    """
    match = re.search(r"/vereine/([^/]+)", club_url)
    if not match:
        raise ValueError("Die Vereins-URL muss das Format '.../vereine/API_ID' haben.")
    
    response = requests.get(club_url, headers=REQUEST_HEADERS, timeout=10)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')
    
    verein_name = soup.find('h1').get_text(strip=True) if soup.find('h1') else 'Unbekannter Verein'
    
    # Nutze die eindeutige Api_ID als Konflikt-Ziel
    # ANNAHME: Die Spalte 'Api_ID' existiert in der Tabelle 'Vereine'
    # cursor.execute(
    #     """
    #     INSERT INTO "Vereine" ("Api_ID", "Name") VALUES (%s, %s)
    #     ON CONFLICT ("Api_ID") DO UPDATE SET "Name" = EXCLUDED."Name"
    #     RETURNING "Verein_ID"
    #     """,
    #     (match.group(1), verein_name)
    # )
    # db_verein_id = cursor.fetchone()[0]
    # return db_verein_id, verein_name
    # Fallback zur Namens-basierten Logik, da Api_ID nicht im Schema ist
    cursor.execute(
        """
        INSERT INTO "Vereine" ("Name") VALUES (%s)
        ON CONFLICT ("Name") DO UPDATE SET "Name" = EXCLUDED."Name"
        RETURNING "Verein_ID"
        """,
        (verein_name,)
    )
    db_verein_id = cursor.fetchone()[0]
    return db_verein_id, verein_name


def _link_teams_to_verein(cursor, club_url: str, db_verein_id: int):
    """Sucht alle Teams auf der Vereinsseite und verknüpft sie in der DB mit der Vereins-ID."""
    soup = BeautifulSoup(requests.get(club_url, headers=REQUEST_HEADERS).text, 'html.parser')
    team_link_pattern = re.compile(r"/mannschaften/(handball4all\.[^/]+)")
    all_team_links = soup.find_all('a', href=team_link_pattern)
    
    team_api_ids = {match.group(1) for link in all_team_links if (match := team_link_pattern.search(link.get('href')))}
    
    if team_api_ids:
        params = (db_verein_id, tuple(team_api_ids))
        cursor.execute(
            'UPDATE "Teams" SET "Verein_ID" = %s WHERE "Team_ID" IN %s',
            params
        )
        st.info(f"{len(team_api_ids)} zugehörige Teams gefunden und mit dem Verein verknüpft.")
    else:
        st.warning("Keine Teams auf der Vereinsseite gefunden zum Verknüpfen.")

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
        
        # Dieser Teil ist nicht mehr nötig, da die Logik in analyse_game_json verlagert wurde
        # with conn.cursor() as cursor:
        #     with st.spinner("Analysiere Verein und verknüpfe Teams..."):
        #         db_verein_id, verein_name = _get_or_create_verein(cursor, club_url)
        #         st.success(f"Verein '{verein_name}' (ID: {db_verein_id}) in Datenbank registriert/gefunden.")
        #         _link_teams_to_verein(cursor, club_url, db_verein_id)
        #     conn.commit()

        st.info("Suche nach Mannschafts-Links, um Spielpläne zu finden...")
        response = requests.get(club_url, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        team_schedule_pattern = re.compile(r"/mannschaften/handball4all\..*/spielplan")
        all_team_links = soup.find_all('a', href=team_schedule_pattern)
        
        if not all_team_links:
            st.warning("Keine Mannschafts-Links auf der Vereinsseite gefunden. Es können keine Spiele importiert werden.")
            return []

        all_game_ids: Set[str] = set()
        progress_bar = st.progress(0.0, text="Sammle Spiel-IDs...")

        for i, link_tag in enumerate(all_team_links):
            href = link_tag.get('href')
            if href:
                full_url = urljoin("https://www.handball.net/", href)
                full_league_url = full_url.replace("/spielplan", "/liga-spielplan")
                game_ids = fetch_game_ids_from_html_page(full_league_url, id_prefix)
                all_game_ids.update(game_ids)
            progress_bar.progress((i + 1) / len(all_team_links), text=f"Team {i+1}/{len(all_team_links)} verarbeitet")
        
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