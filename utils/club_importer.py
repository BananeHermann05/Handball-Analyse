import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import logging
from typing import List, Set
from urllib.parse import urljoin

# Annahme: Die folgende Funktion existiert bereits in fetch_html_game_ids.py
# Wir importieren sie, um sie wiederzuverwenden.
from fetch_html_game_ids import fetch_game_ids_from_html_page, REQUEST_HEADERS

logger = logging.getLogger(__name__)

def _fetch_league_urls_from_club_page(club_url: str) -> List[str]:
    """
    Extrahiert alle Links zu den einzelnen Team-Spielplänen von einer Vereinsseite.
    Interne Hilfsfunktion.
    """
    team_urls: Set[str] = set()
    try:
        logger.info(f"Rufe Vereinsseiten-HTML von {club_url} ab...")
        response = requests.get(club_url, headers=REQUEST_HEADERS, timeout=20)
        response.raise_for_status()

        logger.info(f"HTML-Inhalt erfolgreich von {club_url} abgerufen. Parse mit BeautifulSoup...")
        soup = BeautifulSoup(response.text, 'html.parser')

        # === KORREKTUR HIER ===
        # Suche nun nach dem korrekten Muster /mannschaften/ anstatt /ligen/
        link_pattern = re.compile(r"/mannschaften/handball4all\..*/spielplan")
        all_links = soup.find_all('a', href=link_pattern)

        if not all_links:
            logger.warning(f"Keine Team-Spielplan-Links auf der Vereinsseite {club_url} gefunden.")
            return []

        for link in all_links:
            href = link.get('href')
            if href:
                base_url = "https://www.handball.net/"
                full_url = urljoin(base_url, href)
                team_urls.add(full_url)

        logger.info(f"{len(team_urls)} eindeutige Team-URLs auf {club_url} gefunden.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Fehler beim Abrufen der Vereins-URL {club_url}: {e}")
        st.error(f"Netzwerkfehler beim Abrufen der Vereinsseite: {e}")
    except Exception as e:
        logger.error(f"Ein unerwarteter Fehler ist beim Verarbeiten der Vereinsseite {club_url} aufgetreten: {e}", exc_info=True)
        st.error(f"Ein allgemeiner Fehler ist aufgetreten: {e}")

    return sorted(list(team_urls))

def get_all_game_ids_for_club(club_url: str, id_prefix: str) -> List[str]:
    """
    Orchestriert den gesamten Prozess des Sammelns von Spiel-IDs für einen Verein
    und gibt den Fortschritt in der Streamlit-Oberfläche aus.
    """
    status_container = st.container()
    
    # Ruft die Team-URLs ab (z.B. .../mannschaften/.../spielplan)
    team_urls = _fetch_league_urls_from_club_page(club_url)
    if not team_urls:
        status_container.warning("Keine Team-URLs auf der Vereinsseite gefunden. Der Vorgang wird abgebrochen.")
        return []

    status_container.success(f"{len(team_urls)} Teams/Ligen für den Verein gefunden. Sammle nun alle Spiel-IDs...")
    
    all_game_ids: Set[str] = set()
    progress_bar = st.progress(0.0, text="Sammle Spiel-IDs aus den Ligen...")

    for i, team_url in enumerate(team_urls):
        # Ersetze 'spielplan' mit 'liga-spielplan', um die komplette Liste zu erhalten
        full_league_schedule_url = team_url.replace("/spielplan", "/liga-spielplan")
        
        with status_container.expander(f"Verarbeite URL {i+1}/{len(team_urls)}: {full_league_schedule_url}", expanded=False):
            game_ids_of_league = fetch_game_ids_from_html_page(full_league_schedule_url, id_prefix)
            if game_ids_of_league:
                st.write(f"  -> {len(game_ids_of_league)} Spiel-IDs gefunden.")
                all_game_ids.update(game_ids_of_league)
            else:
                st.write("  -> Keine Spiel-IDs unter dieser URL gefunden.")
        
        progress_bar.progress((i + 1) / len(team_urls), text=f"URL {i+1}/{len(team_urls)} verarbeitet")
    
    final_ids = sorted(list(all_game_ids))
    if final_ids:
        st.success(f"Insgesamt {len(final_ids)} eindeutige Spiel-IDs für den Import vorbereitet.")
    else:
        st.error("Konnte keine einzigen Spiel-IDs für den gesamten Verein extrahieren.")

    return final_ids