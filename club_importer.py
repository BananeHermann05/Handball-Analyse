import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
import logging
from typing import List, Set
from urllib.parse import urljoin
from utils.ui import display_dataframe_with_title # Import für eine einheitliche UI

# Annahme: Die folgende Funktion existiert bereits in fetch_html_game_ids.py
# Wir importieren sie, um sie wiederzuverwenden.
from fetch_html_game_ids import fetch_game_ids_from_html_page, REQUEST_HEADERS

logger = logging.getLogger(__name__)

def _fetch_league_urls_from_club_page(club_url: str) -> List[str]:
    """
    Extrahiert alle Links zu den einzelnen Liga-Spielplänen von einer Vereinsseite.
    Interne Hilfsfunktion.
    """
    league_urls: Set[str] = set()
    try:
        logger.info(f"Rufe Vereinsseiten-HTML von {club_url} ab...")
        response = requests.get(club_url, headers=REQUEST_HEADERS, timeout=20)
        response.raise_for_status()

        logger.info(f"HTML-Inhalt erfolgreich von {club_url} abgerufen. Parse mit BeautifulSoup...")
        soup = BeautifulSoup(response.text, 'html.parser')

        link_pattern = re.compile(r"/ligen/handball4all\..*/spielplan")
        all_links = soup.find_all('a', href=link_pattern)

        if not all_links:
            logger.warning(f"Keine Liga-Spielplan-Links auf der Vereinsseite {club_url} gefunden.")
            return []

        for link in all_links:
            href = link.get('href')
            if href:
                base_url = "https://www.handball.net/"
                full_url = urljoin(base_url, href)
                league_urls.add(full_url)

        logger.info(f"{len(league_urls)} eindeutige Liga-URLs auf {club_url} gefunden.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Fehler beim Abrufen der Vereins-URL {club_url}: {e}")
        # Gib den Fehler an die Streamlit-Oberfläche weiter
        st.error(f"Netzwerkfehler beim Abrufen der Vereinsseite: {e}")
    except Exception as e:
        logger.error(f"Ein unerwarteter Fehler ist beim Verarbeiten der Vereinsseite {club_url} aufgetreten: {e}", exc_info=True)
        st.error(f"Ein allgemeiner Fehler ist aufgetreten: {e}")

    return sorted(list(league_urls))

def get_all_game_ids_for_club(club_url: str, id_prefix: str) -> List[str]:
    """
    Orchestriert den gesamten Prozess des Sammelns von Spiel-IDs für einen Verein.
    """
    status_container = st.container()
    
    league_urls = _fetch_league_urls_from_club_page(club_url)
    if not league_urls:
        status_container.warning("Keine Liga-URLs auf der Vereinsseite gefunden. Der Vorgang wird abgebrochen.")
        return []

    status_container.success(f"{len(league_urls)} Ligen für den Verein gefunden. Sammle nun alle Spiel-IDs...")
    
    all_game_ids: Set[str] = set()
    progress_bar = st.progress(0.0, text="Sammle Spiel-IDs aus den Ligen...")

    for i, league_url in enumerate(league_urls):
        # Ersetze 'spielplan' mit 'liga-spielplan' für die komplette Liste
        full_league_url = league_url.replace("/spielplan", "/liga-spielplan")
        
        # Verwende ein Expander, um die Ausgabe übersichtlich zu halten
        with status_container.expander(f"Verarbeite Liga {i+1}/{len(league_urls)}: {full_league_url}", expanded=False):
            game_ids_of_league = fetch_game_ids_from_html_page(full_league_url, id_prefix)
            if game_ids_of_league:
                st.write(f"  -> {len(game_ids_of_league)} Spiel-IDs gefunden.")
                all_game_ids.update(game_ids_of_league)
            else:
                st.write("  -> Keine Spiel-IDs in dieser Liga gefunden.")
        
        progress_bar.progress((i + 1) / len(league_urls), text=f"Liga {i+1}/{len(league_urls)} verarbeitet")
    
    final_ids = sorted(list(all_game_ids))
    if final_ids:
        st.success(f"Insgesamt {len(final_ids)} eindeutige Spiel-IDs für den Import vorbereitet.")
    else:
        st.error("Konnte keine einzigen Spiel-IDs für den gesamten Verein extrahieren.")

    return final_ids