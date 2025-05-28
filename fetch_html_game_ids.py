import requests
from bs4 import BeautifulSoup
import re
import logging
from typing import List, Set, Dict, Optional

# --- Logging Configuration ---
# BasicConfig wird hier nur aufgerufen, wenn das Skript direkt ausgeführt wird.
# Beim Import sollte die Konfiguration vom importierenden Skript übernommen werden.
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants ---
REQUEST_HEADERS: Dict[str, str] = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def fetch_game_ids_from_html_page(url: str, id_prefix: str) -> List[str]:
    """
    Extrahiert Spiel-IDs aus dem HTML-Quelltext einer Webseite,
    indem nach div-Elementen mit einer spezifischen ID-Struktur gesucht wird.

    Args:
        url (str): Die URL der Webseite, von der die Spiel-IDs extrahiert werden sollen.
        id_prefix (str): Das Präfix, mit dem die gesuchten IDs beginnen sollen 
                         (z.B. "handball4all.westfalen.").

    Returns:
        List[str]: Eine sortierte Liste von eindeutigen, numerischen Spiel-IDs.
                   Gibt eine leere Liste zurück, wenn keine IDs gefunden oder Fehler aufgetreten sind.
    """
    game_ids: Set[str] = set()
    try:
        logger.info(f"Rufe HTML von {url} mit ID-Präfix '{id_prefix}' ab...")
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=20) # Timeout erhöht
        response.raise_for_status()
        
        logger.info(f"HTML-Inhalt erfolgreich von {url} abgerufen. Parse mit BeautifulSoup...")
        soup = BeautifulSoup(response.text, 'html.parser')
        
        id_pattern_regex = re.compile(r"^" + re.escape(id_prefix) + r"(\d+)$")
        
        all_divs_with_id = soup.find_all('div', id=True) 
        
        if not all_divs_with_id:
            logger.warning(f"Keine div-Elemente mit einem ID-Attribut auf {url} gefunden.")
            return []

        logger.debug(f"{len(all_divs_with_id)} divs mit IDs auf {url} gefunden. Filtere nach Muster '{id_prefix}<Zahl>'...")
        
        found_count = 0
        for div in all_divs_with_id:
            current_id: Optional[str] = div.get('id')
            if current_id:
                match = id_pattern_regex.match(current_id)
                if match:
                    numeric_part: str = match.group(1)
                    if numeric_part.isdigit():
                        game_ids.add(numeric_part)
                        found_count +=1
                    else:
                        logger.debug(f"Numerischer Teil '{numeric_part}' aus ID '{current_id}' auf {url} ist keine reine Zahl.")
        
        if found_count == 0:
            logger.warning(f"Keine IDs passend zum Muster '{id_prefix}<Zahl>' in den div-Elementen auf {url} entdeckt.")
        else:
            logger.info(f"{found_count} passende Spiel-IDs von {url} extrahiert.")

    except requests.exceptions.Timeout:
        logger.error(f"Timeout beim Abrufen der URL {url}.")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP-Fehler {e.response.status_code} beim Abrufen von {url}: {e.response.text[:200]}...")
    except requests.exceptions.RequestException as e:
        logger.error(f"Allgemeiner Fehler beim Abrufen der URL {url}: {e}")
    except Exception as e:
        logger.error(f"Ein unerwarteter Fehler ist beim Verarbeiten von {url} aufgetreten: {e}", exc_info=True)
        
    return sorted(list(game_ids))

if __name__ == "__main__":
    # Testausführung
    test_target_url = "https://www.handball.net/ligen/handball4all.westfalen.f-kk-1_wfms/spielplan?season=2024"
    test_id_prefix = "handball4all.westfalen." # Korrektes Präfix für die Test-URL
    
    logger.info(f"Starte Test-Extraktion der Spiel-IDs von: {test_target_url} mit Präfix {test_id_prefix}")
    extracted_ids = fetch_game_ids_from_html_page(test_target_url, test_id_prefix)
    
    if extracted_ids:
        logger.info(f"--- Insgesamt {len(extracted_ids)} eindeutige Spiel-IDs gefunden: ---")
        for spiel_id in extracted_ids:
            print(spiel_id) 
    else:
        logger.warning(f"Keine Spiel-IDs konnten von {test_target_url} extrahiert werden.")
        logger.info("Stelle sicher, dass die IDs direkt im HTML-Quelltext der Seite vorhanden sind und das ID_PREFIX korrekt ist.")
        logger.info("Wenn die Inhalte erst durch JavaScript geladen werden, ist dieser Ansatz nicht ausreichend.")

    # This import and call might be better handled by a main script orchestrating the process
    # For now, keeping it as per original structure if this script is run standalone.
    try:
        from analyse_game_json import main as analyse_main # Renamed to avoid conflict
        if extracted_ids:
             analyse_main(extracted_ids)
        else:
            logger.info("Keine IDs zum Analysieren an analyse_game_json.py übergeben.")
    except ImportError:
        logger.error("analyse_game_json.py konnte nicht importiert werden. Stelle sicher, dass es im selben Verzeichnis liegt.")
    except Exception as e:
        logger.error(f"Fehler beim Aufrufen von analyse_game_json.main: {e}")