import json
import re
import logging
from typing import List, Dict, Any, Set, Optional, Union # Added for type hinting

# Assuming analyse_game_json.py might be used for type hinting or future integration
# If not directly used, it can be removed.
# import analyse_game_json # Commented out if not strictly needed for this file's logic

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants ---
# Keys that often contain lists of game references (from experience and examples)
KNOWN_GAME_LIST_KEYS: List[str] = ['f', '12']
# '12' is hexadecimal for 18, which appeared in an example.
# Sometimes it's also the value of a "games" key in another object.

GAME_SUMMARY_TYPE: str = 'GameSummary'
META_FIELD: str = 'meta'
HOME_TEAM_FIELD: str = 'homeTeam'
ID_FIELD: str = 'id'
TYPE_FIELD: str = 'type'
GAMES_FIELD: str = 'games'


def extract_game_ids_from_schedule_data(schedule_data_string: str) -> List[str]:
    """
    Extrahiert numerische Spiel-IDs aus den speziellen, zeilenbasierten Daten 
    einer handball.net Spielplan- oder "Alle Spiele"-Seite.
    Diese Version ist robuster gegenüber Variationen in der Datenstruktur.

    Args:
        schedule_data_string (str): Der mehrzeilige String mit den Rohdaten.

    Returns:
        List[str]: Eine Liste von eindeutigen, sortierten Spiel-IDs (als Strings).
                   Gibt eine leere Liste zurück, wenn keine IDs gefunden werden konnten.
    """
    definitions: Dict[str, Any] = {}

    # 1. Daten parsen und Definitionen sammeln
    for line_num, line in enumerate(schedule_data_string.splitlines()):
        line = line.strip()
        if not line:
            continue

        try:
            key, value_str = line.split(':', 1)
            key = key.strip()
            value_str = value_str.strip()

            if value_str.startswith('I['):  # Format wie "4:I[...]"
                value_str = value_str[1:]

            parsed_value: Any = json.loads(value_str)
            definitions[key] = parsed_value
        except (ValueError, json.JSONDecodeError) as e:
            logger.debug(f"Zeile {line_num + 1} konnte nicht als Key-Value/JSON geparst werden: {line[:70]}... ({e})")
            pass
        except Exception as e:
            logger.error(f"Allgemeiner Fehler beim Parsen von Zeile {line_num + 1}: {line[:70]}... ({e})")
            pass

    game_ids: Set[str] = set()  # Verwende ein Set, um Duplikate automatisch zu vermeiden

    # 2. Versuche, die Hauptliste der Spielreferenzen zu finden und zu verarbeiten
    game_summary_refs: List[Any] = []
    identified_ref_list_key: Optional[str] = None

    # Prüfe bekannte Schlüssel zuerst
    for key in KNOWN_GAME_LIST_KEYS:
        if key in definitions and isinstance(definitions[key], list):
            potential_refs: List[Any] = definitions[key]
            is_likely_game_list: bool = False
            if potential_refs:
                ref_count: int = 0
                game_like_target_count: int = 0
                for item in potential_refs:
                    if isinstance(item, str) and item.startswith('$'):
                        ref_count += 1
                        target_key: str = item[1:]
                        target_obj: Optional[Dict[str, Any]] = definitions.get(target_key)
                        if isinstance(target_obj, dict) and \
                           (target_obj.get(TYPE_FIELD) == GAME_SUMMARY_TYPE or
                            META_FIELD in target_obj or HOME_TEAM_FIELD in target_obj):
                            game_like_target_count += 1
                # Wenn ein signifikanter Anteil Referenzen sind und diese auf spielähnliche Objekte zeigen
                if ref_count > 0 and (game_like_target_count / ref_count) > 0.5:
                    is_likely_game_list = True
            
            if is_likely_game_list:
                game_summary_refs = potential_refs
                identified_ref_list_key = key
                logger.info(f"Spielreferenzliste unter bekanntem Schlüssel '{key}' gefunden.")
                break
    
    # Wenn kein bekannter Schlüssel passt, suche nach anderen Indikatoren
    if not identified_ref_list_key:
        for key, value in definitions.items():
            # Suchen nach Strukturen wie {"games": "$ref_zu_liste"}
            if isinstance(value, dict) and isinstance(value.get(GAMES_FIELD), str) and value.get(GAMES_FIELD, "").startswith('$'):
                ref_list_key_candidate: str = value.get(GAMES_FIELD, "")[1:]
                if ref_list_key_candidate in definitions and isinstance(definitions[ref_list_key_candidate], list):
                    game_summary_refs = definitions[ref_list_key_candidate]
                    identified_ref_list_key = ref_list_key_candidate
                    logger.info(f"Spielreferenzliste über verschachtelten '{GAMES_FIELD}'-Schlüssel '{identified_ref_list_key}' gefunden.")
                    break
            # Heuristik für die längste Liste von $-Referenzen (weniger zuverlässig)
            elif isinstance(value, list) and len(value) > 10:  # Mindestlänge einer Spielliste
                ref_count = sum(1 for item in value if isinstance(item, str) and item.startswith('$'))
                if ref_count / len(value) > 0.8:  # Über 80% sind Referenzen
                    if not game_summary_refs or len(value) > len(game_summary_refs):
                        game_summary_refs = value
                        identified_ref_list_key = key
        if identified_ref_list_key and not game_summary_refs: # Falls Schlüssel gefunden, aber Liste leer
             game_summary_refs = definitions.get(identified_ref_list_key, [])

        if identified_ref_list_key and game_summary_refs:
             logger.info(f"Spielreferenzliste heuristisch unter Schlüssel '{identified_ref_list_key}' gefunden.")

    # 3. Extrahiere IDs aus der gefundenen Referenzliste
    if game_summary_refs:
        for ref_item in game_summary_refs:
            if isinstance(ref_item, str) and ref_item.startswith('$'):
                game_summary_key: str = ref_item[1:]
                game_summary_object: Optional[Dict[str, Any]] = definitions.get(game_summary_key)

                if game_summary_object and isinstance(game_summary_object, dict):
                    meta_str: Optional[str] = game_summary_object.get(META_FIELD)
                    if meta_str and isinstance(meta_str, str):
                        match: Optional[re.Match] = re.search(r'H4A-Spiel-ID:\s*(\d+)', meta_str)
                        if match:
                            game_ids.add(match.group(1))
                            continue
                    
                    full_id_str: Optional[str] = game_summary_object.get(ID_FIELD)
                    if full_id_str and isinstance(full_id_str, str) and '.' in full_id_str:
                        potential_id: str = full_id_str.split('.')[-1]
                        if potential_id.isdigit():
                            game_ids.add(potential_id)
            else:
                logger.debug(f"Ignoriere Element in Spielreferenzliste: {ref_item} (Typ: {type(ref_item)})")
    
    # 4. Fallback: Wenn keine Referenzliste gefunden wurde oder sie leer war,
    # durchsuche alle Definitionen nach GameSummary-Objekten.
    if not game_ids:
        logger.info("Keine Spiel-IDs über Referenzliste gefunden. Durchsuche alle Definitionen nach GameSummary-Objekten.")
        for key, value_obj in definitions.items():
            if isinstance(value_obj, dict):
                is_game_summary: bool = value_obj.get(TYPE_FIELD) == GAME_SUMMARY_TYPE
                has_meta_id: bool = False
                meta_str = value_obj.get(META_FIELD)
                if meta_str and isinstance(meta_str, str):
                    if re.search(r'H4A-Spiel-ID:\s*(\d+)', meta_str):
                        has_meta_id = True
                
                if is_game_summary or has_meta_id:
                    if has_meta_id and meta_str: # Ensure meta_str is not None
                         match = re.search(r'H4A-Spiel-ID:\s*(\d+)', meta_str)
                         if match: game_ids.add(match.group(1))
                    
                    full_id_str = value_obj.get(ID_FIELD)
                    if full_id_str and isinstance(full_id_str, str) and '.' in full_id_str:
                        potential_id = full_id_str.split('.')[-1]
                        if potential_id.isdigit():
                            game_ids.add(potential_id)
                            
    return sorted(list(game_ids))


if __name__ == "__main__":
    logger.info("Bitte füge den mehrzeiligen Datenblock von der Webseite ein.")
    logger.info("Beende die Eingabe mit einer leeren Zeile und Strg+D (Linux/macOS) oder Strg+Z+Enter (Windows).")
    
    input_lines: List[str] = []
    while True:
        try:
            line: str = input()
            if line == "": 
                break
            input_lines.append(line)
        except EOFError: 
            break
            
    deine_schedule_daten_eingabe: str = "\n".join(input_lines)

    gefundene_ids_main: List[str] = []
    if not deine_schedule_daten_eingabe.strip():
        logger.warning("Keine Daten eingegeben. Verwende stattdessen die Beispieldaten aus dem Skript.")
        # Hier die Daten von deinem letzten Beispiel
        deine_schedule_daten_eingabe = """
        3:I[4707,[],""]
        5:I[36423,[],""]
        4:["tournamentId","handball4all.westfalen.f-kk-1_wfms","d"]
        f:["$13","$1a","$21"]
        13:{"id":"handball4all.westfalen.7618101","meta":"H4A-Spiel-ID: 7618101"}
        1a:{"id":"handball4all.westfalen.7618111","meta":"H4A-Spiel-ID: 7618111"}
        21:{"id":"handball4all.westfalen.7618121","meta":"H4A-Spiel-ID: 7618121"}
        """ 
        logger.info("Verwende gekürzte Beispieldaten...")

    if deine_schedule_daten_eingabe:
        gefundene_ids_main = extract_game_ids_from_schedule_data(deine_schedule_daten_eingabe)
        if gefundene_ids_main:
            logger.info(f"--- Gefundene Spiel-IDs ({len(gefundene_ids_main)}) ---")
            for spiel_id in gefundene_ids_main:
                logger.info(spiel_id)
        else:
            logger.warning("Keine Spiel-IDs gefunden oder die Datenstruktur ist zu unterschiedlich.")
    else:
        logger.info("Keine Daten zur Verarbeitung.")

    # This import and call might be better handled by a main script orchestrating the process
    # For now, keeping it as per original structure if this script is run standalone.
    try:
        from analyse_game_json import main as analyse_main # Renamed to avoid conflict
        if gefundene_ids_main:
             analyse_main(gefundene_ids_main)
        else:
            logger.info("Keine IDs zum Analysieren an analyse_game_json.py übergeben.")
    except ImportError:
        logger.error("analyse_game_json.py konnte nicht importiert werden. Stelle sicher, dass es im selben Verzeichnis liegt.")
    except Exception as e:
        logger.error(f"Fehler beim Aufrufen von analyse_game_json.main: {e}")
