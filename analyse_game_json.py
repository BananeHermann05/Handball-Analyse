import streamlit as st # Hinzufügen für st.secrets
import psycopg2
import psycopg2.extras
import requests
import json
import time
import re
import logging
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional, Set
import os
from dotenv import load_dotenv # Behalten für lokalen Fallback

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

# --- Lade .env für lokalen Fallback (python-dotenv) ---
env_paths_to_try = [
    os.path.join(os.path.dirname(__file__), 'database.env'),
    os.path.join(os.path.dirname(__file__), '.env'),
    os.path.join(os.getcwd(), '.env')
]
loaded_env_file = None
for path_to_try in env_paths_to_try:
    if os.path.exists(path_to_try):
        if load_dotenv(path_to_try, verbose=True):
            loaded_env_file = path_to_try
            logger.info(f"Lokale .env/.database.env aus {loaded_env_file} geladen für analyse_game_json (Fallback).") #
            break

# --- Datenbank-Credentials laden (st.secrets primär, dann os.environ) ---
DB_NAME_PG = st.secrets.get("PG_DB_NAME", os.environ.get("PG_DB_NAME"))
DB_USER_PG = st.secrets.get("PG_DB_USER", os.environ.get("PG_DB_USER"))
DB_PASSWORD_PG = st.secrets.get("PG_DB_PASSWORD", os.environ.get("PG_DB_PASSWORD"))
DB_HOST_PG = st.secrets.get("PG_DB_HOST", os.environ.get("PG_DB_HOST"))
DB_PORT_PG = st.secrets.get("PG_DB_PORT", os.environ.get("PG_DB_PORT", "5432"))

# Logging der geladenen Variablen (ohne Passwort) für Debugging
logger.info(f"analyse_game_json - DB_NAME_PG: '{DB_NAME_PG}'") #
logger.info(f"analyse_game_json - DB_USER_PG: '{DB_USER_PG}'") #
logger.info(f"analyse_game_json - DB_HOST_PG: '{DB_HOST_PG}'") #
logger.info(f"analyse_game_json - DB_PORT_PG: '{DB_PORT_PG}'") #


# --- Constants ---
BASE_URL: str = "https://www.handball.net/a/sportdata/1/games/handball4all.westfalen.{id}/combined?" #
# ... (Rest der Konstanten und Hilfsfunktionen wie zuvor) ...
# Die Funktion get_db_connection() verwendet bereits die global definierten Credentials.

# ... (Alle anderen Funktionen wie parse_score, get_saison_from_timestamp, extract_data_from_game_json,
#      batch_upsert_entities, batch_upsert_spiele, batch_insert_data, main_batched bleiben strukturell gleich,
#      da sie get_db_connection() aufrufen, das jetzt die Credentials korrekt bezieht) ...


# --- Constants ---
BASE_URL: str = "https://www.handball.net/a/sportdata/1/games/handball4all.westfalen.{id}/combined?" #
REQUEST_HEADERS: Dict[str, str] = { #
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
DEFAULT_BATCH_SIZE: int = 20 # Anzahl der Spiele pro Batch

TABLE_LIGEN: str = "\"Ligen\"" #
TABLE_TEAMS: str = "\"Teams\"" #
TABLE_SPIELER: str = "\"Spieler\"" #
TABLE_HALLEN: str = "\"Hallen\"" #
TABLE_SPIELE: str = "\"Spiele\"" #
TABLE_KADER_STATS: str = "\"Spiel_Kader_Statistiken\"" #
TABLE_EREIGNISSE: str = "\"Ereignisse\"" #

# Spaltennamen für execute_values (ohne Anführungszeichen für Dict-Keys, mit für SQL)
LEAGUE_COLS = ["Liga_ID", "Name", "Akronym", "Saison", "Altersgruppe", "Typ"]
TEAM_COLS = ["Team_ID", "Name", "Akronym", "Logo_URL"]
HALL_COLS = ["Hallen_ID", "Name", "Stadt", "Hallen_Nummer"]
PLAYER_COLS = ["Spieler_ID", "Vorname", "Nachname", "Ist_NN", "Ist_Offizieller"] # Position, etc. werden hier nicht gebatcht, da sie nicht immer vorhanden sind
GAME_COLS_INITIAL = ["Spiel_ID", "Liga_ID", "Phase_ID", "Hallen_ID", "Spiel_Nummer", "Start_Zeit", "Heim_Team_ID", "Gast_Team_ID", "Status", "PDF_URL", "SchiedsrichterInfo"]
GAME_COLS_RESULTS = ["Tore_Heim", "Tore_Gast", "Tore_Heim_HZ", "Tore_Gast_HZ", "Punkte_Heim_Offiziell", "Punkte_Gast_Offiziell"]
KADER_STATS_COLS = ["Spiel_ID", "Spieler_ID", "Team_ID", "Rueckennummer", "Tore_Gesamt", "Tore_7m", "Fehlwurf_7m", "Gelbe_Karten", "Rote_Karten", "Blaue_Karten", "Zwei_Minuten_Strafen"]
EVENT_COLS = ["H4A_Ereignis_ID", "Spiel_ID", "Zeitstempel", "Spiel_Minute", "Typ", "Score_Heim", "Score_Gast", "Team_Seite", "Nachricht", "Referenz_Spieler_ID"]

# --- Hilfsfunktionen ---
def get_db_connection() -> Optional[psycopg2.extensions.connection]: #
    if not all([DB_NAME_PG, DB_USER_PG, DB_PASSWORD_PG, DB_HOST_PG, DB_PORT_PG]): #
        logger.error("Unvollständige PostgreSQL-Verbindungsinformationen in analyse_game_json.") #
        return None
    try:
        conn = psycopg2.connect( #
            dbname=DB_NAME_PG, user=DB_USER_PG, password=DB_PASSWORD_PG,
            host=DB_HOST_PG.strip() if DB_HOST_PG else None,
            port=DB_PORT_PG,
            sslmode='require'
        )
        return conn
    except psycopg2.Error as e: #
        logger.error(f"Fehler beim Verbinden mit PostgreSQL: {e}") #
    except Exception as e_gen: #
        logger.error(f"Allgemeiner Fehler bei DB-Verbindung: {e_gen}", exc_info=True) #
    return None

def parse_score(score_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]: #
    if not score_str: return None, None #
    try:
        heim, gast = map(int, score_str.split(':')) #
        return heim, gast
    except ValueError: #
        logger.warning(f"Ungültiges Score-Format: {score_str}") #
        return None, None
    except Exception as e: #
        logger.error(f"Fehler beim Parsen des Scores '{score_str}': {e}") #
        return None, None

def parse_player_from_message(message: str) -> Optional[int]: #
    match_parentheses: Optional[re.Match] = re.search(r'\(([\d]+)\.\)', message) #
    if match_parentheses: return int(match_parentheses.group(1)) #
    match_direct: Optional[re.Match] = re.search(r'(?:Tor durch|durch)\s+([\d]+)\.', message) #
    if match_direct: return int(match_direct.group(1)) #
    return None

def get_saison_from_timestamp(timestamp_ms: Optional[int]) -> str: #
    if timestamp_ms is None: return "Unbekannt" #
    try:
        dt_object: datetime = datetime.fromtimestamp(timestamp_ms / 1000) #
        year: int = dt_object.year #
        month: int = dt_object.month #
        if month >= 7: return f"{year}/{year + 1}" #
        else: return f"{year - 1}/{year}" #
    except Exception as e: #
        logger.error(f"Fehler beim Ermitteln der Saison aus Timestamp {timestamp_ms}: {e}") #
        return "Unbekannt"

# --- Datenextraktionsfunktion (ersetzt den Kern von process_single_game) ---
def extract_data_from_game_json(game_json_data: Dict[str, Any], game_id_for_url: str) -> Optional[Dict[str, Any]]:
    """Extrahiert alle relevanten Daten aus der JSON-Antwort eines Spiels."""
    extracted_batch_data = {
        "leagues": set(), "teams": set(), "halls": set(), "players": set(),
        "game_initial_data": None, "game_result_data": None,
        "kader_stats": [], "events": [], "spiel_id_full": None,
        "player_map_for_events": {}
    }

    game_data_node: Optional[Dict[str, Any]] = game_json_data.get('data') #
    if not game_data_node: #
        logger.error(f"Kein 'data'-Knoten in JSON für Spiel {game_id_for_url} gefunden.") #
        return None

    summary: Optional[Dict[str, Any]] = game_data_node.get('summary') #
    lineup: Dict[str, Any] = game_data_node.get('lineup', {}) #
    events_raw: List[Dict[str, Any]] = game_data_node.get('events', []) #

    if not summary: #
        logger.error(f"Unvollständige Datenstruktur für Spiel {game_id_for_url}. Summary fehlt.") #
        return None
    
    spiel_id_full = summary['id'] #
    extracted_batch_data["spiel_id_full"] = spiel_id_full

    # 1. Ligen, Teams, Hallen
    tournament_data = summary.get('tournament') #
    round_data = summary.get('round') #
    db_liga_id_fuer_spiel = None
    if tournament_data: #
        saison = get_saison_from_timestamp(round_data.get('startsAt') if round_data else tournament_data.get('startsAt')) #
        original_tournament_id = tournament_data['id'] #
        db_liga_id_fuer_spiel = f"{original_tournament_id}_{saison.replace('/', '_')}" #
        display_name = tournament_data['name'] #
        if saison != "Unbekannt" and saison not in display_name and not re.search(r'\b\d{4}\b', display_name): #
            display_name = f"{tournament_data['name']} ({saison})" #
        extracted_batch_data["leagues"].add(tuple(tournament_data.get(col) for col in ["id", "name", "acronym", "ageGroup", "tournamentType"]) + (saison, db_liga_id_fuer_spiel, display_name))
    
    home_team = summary.get('homeTeam') #
    away_team = summary.get('awayTeam') #
    field_data = summary.get('field') #

    if home_team: extracted_batch_data["teams"].add((home_team['id'], home_team['name'], home_team.get('acronym'), home_team.get('logo'))) #
    if away_team: extracted_batch_data["teams"].add((away_team['id'], away_team['name'], away_team.get('acronym'), away_team.get('logo'))) #
    if field_data: extracted_batch_data["halls"].add((field_data['id'], field_data['name'], field_data.get('city'), field_data.get('fieldNumber'))) #

    # 2. Spiel Initialdaten
    home_team_id = home_team.get('id') if home_team else None
    away_team_id = away_team.get('id') if away_team else None
    if not home_team_id or not away_team_id:
        logger.error(f"Heim- oder Gastteam-ID fehlt im Summary für Spiel {spiel_id_full}.") #
        return None
    
    extracted_batch_data["game_initial_data"] = {
        'Spiel_ID': spiel_id_full, 'Liga_ID': db_liga_id_fuer_spiel,
        'Phase_ID': summary.get('phase', {}).get('id'), 'Hallen_ID': field_data.get('id') if field_data else None,
        'Spiel_Nummer': summary.get('gameNumber'), 'Start_Zeit': summary.get('startsAt', 0) // 1000,
        'Heim_Team_ID': home_team_id, 'Gast_Team_ID': away_team_id,
        'Status': summary.get('state'), 'PDF_URL': summary.get('pdfUrl'),
        'SchiedsrichterInfo': summary.get('refereeInfo')
    }

    # 3. Spieler und Kader
    player_map_for_events = {} # (team_side_str, number_int) -> player_id_str
    temp_kader_stats = []
    two_min_counts_for_game = {} # player_id -> count

    def process_lineup_side(side_name_json: str, player_list_raw: Optional[List[Dict[str, Any]]], officials_list_raw: Optional[List[Dict[str, Any]]]): #
        team_id_for_stats = home_team_id if side_name_json == 'home' else away_team_id
        if not team_id_for_stats: return

        for player_json in (player_list_raw or []): #
            if not isinstance(player_json, dict) or 'id' not in player_json: continue #
            is_nn = 1 if player_json.get('firstname', '').upper() == 'N.N.' else 0 #
            extracted_batch_data["players"].add((player_json['id'], player_json.get('firstname'), player_json.get('lastname'), is_nn, 0))
            
            tore_7m = player_json.get('penaltyGoals', 0) or 0 #
            fehlwurf_7m = player_json.get('penaltyMissed', 0) or 0 #
            
            kader_entry = { #
                'Spiel_ID': spiel_id_full, 'Spieler_ID': player_json['id'], 'Team_ID': team_id_for_stats,
                'Rueckennummer': player_json.get('number'), 'Tore_Gesamt': player_json.get('goals', 0),
                'Tore_7m': tore_7m, 'Fehlwurf_7m': fehlwurf_7m,
                'Gelbe_Karten': player_json.get('yellowCards', 0), 'Rote_Karten': player_json.get('redCards', 0),
                'Blaue_Karten': player_json.get('blueCards', 0), 'Zwei_Minuten_Strafen': 0 # Wird später aus Events gefüllt
            }
            temp_kader_stats.append(kader_entry)
            if player_json.get('number') is not None: #
                player_map_for_events[(side_name_json.capitalize(), player_json['number'])] = player_json['id'] #

        for official_json in (officials_list_raw or []): #
            if not isinstance(official_json, dict) or 'id' not in official_json: continue #
            extracted_batch_data["players"].add((official_json['id'], official_json.get('firstname'), official_json.get('lastname'), 0, 1))
            # Officials haben keine Kader-Stats im engeren Sinne

    process_lineup_side('home', lineup.get('home'), lineup.get('homeOfficials')) #
    process_lineup_side('away', lineup.get('away'), lineup.get('awayOfficials')) #
    extracted_batch_data["player_map_for_events"] = player_map_for_events
    
    # 4. Events und daraus abgeleitete Stats (2-Minuten, Spielstände)
    final_score_event = (None, None) #
    half_time_score_event = (None, None) #

    for event_json in events_raw: #
        if not isinstance(event_json, dict) or 'id' not in event_json: continue #
        h4a_id = event_json['id'] #
        timestamp = event_json.get('timestamp', 0) // 1000 #
        minute_val = event_json.get('time', "00:00") #
        typ_val = event_json.get('type', "Unknown") #
        score_h, score_g = parse_score(event_json.get('score')) #
        team_seite = event_json.get('team') #
        nachricht = event_json.get('message', '') #
        ref_spieler_id = None
        nummer = parse_player_from_message(nachricht) #
        if nummer is not None and team_seite is not None: #
            ref_spieler_id = player_map_for_events.get((team_seite, nummer)) #
        
        if typ_val == 'TwoMinutePenalty' and ref_spieler_id: #
            two_min_counts_for_game[ref_spieler_id] = two_min_counts_for_game.get(ref_spieler_id, 0) + 1 #

        if nachricht == "Spielstand 1. Halbzeit": half_time_score_event = (score_h, score_g) #
        if nachricht == "Spielstand 2. Halbzeit" or nachricht == "Spielabschluss mit Pins Heim/Gast/SRA/SRB": #
             if score_h is not None and score_g is not None: final_score_event = (score_h, score_g) #

        extracted_batch_data["events"].append({ #
            'H4A_Ereignis_ID': h4a_id, 'Spiel_ID': spiel_id_full, 'Zeitstempel': timestamp,
            'Spiel_Minute': minute_val, 'Typ': typ_val, 'Score_Heim': score_h, 'Score_Gast': score_g,
            'Team_Seite': team_seite, 'Nachricht': nachricht, 'Referenz_Spieler_ID': ref_spieler_id
        })

    # Update Kader-Stats mit 2-Minuten-Strafen
    for kader_entry in temp_kader_stats:
        kader_entry['Zwei_Minuten_Strafen'] = two_min_counts_for_game.get(kader_entry['Spieler_ID'], 0)
    extracted_batch_data["kader_stats"] = temp_kader_stats
    
    # 5. Spiel Endergebnis und Punkte
    extra_states = summary.get('extraStates') or [] #
    tore_h_final = final_score_event[0] if final_score_event[0] is not None else summary.get('homeGoals') #
    tore_g_final = final_score_event[1] if final_score_event[1] is not None else summary.get('awayGoals') #
    hzh_final = half_time_score_event[0] if half_time_score_event[0] is not None else summary.get('homeGoalsHalf') #
    hzg_final = half_time_score_event[1] if half_time_score_event[1] is not None else summary.get('awayGoalsHalf') #
    punkte_h_offiziell, punkte_g_offiziell = None, None #

    if "WoHome" in extra_states: #
        tore_h_final, tore_g_final, hzh_final, hzg_final = 0, 0, None, None #
        punkte_h_offiziell, punkte_g_offiziell = 0, 2 #
    elif "WoAway" in extra_states: #
        tore_h_final, tore_g_final, hzh_final, hzg_final = 0, 0, None, None #
        punkte_h_offiziell, punkte_g_offiziell = 2, 0 #
    else: #
        if tore_h_final is None: tore_h_final = summary.get('homeGoals', 0) #
        if tore_g_final is None: tore_g_final = summary.get('awayGoals', 0) #
        if tore_h_final is not None and tore_g_final is not None: #
            if tore_h_final > tore_g_final: punkte_h_offiziell, punkte_g_offiziell = 2, 0 #
            elif tore_h_final < tore_g_final: punkte_h_offiziell, punkte_g_offiziell = 0, 2 #
            else: punkte_h_offiziell, punkte_g_offiziell = 1, 1 #
    
    extracted_batch_data["game_result_data"] = { #
        'Spiel_ID': spiel_id_full, 
        'Tore_Heim': tore_h_final if tore_h_final is not None else 0, 
        'Tore_Gast': tore_g_final if tore_g_final is not None else 0,
        'Tore_Heim_HZ': hzh_final, 'Tore_Gast_HZ': hzg_final,
        'Punkte_Heim_Offiziell': punkte_h_offiziell, 'Punkte_Gast_Offiziell': punkte_g_offiziell
    }
    
    return extracted_batch_data

# --- Batch Datenbankfunktionen ---
def batch_upsert_entities(cursor: psycopg2.extensions.cursor, data_set: Set[Tuple], table_name: str, pk_col_name: str, column_names: List[str]):
    if not data_set: return
    
    cols_sql = ", ".join([f'"{col}"' for col in column_names])
    placeholders = ", ".join(["%s"] * len(column_names))
    
    # Konvertiere das Set von Tupeln in eine Liste von Tupeln für execute_values
    data_list = list(data_set)

    # Für Ligen müssen wir das Tupel anpassen, da wir spezifische Werte aus dem größeren Tupel brauchen
    if table_name == TABLE_LIGEN:
        # (original_id, name, acronym, ageGroup, tournamentType, saison, db_liga_id_fuer_spiel, display_name)
        # Zielspalten: Liga_ID (db_liga_id_fuer_spiel), Name (display_name), Akronym, Saison, Altersgruppe, Typ
        formatted_data_list = [
            (item[6], item[7], item[2], item[5], item[3], item[4]) for item in data_list
        ]
        sql = f"""INSERT INTO {table_name} ("Liga_ID", "Name", "Akronym", "Saison", "Altersgruppe", "Typ") VALUES %s
                  ON CONFLICT ("Liga_ID") DO NOTHING;"""
        psycopg2.extras.execute_values(cursor, sql, formatted_data_list, page_size=len(data_list))
    else:
        sql = f"""INSERT INTO {table_name} ({cols_sql}) VALUES %s
                  ON CONFLICT ("{pk_col_name}") DO NOTHING;"""
        psycopg2.extras.execute_values(cursor, sql, data_list, page_size=len(data_list))
    logger.info(f"{len(data_list)} Entitäten in {table_name} verarbeitet (INSERT/IGNORE).")


def batch_upsert_spiele(cursor: psycopg2.extensions.cursor, games_initial_list: List[Dict[str, Any]], games_results_list: List[Dict[str, Any]]):
    if not games_initial_list: return

    # 1. Batch Upsert für initiale Spieldaten
    initial_data_tuples = []
    for game_data in games_initial_list:
        initial_data_tuples.append(tuple(game_data.get(col) for col in GAME_COLS_INITIAL))

    cols_initial_sql = ", ".join([f'"{col}"' for col in GAME_COLS_INITIAL])
    update_cols_initial = [f'"{col}" = excluded."{col}"' for col in GAME_COLS_INITIAL if col != "Spiel_ID"]
    
    sql_initial = f"""INSERT INTO {TABLE_SPIELE} ({cols_initial_sql}) VALUES %s
                      ON CONFLICT ("Spiel_ID") DO UPDATE SET {', '.join(update_cols_initial)};"""
    psycopg2.extras.execute_values(cursor, sql_initial, initial_data_tuples, page_size=len(initial_data_tuples))
    logger.info(f"{len(initial_data_tuples)} Spiele (Initialdaten) verarbeitet (UPSERT).")

    # 2. Batch Update für Spielergebnisse (kann nicht direkt mit execute_values für ON CONFLICT mit verschiedenen WHEREs)
    # Stattdessen verwenden wir eine Serie von UPDATEs in einer Transaktion.
    # Oder, wenn alle Spiele im Batch immer aktualisiert werden sollen, ein simplerer UPDATE
    # Hier gehen wir davon aus, dass wir nur die Spiele im Batch aktualisieren wollen.
    # Besser: execute_values mit ON CONFLICT ... DO UPDATE SET ...
    # Aber da wir bereits oben die initialen Daten upserted haben, können wir hier normale UPDATEs machen
    # ODER wir kombinieren es in den UPSERT oben, falls die Result-Daten schon da sind
    # Für dieses Refactoring machen wir separate Updates für Klarheit
    
    result_data_tuples = []
    for res_data in games_results_list:
         result_data_tuples.append(tuple(res_data.get(col) for col in GAME_COLS_RESULTS) + (res_data["Spiel_ID"],))

    if result_data_tuples:
        cols_results_set_sql = [f'"{col}" = %s' for col in GAME_COLS_RESULTS]
        sql_results_update = f"""UPDATE {TABLE_SPIELE} SET {', '.join(cols_results_set_sql)} WHERE "Spiel_ID" = %s;"""
        psycopg2.extras.execute_batch(cursor, sql_results_update, result_data_tuples, page_size=len(result_data_tuples))
        logger.info(f"{len(result_data_tuples)} Spielergebnisse aktualisiert.")


def batch_insert_data(cursor: psycopg2.extensions.cursor, data_list: List[Dict[str,Any]], table_name: str, column_names: List[str], unique_constraint_cols: Optional[List[str]] = None):
    if not data_list: return
    
    tuples_to_insert = [tuple(item.get(col) for col in column_names) for item in data_list]
    cols_sql = ", ".join([f'"{col}"' for col in column_names])
    
    if unique_constraint_cols:
        conflict_target = ", ".join([f'"{col}"' for col in unique_constraint_cols])
        # Erstelle den DO UPDATE Teil dynamisch
        update_assignments = [f'"{col}" = excluded."{col}"' for col in column_names if col not in unique_constraint_cols]
        update_clause = ", ".join(update_assignments)
        sql = f"""INSERT INTO {table_name} ({cols_sql}) VALUES %s
                  ON CONFLICT ({conflict_target}) DO UPDATE SET {update_clause};"""
    else: # Reiner Insert
        sql = f"""INSERT INTO {table_name} ({cols_sql}) VALUES %s;"""

    psycopg2.extras.execute_values(cursor, sql, tuples_to_insert, page_size=len(tuples_to_insert))
    logger.info(f"{len(tuples_to_insert)} Einträge in {table_name} verarbeitet.")


# --- Haupt-Batch-Verarbeitungsfunktion ---
def main_batched(game_ids_to_process: List[str], batch_size: int = DEFAULT_BATCH_SIZE):
    if not all([DB_NAME_PG, DB_USER_PG, DB_PASSWORD_PG, DB_HOST_PG, DB_PORT_PG]): #
        logger.critical("PostgreSQL-Verbindungsinformationen nicht gesetzt. Batch-Verarbeitung kann nicht ausgeführt werden.") #
        return {"success": 0, "error": len(game_ids_to_process), "total": len(game_ids_to_process)}

    if not game_ids_to_process: #
        logger.warning("Keine Spiel-IDs zum Verarbeiten übergeben.") #
        return {"success": 0, "error": 0, "total": 0}

    conn: Optional[psycopg2.extensions.connection] = get_db_connection() #
    if not conn: #
        logger.critical("Konnte keine Datenbankverbindung herstellen. Breche ab.") #
        return {"success": 0, "error": len(game_ids_to_process), "total": len(game_ids_to_process)}

    processed_successfully_count = 0
    error_count = 0
    total_to_process = len(game_ids_to_process)
    
    logger.info(f"Starte Batch-Verarbeitung von {total_to_process} Spielen mit Batch-Größe {batch_size}...")

    # Sammelbehälter für einen Batch
    leagues_batch: Set[Tuple] = set()
    teams_batch: Set[Tuple] = set()
    halls_batch: Set[Tuple] = set()
    players_batch: Set[Tuple] = set()
    games_initial_batch: List[Dict[str, Any]] = []
    games_results_batch: List[Dict[str, Any]] = []
    kader_stats_batch: List[Dict[str, Any]] = []
    events_batch: List[Dict[str, Any]] = []
    game_ids_in_current_batch: List[str] = []

    cursor: Optional[psycopg2.extensions.cursor] = None
    try:
        cursor = conn.cursor()

        for i, game_id_for_url in enumerate(game_ids_to_process):
            logger.info(f"Verarbeite Spiel {i+1}/{total_to_process} (URL-ID: {game_id_for_url}) - Extraktionsphase...")
            url = BASE_URL.format(id=game_id_for_url) #
            
            try:
                response = requests.get(url, headers=REQUEST_HEADERS, timeout=20) #
                response.raise_for_status() #
                game_json = response.json() #
            except requests.exceptions.RequestException as e: #
                logger.error(f"Fehler beim Abrufen von Spiel {game_id_for_url}: {e}") #
                error_count += 1
                continue
            except json.JSONDecodeError: #
                logger.error(f"Fehler: Konnte JSON für Spiel {game_id_for_url} nicht parsen.") #
                error_count += 1
                continue

            extracted_data = extract_data_from_game_json(game_json, game_id_for_url)
            if not extracted_data or not extracted_data.get("spiel_id_full"):
                logger.error(f"Fehler beim Extrahieren der Daten für Spiel {game_id_for_url}.")
                error_count += 1
                continue
            
            # Daten sammeln
            leagues_batch.update(extracted_data["leagues"])
            teams_batch.update(extracted_data["teams"])
            halls_batch.update(extracted_data["halls"])
            players_batch.update(extracted_data["players"])
            if extracted_data["game_initial_data"]: games_initial_batch.append(extracted_data["game_initial_data"])
            if extracted_data["game_result_data"]: games_results_batch.append(extracted_data["game_result_data"])
            kader_stats_batch.extend(extracted_data["kader_stats"])
            events_batch.extend(extracted_data["events"])
            game_ids_in_current_batch.append(extracted_data["spiel_id_full"])

            # Batch verarbeiten, wenn Größe erreicht oder letztes Element
            if (i + 1) % batch_size == 0 or (i + 1) == total_to_process:
                logger.info(f"Verarbeite Batch (Spiele {i+1-len(game_ids_in_current_batch)+1} bis {i+1})...")
                try:
                    # 1. Eindeutige Entitäten (upsert)
                    batch_upsert_entities(cursor, leagues_batch, TABLE_LIGEN, "Liga_ID", ["Liga_ID", "Name", "Akronym", "Saison", "Altersgruppe", "Typ"]) # Angepasste Spalten
                    batch_upsert_entities(cursor, teams_batch, TABLE_TEAMS, "Team_ID", TEAM_COLS)
                    batch_upsert_entities(cursor, halls_batch, TABLE_HALLEN, "Hallen_ID", HALL_COLS)
                    batch_upsert_entities(cursor, players_batch, TABLE_SPIELER, "Spieler_ID", PLAYER_COLS)

                    # 2. Spiele (upsert initial, dann update results)
                    batch_upsert_spiele(cursor, games_initial_batch, games_results_batch)

                    # 3. Kader & Events (delete old for batch, then batch insert)
                    if game_ids_in_current_batch:
                        # Erstelle eine Zeichenkette von Platzhaltern: (%s, %s, ...)
                        placeholders = ", ".join(["%s"] * len(game_ids_in_current_batch))
                        
                        logger.info(f"Lösche alte Kader-Statistiken für {len(game_ids_in_current_batch)} Spiele im Batch...")
                        cursor.execute(f"DELETE FROM {TABLE_KADER_STATS} WHERE \"Spiel_ID\" IN ({placeholders})", tuple(game_ids_in_current_batch))
                        
                        logger.info(f"Lösche alte Ereignisse für {len(game_ids_in_current_batch)} Spiele im Batch...")
                        cursor.execute(f"DELETE FROM {TABLE_EREIGNISSE} WHERE \"Spiel_ID\" IN ({placeholders})", tuple(game_ids_in_current_batch))
                    
                    batch_insert_data(cursor, kader_stats_batch, TABLE_KADER_STATS, KADER_STATS_COLS)
                    batch_insert_data(cursor, events_batch, TABLE_EREIGNISSE, EVENT_COLS, unique_constraint_cols=["Spiel_ID", "H4A_Ereignis_ID"]) # Hinzugefügt für ON CONFLICT

                    conn.commit() # Commit nach erfolgreichem Batch
                    processed_successfully_count += len(game_ids_in_current_batch)
                    logger.info(f"Batch erfolgreich verarbeitet. {len(game_ids_in_current_batch)} Spiele.")

                except psycopg2.Error as db_err: #
                    logger.error(f"Datenbankfehler während Batch-Verarbeitung: {db_err}", exc_info=True) #
                    conn.rollback() #
                    error_count += len(game_ids_in_current_batch) # Angenommen, der ganze Batch ist betroffen
                except Exception as e_batch: #
                    logger.error(f"Allgemeiner Fehler während Batch-Verarbeitung: {e_batch}", exc_info=True) #
                    conn.rollback() #
                    error_count += len(game_ids_in_current_batch)
                finally:
                    # Reset für nächsten Batch
                    leagues_batch.clear(); teams_batch.clear(); halls_batch.clear(); players_batch.clear()
                    games_initial_batch.clear(); games_results_batch.clear()
                    kader_stats_batch.clear(); events_batch.clear()
                    game_ids_in_current_batch.clear()
            
            # Kurze Pause, um die API nicht zu überlasten (optional, aber empfohlen)
            # time.sleep(0.1) 

    except Exception as e_main:
        logger.error(f"Unerwarteter Fehler im Haupt-Loop der Batch-Verarbeitung: {e_main}", exc_info=True)
        if conn: conn.rollback()
        # Zähle verbleibende Spiele als Fehler, wenn ein globaler Fehler auftritt
        error_count = total_to_process - processed_successfully_count 
    finally:
        if cursor: cursor.close() #
        if conn: conn.close() #

    logger.info("-" * 30) #
    logger.info("Batch-Verarbeitung abgeschlossen.") #
    logger.info(f"  Insgesamt zu verarbeiten: {total_to_process}") #
    logger.info(f"  Erfolgreich verarbeitet: {processed_successfully_count}") #
    logger.info(f"  Fehlerhaft: {error_count}") #
    logger.info("-" * 30) #
    return {"success": processed_successfully_count, "error": error_count, "total": total_to_process}


i# Am Ende der Datei zur Sicherheit:
if not all([DB_NAME_PG, DB_USER_PG, DB_HOST_PG]):
    logger.warning("Einige DB-Credentials sind für analyse_game_json nicht gesetzt. DB-Operationen könnten fehlschlagen.")
else:
    logger.info("DB-Credentials für analyse_game_json geladen (Host: %s, DB: %s, User: %s).", DB_HOST_PG, DB_NAME_PG, DB_USER_PG)

if __name__ == "__main__":
    # Stelle sicher, dass Secrets geladen werden, wenn das Skript direkt ausgeführt wird
    # (z.B. für lokale Tests ohne laufende Streamlit App)
    # Dies ist ein Hack, da st.secrets normalerweise nur in einer laufenden Streamlit App funktioniert.
    # Besser: Für lokale Tests, die nicht über Streamlit laufen, sich auf .env verlassen.
    if not hasattr(st, 'secrets'): # Prüfen, ob st.secrets überhaupt existiert
        logger.warning("st.secrets nicht verfügbar (wahrscheinlich kein Streamlit-Kontext). Verwende nur os.environ für direkte Skriptausführung.")
        DB_NAME_PG = os.environ.get("PG_DB_NAME")
        DB_USER_PG = os.environ.get("PG_DB_USER")
        DB_PASSWORD_PG = os.environ.get("PG_DB_PASSWORD")
        DB_HOST_PG = os.environ.get("PG_DB_HOST")
        DB_PORT_PG = os.environ.get("PG_DB_PORT", "5432")


    if not all([DB_NAME_PG, DB_USER_PG, DB_PASSWORD_PG, DB_HOST_PG]):
         print("FEHLER: Bitte setze die Umgebungsvariablen für PostgreSQL (PG_DB_NAME, PG_DB_USER, PG_DB_PASSWORD, PG_DB_HOST) in .env oder als Systemvariablen für lokale Tests.")
    else:
        beispiel_spiel_ids = ['7504381','7506331','7618266', '7618101', '7618111']
        logger.info(f"Starte Testlauf mit {len(beispiel_spiel_ids)} Spielen...")
        ergebnis = main_batched(beispiel_spiel_ids, batch_size=2)
        logger.info(f"Testlauf Ergebnis: {ergebnis}")