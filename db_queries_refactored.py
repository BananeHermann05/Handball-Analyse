import psycopg2
import psycopg2.extras
import pandas as pd
import logging
import os
import sqlalchemy
from sqlalchemy.engine.url import URL
from typing import List, Dict, Any, Optional

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

# --- Lade Umgebungsvariablen ---
try:
    from dotenv import load_dotenv
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
                logger.info(f"Umgebungsvariablen aus {loaded_env_file} geladen für db_queries_refactored.")
                break
    if not loaded_env_file:
        logger.info("Keine .env oder database.env Datei gefunden. Umgebungsvariablen müssen anderweitig gesetzt sein.")
except ImportError:
    logger.info("python-dotenv nicht installiert. Umgebungsvariablen müssen manuell gesetzt werden.")

DB_NAME_PG = os.environ.get("PG_DB_NAME")
DB_USER_PG = os.environ.get("PG_DB_USER")
DB_PASSWORD_PG = os.environ.get("PG_DB_PASSWORD")
DB_HOST_PG = os.environ.get("PG_DB_HOST")
DB_PORT_PG = os.environ.get("PG_DB_PORT", "5432")

# --- SQL Directory ---
SQL_DIR = os.path.join(os.path.dirname(__file__), 'sql')
if not os.path.exists(SQL_DIR):
    logger.warning(f"SQL-Verzeichnis '{SQL_DIR}' nicht gefunden. Erstelle es.")
    os.makedirs(SQL_DIR, exist_ok=True)

# --- Constants ---
COL_LIGA_ID: str = "Liga_ID"
COL_SPIEL_ID: str = "Spiel_ID"
COL_NAME: str = "Name"
COL_SAISON: str = "Saison"
COL_ALTERSGRUPPE: str = "Altersgruppe"
COL_TYP: str = "Typ"
COL_TEAM_ID: str = "Team_ID"
COL_AKRONYM: str = "Akronym"
COL_LOGO_URL: str = "Logo_URL"
COL_SPIELER_ID: str = "Spieler_ID"
COL_START_ZEIT: str = "Start_Zeit"
COL_STATUS: str = "Status"
COL_TORE_HEIM: str = "Tore_Heim"
COL_TORE_GAST: str = "Tore_Gast"
COL_PUNKTE_HEIM_OFFIZIELL: str = "Punkte_Heim_Offiziell"
COL_PUNKTE_GAST_OFFIZIELL: str = "Punkte_Gast_Offiziell"
COL_TORE_GESAMT: str = "Tore_Gesamt"

# --- DB Connection & SQL Loader ---
def get_db_engine() -> Optional[sqlalchemy.engine.Engine]:
    if not all([DB_NAME_PG, DB_USER_PG, DB_PASSWORD_PG, DB_HOST_PG, DB_PORT_PG]):
        logger.error("Unvollständige PostgreSQL-Verbindungsinformationen.")
        return None
    try:
        db_url = URL.create(
            drivername="postgresql+psycopg2", username=DB_USER_PG, password=DB_PASSWORD_PG,
            host=DB_HOST_PG.strip(), port=int(DB_PORT_PG), database=DB_NAME_PG
        )
        engine = sqlalchemy.create_engine(db_url, connect_args={'sslmode': 'require'})
        return engine
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der SQLAlchemy Engine: {e}", exc_info=True)
    return None

def get_db_connection() -> Optional[psycopg2.extensions.connection]:
    if not all([DB_NAME_PG, DB_USER_PG, DB_PASSWORD_PG, DB_HOST_PG, DB_PORT_PG]):
        logger.error("Unvollständige PostgreSQL-Verbindungsinformationen.")
        return None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME_PG, user=DB_USER_PG, password=DB_PASSWORD_PG,
            host=DB_HOST_PG.strip(), port=DB_PORT_PG, sslmode='require',
            cursor_factory=psycopg2.extras.DictCursor
        )
        return conn
    except Exception as e:
        logger.error(f"Fehler beim Herstellen der psycopg2-Verbindung: {e}", exc_info=True)
    return None

def load_sql(filename: str) -> str:
    path = os.path.join(SQL_DIR, filename)
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        logger.error(f"SQL file not found: {path}. Bitte erstelle diese Datei!")
        return "" 
    except Exception as e:
        logger.error(f"Error loading SQL file {path}: {e}")
        return ""

def execute_query(query_str: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
    engine = get_db_engine()
    if engine and query_str: 
        try:
            with engine.connect() as connection:
                df = pd.read_sql_query(sql=sqlalchemy.text(query_str), con=connection, params=params)
            return df
        except Exception as e:
            logger.error(f"Fehler bei SQL-Abfrage: {query_str[:100]}... | Fehler: {e}", exc_info=True)
        finally:
            engine.dispose()
    elif not query_str:
        logger.warning("Leere SQL-Abfrage erhalten, wahrscheinlich wurde die .sql-Datei nicht gefunden.")
    return pd.DataFrame()

# --- Refaktorierte Query-Funktionen ---

def fetch_all_leagues() -> pd.DataFrame:
    query = load_sql("fetch_all_leagues.sql")
    return execute_query(query)

def fetch_teams_for_league(league_id: str) -> pd.DataFrame:
    if not league_id: return pd.DataFrame()
    query = load_sql("fetch_teams_for_league.sql")
    return execute_query(query, params={'league_id': league_id})

def fetch_players_for_team(team_id: str) -> pd.DataFrame:
    if not team_id: return pd.DataFrame()
    query = load_sql("fetch_players_for_team.sql")
    return execute_query(query, params={'team_id': team_id})

def fetch_league_table(league_id: str, season: str) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_league_table.sql")
    return execute_query(query, params={'league_id': league_id, 'season': season})

def fetch_schedule_for_league(league_id: str, season: str) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_schedule_for_league.sql")
    df = execute_query(query, params={'league_id': league_id, 'season': season})
    if not df.empty and 'Spieldatum' in df.columns:
        df['Spieldatum'] = pd.to_datetime(df['Spieldatum'], format='%d.%m.%Y %H:%M', errors='coerce')
    return df

def fetch_player_season_stats(player_id: str, season: str) -> pd.DataFrame:
    if not player_id or not season: return pd.DataFrame()
    query = load_sql("fetch_player_season_stats.sql")
    # NEU: Konvertiere 'Wert'-Spalte zu String, falls die Abfrage eine solche erzeugt (was hier der Fall ist)
    df = execute_query(query, params={'player_id': player_id, 'season': season})
    # Die SQL gibt direkt Spalten zurück, keine Transponierung hier nötig
    return df


def fetch_player_game_log(player_id: str, season: str) -> pd.DataFrame:
    if not player_id or not season: return pd.DataFrame()
    query = load_sql("fetch_player_game_log.sql")
    df = execute_query(query, params={'player_id': player_id, 'season': season})
    if not df.empty and 'Spieldatum' in df.columns:
        df['Spieldatum'] = pd.to_datetime(df['Spieldatum'], format='%d.%m.%Y %H:%M', errors='coerce')
    return df

def fetch_game_details(game_id: str) -> Optional[Dict[str, Any]]:
    if not game_id: return None
    query = load_sql("fetch_game_details.sql")
    df = execute_query(query, params={'game_id': game_id})
    if not df.empty:
        details = df.iloc[0].to_dict()
        if details.get(COL_TORE_HEIM) is not None and details.get(COL_TORE_GAST) is not None:
            details['Ergebnis'] = f"{details[COL_TORE_HEIM]}:{details[COL_TORE_GAST]}"
        if details.get('Tore_Heim_HZ') is not None and details.get('Tore_Gast_HZ') is not None:
            details['Halbzeit'] = f"{details['Tore_Heim_HZ']}:{details['Tore_Gast_HZ']}"
        return details
    return None

def fetch_game_lineup(game_id: str, team_id: str) -> pd.DataFrame:
    if not game_id or not team_id: return pd.DataFrame()
    query = load_sql("fetch_game_lineup.sql")
    return execute_query(query, params={'game_id': game_id, 'team_id': team_id})

def fetch_game_events(game_id: str) -> pd.DataFrame:
    if not game_id: return pd.DataFrame()
    query = load_sql("fetch_game_events.sql")
    return execute_query(query, params={'game_id': game_id})

def fetch_player_all_time_stats(player_id: str) -> pd.DataFrame:
    if not player_id: return pd.DataFrame()
    query = load_sql("fetch_player_all_time_stats.sql")
    df = execute_query(query, params={'player_id': player_id})
    if not df.empty and "Spiele" in df.columns and df["Spiele"].iloc[0] > 0:
        df_transposed = df.T.reset_index().rename(columns={'index':'Statistik', 0:'Wert'})
        # NEU: Konvertiere 'Wert'-Spalte zu String
        df_transposed['Wert'] = df_transposed['Wert'].astype(str)
        return df_transposed
    return pd.DataFrame()

def fetch_player_stats_vs_opponent(player_id: str, opponent_team_id: str, season: Optional[str] = None) -> pd.DataFrame:
    if not player_id or not opponent_team_id: return pd.DataFrame()
    query = load_sql("fetch_player_stats_vs_opponent.sql")
    params = {'player_id': player_id, 'opponent_team_id': opponent_team_id}
    params['season'] = season if season and season != "Alle Saisons" else None
    df = execute_query(query, params=params)
    if not df.empty and "Spiele_gg_Gegner" in df.columns and df.iloc[0]['Spiele_gg_Gegner'] > 0:
        df_transposed = df.T.reset_index().rename(columns={'index':'Statistik', 0:'Wert'})
        # NEU: Konvertiere 'Wert'-Spalte zu String
        df_transposed['Wert'] = df_transposed['Wert'].astype(str)
        return df_transposed
    return pd.DataFrame()

def fetch_player_stats_in_game(player_id: str, game_id: str) -> pd.DataFrame:
    if not player_id or not game_id: return pd.DataFrame()
    query = load_sql("fetch_player_stats_in_game.sql")
    df = execute_query(query, params={'player_id': player_id, 'game_id': game_id})
    if not df.empty:
        df_transposed = df.T.reset_index().rename(columns={'index':'Statistik', 0:'Wert'})
        # NEU: Konvertiere 'Wert'-Spalte zu String
        df_transposed['Wert'] = df_transposed['Wert'].astype(str)
        return df_transposed
    return pd.DataFrame()

def fetch_all_teams_simple() -> pd.DataFrame:
    query = load_sql("fetch_all_teams_simple.sql")
    return execute_query(query)

def fetch_leagues_for_team(team_id: str) -> pd.DataFrame:
    if not team_id: return pd.DataFrame()
    query = load_sql("fetch_leagues_for_team.sql")
    return execute_query(query, params={'team_id': team_id})

def fetch_opponents_for_player(player_id: str, season: Optional[str] = None) -> pd.DataFrame:
    if not player_id: return pd.DataFrame()
    query = load_sql("fetch_opponents_for_player.sql")
    params = {'player_id': player_id}
    params['season'] = season if season and season != "Alle Saisons" else None
    return execute_query(query, params=params)

def fetch_points_progression_for_league(league_id: str, season: str) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_points_progression_for_league.sql")
    df = execute_query(query, params={'league_id': league_id, 'season': season})
    if not df.empty and 'Spieldatum' in df.columns:
        df['Spieldatum'] = pd.to_datetime(df['Spieldatum'], format='%d.%m.%Y %H:%M', errors='coerce')
    return df

def fetch_league_top_scorers(league_id: str, season: str, limit: int = 10) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_league_top_scorers.sql")
    return execute_query(query, params={'league_id': league_id, 'season': season, 'limit': limit})

def fetch_league_penalty_leaders(league_id: str, season: str, penalty_column_name: str, column_alias: str, limit: int = 10) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    
    penalty_map = {
        "Zwei_Minuten_Strafen": "fetch_league_penalty_zwei_minuten_strafen.sql",
        "Gelbe_Karten": "fetch_league_penalty_gelbe_karten.sql",
        "Rote_Karten": "fetch_league_penalty_rote_karten.sql",
        "Blaue_Karten": "fetch_league_penalty_blaue_karten.sql"
    }
    
    sql_filename = penalty_map.get(penalty_column_name)
    if not sql_filename:
        logger.error(f"Ungültiger penalty_column_name: {penalty_column_name}")
        return pd.DataFrame()

    query = load_sql(sql_filename)
    df = execute_query(query, params={'league_id': league_id, 'season': season, 'limit': limit})
    # Umbenennung, falls der Alias in SQL-Datei nicht direkt `column_alias` ist
    # (Die SQL-Dateien sind so geschrieben, dass sie bereits den korrekten Alias haben)
    if df.columns[2] != column_alias and column_alias in penalty_map.get(penalty_column_name,""): # Kleiner Check
         df = df.rename(columns={df.columns[2]: column_alias})
    return df

def fetch_team_top_scorers(team_id: str, league_id: str, season: str, limit: int = 5) -> pd.DataFrame:
    if not team_id or not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_team_top_scorers.sql")
    return execute_query(query, params={'team_id': team_id, 'league_id': league_id, 'season': season, 'limit': limit})

def fetch_team_penalty_leaders(team_id: str, league_id: str, season: str, penalty_column_name: str, column_alias: str, limit: int = 5) -> pd.DataFrame:
    if not team_id or not league_id or not season: return pd.DataFrame()

    penalty_map = {
        "Zwei_Minuten_Strafen": "fetch_team_penalty_zwei_minuten_strafen.sql",
        "Gelbe_Karten": "fetch_team_penalty_gelbe_karten.sql",
        "Rote_Karten": "fetch_team_penalty_rote_karten.sql",
        "Blaue_Karten": "fetch_team_penalty_blaue_karten.sql"
    }

    sql_filename = penalty_map.get(penalty_column_name)
    if not sql_filename:
        logger.error(f"Ungültiger penalty_column_name für Team: {penalty_column_name}")
        return pd.DataFrame()

    query = load_sql(sql_filename)
    df = execute_query(query, params={'team_id': team_id, 'league_id': league_id, 'season': season, 'limit': limit})
    # Umbenennung wie oben, falls nötig
    if df.columns[1] != column_alias and column_alias in penalty_map.get(penalty_column_name,""):
         df = df.rename(columns={df.columns[1]: column_alias})
    return df

def fetch_league_home_away_balance(league_id: str, season: str) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_league_home_away_balance.sql")
    return execute_query(query, params={'league_id': league_id, 'season': season})

def fetch_league_average_goals(league_id: str, season: str) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_league_average_goals.sql")
    return execute_query(query, params={'league_id': league_id, 'season': season})

def fetch_team_performance_halves(team_id: str, league_id: str, season: str) -> pd.DataFrame:
    if not team_id or not league_id or not season: return pd.DataFrame()
    query = load_sql("fetch_team_performance_halves.sql")
    df = execute_query(query, params={'team_id': team_id, 'league_id': league_id, 'season': season})
    if not df.empty and pd.notna(df['Tore_HZ1_Erziehlt'].iloc[0]):
        df['Diff_HZ1'] = df['Tore_HZ1_Erziehlt'] - df['Tore_HZ1_Kassiert']
        df['Diff_HZ2'] = df['Tore_HZ2_Erziehlt'] - df['Tore_HZ2_Kassiert']
    else:
        return pd.DataFrame(columns=['Tore_HZ1_Erziehlt', 'Tore_HZ1_Kassiert', 'Tore_HZ2_Erziehlt', 'Tore_HZ2_Kassiert', 'Diff_HZ1', 'Diff_HZ2'])
    return df

def fetch_team_head_to_head_with_stats(team1_id: str, team2_id: str, league_id: Optional[str] = None, season: Optional[str] = None) -> Dict[str, Any]:
    results: Dict[str, Any] = {"spiele_df": pd.DataFrame(), "stats": {}}
    if not team1_id or not team2_id: return results
    
    query = load_sql("fetch_team_head_to_head_with_stats.sql")
    params = {
        'team1_id': team1_id, 'team2_id': team2_id,
        'league_id': league_id, 'season': season
    }
    df = execute_query(query, params=params)

    if not df.empty:
        if 'Spieldatum' in df.columns:
            df['Spieldatum'] = pd.to_datetime(df['Spieldatum'], format='%d.%m.%Y %H:%M', errors='coerce')
        df["Ergebnis"] = df["Tore_Heim_Spiel"].astype(str) + ':' + df["Tore_Gast_Spiel"].astype(str)
        results["spiele_df"] = df[["Spieldatum", "Heimteam", "Gastteam", "Ergebnis"]]

        team1_name_df = execute_query(f'SELECT "Name" FROM "Teams" WHERE "Team_ID" = :id', {'id': team1_id})
        team1_name = team1_name_df.iloc[0,0] if not team1_name_df.empty else "Team 1"
        
        siege_team1, siege_team2, unentschieden = 0, 0, 0
        tore_team1, tore_team2 = 0, 0

        for _, row in df.iterrows():
            is_team1_home = row["Heim_Team_ID_H2H"] == team1_id
            
            if row["Punkte_Heim"] == 2: siege_team1 += 1 if is_team1_home else 0; siege_team2 += 0 if is_team1_home else 1
            elif row["Punkte_Gast"] == 2: siege_team1 += 0 if is_team1_home else 1; siege_team2 += 1 if is_team1_home else 0
            elif row["Punkte_Heim"] == 1: unentschieden += 1

            tore_team1 += row["Tore_Heim_Spiel"] if is_team1_home else row["Tore_Gast_Spiel"]
            tore_team2 += row["Tore_Gast_Spiel"] if is_team1_home else row["Tore_Heim_Spiel"]

        results["stats"] = {
            "Siege_Team1": siege_team1, "Siege_Team2": siege_team2,
            "Unentschieden": unentschieden, "Torverhaeltnis": f"{tore_team1}:{tore_team2}"
        }
    return results

def fetch_player_goal_timing_stats(player_id: str, season: str) -> pd.DataFrame:
    if not player_id or not season: return pd.DataFrame()
    query = load_sql("fetch_player_goal_timing_stats.sql")
    return execute_query(query, params={'player_id': player_id, 'season': season})

def fetch_player_goal_contribution_to_team(player_id: str, team_id: str, league_id: str, season: str) -> pd.DataFrame:
    if not all([player_id, team_id, league_id, season]): return pd.DataFrame()

    player_query = load_sql("fetch_player_goals_for_contribution.sql")
    team_query = load_sql("fetch_team_goals_for_contribution.sql")

    params = {'player_id': player_id, 'team_id': team_id, 'league_id': league_id, 'season': season}
    
    df_player = execute_query(player_query, params)
    spieler_tore = df_player['Spieler_Tore'].iloc[0] if not df_player.empty else 0

    df_team = execute_query(team_query, params)
    team_gesamttore = df_team['Team_Gesamttore'].iloc[0] if not df_team.empty and pd.notna(df_team['Team_Gesamttore'].iloc[0]) else 0

    anteil = (spieler_tore / team_gesamttore * 100.0) if team_gesamttore > 0 else 0.0

    result_df = pd.DataFrame({
        'Statistik': ['Spieler Tore', 'Team Gesamttore (Saison)', 'Anteil Spieler an Teamtoren (%)'],
        'Wert': [spieler_tore, team_gesamttore, round(anteil, 2)]
    })
    # NEU: Konvertiere 'Wert'-Spalte zu String
    result_df['Wert'] = result_df['Wert'].astype(str)
    return result_df

def fetch_players_by_name_search(search_term: str, limit: int = 50) -> pd.DataFrame:
    engine = get_db_engine()
    if engine and search_term:
        try:
            search_words = search_term.lower().split()
            conditions = []
            params_dict: Dict[str, Any] = {'limit': limit}
            
            for i, word in enumerate(search_words):
                param_name_vor = f"search_word_vor_{i}"
                param_name_nach = f"search_word_nach_{i}"
                conditions.append(f"""(LOWER(s."Vorname") LIKE :{param_name_vor} OR 
                                     LOWER(s."Nachname") LIKE :{param_name_nach})""")
                params_dict[param_name_vor] = f"%{word}%"
                params_dict[param_name_nach] = f"%{word}%"

            if not conditions: return pd.DataFrame()
            where_clause = " AND ".join(conditions)

            query_str = f"""
                SELECT DISTINCT
                    s."Spieler_ID", s."Vorname", s."Nachname"
                FROM "Spieler" s
                WHERE s."Ist_Offizieller" = 0 AND ({where_clause})
                ORDER BY s."Nachname", s."Vorname"
                LIMIT :limit;
            """
            return execute_query(query_str, params=params_dict)
        except Exception as e:
            logger.error(f"Fehler bei Spielersuche '{search_term}': {e}", exc_info=True)
        finally: 
            if engine: engine.dispose() 
    return pd.DataFrame()

def fetch_basic_db_stats() -> Dict[str, int]:
    stats = {"ligen": 0, "teams": 0, "spiele": 0, "spieler": 0}
    engine = get_db_engine()
    if engine:
        try:
            with engine.connect() as connection:
                stats["ligen"] = pd.read_sql_query(sql=sqlalchemy.text(load_sql("fetch_count_ligen.sql")), con=connection).iloc[0,0]
                stats["teams"] = pd.read_sql_query(sql=sqlalchemy.text(load_sql("fetch_count_teams.sql")), con=connection).iloc[0,0]
                stats["spiele"] = pd.read_sql_query(sql=sqlalchemy.text(load_sql("fetch_count_spiele.sql")), con=connection).iloc[0,0]
                stats["spieler"] = pd.read_sql_query(sql=sqlalchemy.text(load_sql("fetch_count_spieler.sql")), con=connection).iloc[0,0]
        except Exception as e:
            logger.error(f"Fehler bei DB-Basisstatistiken: {e}", exc_info=True)
        finally:
            engine.dispose()
    return stats

def fetch_club_overview() -> pd.DataFrame:
    query = load_sql("fetch_club_overview.sql")
    return execute_query(query)

logger.info("db_queries_refactored.py module successfully loaded and all functions defined.")