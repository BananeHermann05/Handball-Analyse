import streamlit as st
import pandas as pd
from typing import Optional, List, Dict, Any
import db_queries_refactored as db_queries # Importiere das refaktorierte Modul
import re

# --- Hilfsfunktionen ---
def get_base_league_name_from_display(league_display_name_with_season: str) -> str:
    if not isinstance(league_display_name_with_season, str): return "N/A"
    match = re.match(r"^(.*)\s*\(\d{4}/\d{4}\)$", league_display_name_with_season)
    if match:
        return match.group(1).strip()
    return league_display_name_with_season

# --- Gecachte Datenladefunktionen ---
@st.cache_data
def get_leagues_cached() -> pd.DataFrame:
    df = db_queries.fetch_all_leagues()
    if not df.empty:
        name_col = db_queries.COL_NAME
        if name_col in df.columns:
            df['Base_League_Name'] = df[name_col].apply(get_base_league_name_from_display)
        else:
            df['Base_League_Name'] = "Unbekannte Liga"
    return df

@st.cache_data
def get_teams_for_league_cached(league_id: Optional[str]) -> pd.DataFrame:
    if not league_id: return pd.DataFrame()
    return db_queries.fetch_teams_for_league(league_id)

@st.cache_data
def get_league_table_cached(league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_league_table(league_id, season)

@st.cache_data
def get_schedule_cached(league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_schedule_for_league(league_id, season)

@st.cache_data
def get_players_for_team_cached(team_id: Optional[str]) -> pd.DataFrame:
    if not team_id: return pd.DataFrame()
    return db_queries.fetch_players_for_team(team_id)

@st.cache_data
def get_player_season_stats_cached(player_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not player_id or not season: return pd.DataFrame()
    return db_queries.fetch_player_season_stats(player_id, season)

@st.cache_data
def get_player_game_log_cached(player_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not player_id or not season: return pd.DataFrame()
    return db_queries.fetch_player_game_log(player_id, season)

@st.cache_data
def get_game_details_cached(game_id: Optional[str]) -> Optional[Dict[str, Any]]:
    if not game_id: return None
    return db_queries.fetch_game_details(game_id)

@st.cache_data
def get_game_lineup_cached(game_id: Optional[str], team_id: Optional[str]) -> pd.DataFrame:
    if not game_id or not team_id: return pd.DataFrame()
    return db_queries.fetch_game_lineup(game_id, team_id)

@st.cache_data
def get_game_events_cached(game_id: Optional[str]) -> pd.DataFrame:
    if not game_id: return pd.DataFrame()
    return db_queries.fetch_game_events(game_id)

@st.cache_data
def get_player_all_time_stats_cached(player_id: Optional[str]) -> pd.DataFrame:
    if not player_id: return pd.DataFrame()
    return db_queries.fetch_player_all_time_stats(player_id)

@st.cache_data
def get_player_stats_vs_opponent_cached(player_id: Optional[str], opponent_team_id: Optional[str], season: Optional[str]=None) -> pd.DataFrame:
    if not player_id or not opponent_team_id: return pd.DataFrame()
    return db_queries.fetch_player_stats_vs_opponent(player_id, opponent_team_id, season)

@st.cache_data
def get_all_teams_simple_cached() -> pd.DataFrame:
    return db_queries.fetch_all_teams_simple()

@st.cache_data
def get_club_overview_cached() -> pd.DataFrame:
    return db_queries.fetch_club_overview()

@st.cache_data
def get_leagues_for_team_cached(team_id: Optional[str]) -> pd.DataFrame:
    if not team_id: return pd.DataFrame()
    df = db_queries.fetch_leagues_for_team(team_id)
    if not df.empty:
        name_col = db_queries.COL_NAME
        if name_col in df.columns:
            df['Base_League_Name'] = df[name_col].apply(get_base_league_name_from_display)
    return df

@st.cache_data
def get_opponents_for_player_cached(player_id: Optional[str], season: Optional[str]=None) -> pd.DataFrame:
    if not player_id: return pd.DataFrame()
    return db_queries.fetch_opponents_for_player(player_id, season)

@st.cache_data
def get_points_progression_for_league_cached(league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_points_progression_for_league(league_id, season)

@st.cache_data
def get_league_top_scorers_cached(league_id: Optional[str], season: Optional[str], limit: int = 10) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_league_top_scorers(league_id, season, limit)

@st.cache_data
def get_league_penalty_leaders_cached(league_id: Optional[str], season: Optional[str], penalty_column_name: str, column_alias: str, limit: int = 10) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_league_penalty_leaders(league_id, season, penalty_column_name, column_alias, limit)

@st.cache_data
def get_team_top_scorers_cached(team_id: Optional[str], league_id: Optional[str], season: Optional[str], limit: int = 5) -> pd.DataFrame:
    if not team_id or not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_team_top_scorers(team_id, league_id, season, limit)

@st.cache_data
def get_team_penalty_leaders_cached(team_id: Optional[str], league_id: Optional[str], season: Optional[str], penalty_column_name: str, column_alias: str, limit: int = 5) -> pd.DataFrame:
    if not team_id or not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_team_penalty_leaders(team_id, league_id, season, penalty_column_name, column_alias, limit)

@st.cache_data
def get_league_home_away_balance_cached(league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_league_home_away_balance(league_id, season)

@st.cache_data
def get_league_average_goals_cached(league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_league_average_goals(league_id, season)

@st.cache_data
def get_team_performance_halves_cached(team_id: Optional[str], league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not team_id or not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_team_performance_halves(team_id, league_id, season)

@st.cache_data
def get_player_goal_contribution_to_team_cached(player_id: Optional[str], team_id: Optional[str], league_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not player_id or not team_id or not league_id or not season: return pd.DataFrame()
    return db_queries.fetch_player_goal_contribution_to_team(player_id, team_id, league_id, season)

@st.cache_data
def get_team_head_to_head_with_stats_cached(team1_id: Optional[str], team2_id: Optional[str], league_id: Optional[str]=None, season: Optional[str]=None) -> Dict[str, Any]:
    if not team1_id or not team2_id: return {"spiele_df": pd.DataFrame(), "stats": {}}
    return db_queries.fetch_team_head_to_head_with_stats(team1_id, team2_id, league_id, season)

@st.cache_data
def get_player_goal_timing_stats_cached(player_id: Optional[str], season: Optional[str]) -> pd.DataFrame:
    if not player_id or not season: return pd.DataFrame()
    return db_queries.fetch_player_goal_timing_stats(player_id, season)

@st.cache_data
def get_players_by_name_search_cached(search_term: Optional[str]) -> pd.DataFrame:
    if not search_term or len(search_term) < 3:
        return pd.DataFrame()
    return db_queries.fetch_players_by_name_search(search_term)

@st.cache_data
def get_basic_db_stats_cached() -> Dict[str, int]:
    return db_queries.fetch_basic_db_stats()