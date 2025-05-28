import streamlit as st

def init_session_state():
    """Initialisiert alle notwendigen Session State Variablen, falls sie nicht existieren."""
    defaults = {
        "admin_authenticated": False,
        "selected_base_league_name": None,
        "selected_saison_for_league": None,
        "selected_league_id": None,
        "selected_league_name_display": None,
        "selected_team_id": None,
        "selected_team_name": None,
        "selected_player_id": None,
        "selected_player_name": None,
        "selected_game_id": None,
        "club_search_term": "",
        "selected_club_aggregated_name": None,
        "player_search_term": "",
        "h2h_team1_id": None,
        "h2h_team2_id": None,
        "h2h_opponent_team_id": None,
        "player_context_team_id": None,
        "player_context_league_id": None,
        "player_context_season": None,
        "player_context_team_name": None,
        "came_from_team_analysis_for_game_details": False,
        "confirm_db_delete": False,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value