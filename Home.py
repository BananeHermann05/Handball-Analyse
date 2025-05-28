import streamlit as st
import os
import logging
from dotenv import load_dotenv
import db_queries_refactored as db_queries 
from utils.state import init_session_state
from utils.cached_queries import get_basic_db_stats_cached

# --- Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)

# --- App Configuration ---
st.set_page_config(layout="wide", page_title="Handball Analyse V2", initial_sidebar_state="expanded")

# --- Lade Umgebungsvariablen (für lokale Entwicklung) ---
try:
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
                logging.info(f"Umgebungsvariablen aus {loaded_env_file} geladen für Home.") #
                break
    if not loaded_env_file:
        logging.info(".env oder database.env Datei nicht gefunden. Streamlit Secrets oder System-Umgebungsvariablen werden verwendet.") #
except ImportError:
    logging.info("python-dotenv nicht installiert. Streamlit Secrets oder System-Umgebungsvariablen werden verwendet.") #

# --- Session State Initialisierung ---
init_session_state()

# --- Sidebar ---
st.sidebar.image("https://www.handball.net/img/handball-net-logo-sm.svg", width=150) #
st.sidebar.title("Handball Analyse V2") #
st.sidebar.markdown("Navigiere über die Seiten oben.")
if db_queries.DB_HOST_PG: #
     st.sidebar.caption(f"DB: Cloud PG ({db_queries.DB_HOST_PG})") #
else:
     st.sidebar.caption("DB: Konfiguration prüfen") #

# --- Hauptinhalt ---
st.title("Willkommen bei der Handball Analyse V2") #
st.markdown("""
Diese Anwendung dient zur Analyse von Handball-Spieldaten. 
Nutze die Navigation in der Seitenleiste oder die Suchfunktionen unten, um verschiedene Bereiche zu erkunden.
""") #

st.markdown("---") #
st.subheader("Direktsuche") #

# NEU: Direktsuche implementiert
col_club, col_player = st.columns(2) #
with col_club:
    home_club_search = st.text_input("Verein suchen:", key="home_club_search_main_page", placeholder="z.B. SG Handball Steinfurt") #
    if st.button("Verein finden", key="home_club_find_btn_page"): #
        if home_club_search:
            st.session_state.club_search_term = home_club_search #
            st.session_state.selected_team_id = None 
            st.session_state.selected_team_name = None
            st.switch_page("pages/2_Vereine.py")
        else:
            st.warning("Bitte einen Vereinsnamen eingeben.") #
with col_player:
    home_player_search = st.text_input("Spieler suchen:", key="home_player_search_main_page", placeholder="z.B. Felix 'The Goat' Leiß") #
    if st.button("Spieler finden", key="home_player_find_btn_page"): #
        if home_player_search:
            st.session_state.player_search_term = home_player_search #
            st.session_state.selected_player_id = None 
            st.session_state.selected_player_name = None
            st.switch_page("pages/3_Spieler.py")
        else:
            st.warning("Bitte einen Spielernamen eingeben.") #

st.markdown("---") #
st.subheader("Datenbank-Überblick") #
try:
    db_stats = get_basic_db_stats_cached() #
    if db_stats: #
        col1, col2, col3, col4 = st.columns(4) #
        col1.metric("Ligen in DB", db_stats.get("ligen", 0)) #
        col2.metric("Teams in DB", db_stats.get("teams", 0)) #
        col3.metric("Spiele in DB", db_stats.get("spiele", 0)) #
        col4.metric("Spieler in DB", db_stats.get("spieler", 0)) #
    else:
        st.warning("Keine Datenbankstatistiken verfügbar.") #
except Exception as e:
    st.error(f"Fehler beim Laden der Datenbank-Statistiken: {e}") #
    logger.error(f"Fehler DB-Statistiken Home: {e}", exc_info=True) #

st.markdown("---")
st.caption("Entwickelt für die Analyse von Handball-Daten von handball.net.")