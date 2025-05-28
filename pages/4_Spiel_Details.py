import streamlit as st
import pandas as pd
import logging
from utils.state import init_session_state
from utils.cached_queries import (
    get_game_details_cached, get_game_lineup_cached, get_game_events_cached
)
from utils.ui import display_dataframe_with_title
import db_queries_refactored as db_queries

# --- Logging & Init ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
init_session_state()

# --- Seiteninhalt ---
st.header("📖 Spiel-Details")

if not st.session_state.get('selected_game_id'):
    st.warning("Kein Spiel ausgewählt. Bitte wähle ein Spiel aus der 'Ligen'- oder 'Vereine'-Ansicht.")
    st.stop()

game_id = st.session_state.selected_game_id
details = get_game_details_cached(game_id)

if not details:
    st.error(f"Spieldetails für ID {game_id} nicht geladen.")
    if st.button("Zurück zur Ligen-Ansicht"):
        st.session_state.selected_game_id = None
        st.switch_page("pages/1_Ligen.py") # NEU: Direkt wechseln
    st.stop()

# Back button logic - NEU: Mit st.switch_page
if st.session_state.get("came_from_team_analysis_for_game_details", False):
    if st.button("⬅️ Zurück zur Team-Ansicht"):
        st.session_state.selected_game_id = None
        st.switch_page("pages/2_Vereine.py") # Wechsle zur Vereine-Seite (zeigt Team-Details)
else:
    if st.button("⬅️ Zurück zur Ligen-Ansicht"):
        st.session_state.selected_game_id = None
        st.switch_page("pages/1_Ligen.py") # Wechsle zur Ligen-Seite

# ... (Restlicher Code für Spiel-Details wie zuvor) ...
st.subheader(f"{details.get('Heimteam', 'N/A')} vs {details.get('Gastteam', 'N/A')}")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Ergebnis", details.get('Ergebnis', 'N/A'))
col2.metric("Halbzeit", details.get('Halbzeit', 'N/A'))
col3.metric("Datum", details.get('Datum', 'N/A').split(" ")[0] if details.get('Datum') else "N/A")
col4.metric("Uhrzeit", details.get('Datum', 'N/A').split(" ")[1] if details.get('Datum') and " " in details.get('Datum') else "N/A")

with st.expander("Weitere Spieldetails"):
    st.markdown(f"**Liga:** {details.get('Liga_Name', 'N/A')}")
    st.markdown(f"**Halle:** {details.get('Halle', 'N/A')} ({details.get('Hallen_Stadt', 'N/A')})")
    st.markdown(f"**Schiedsrichter:** {details.get('SchiedsrichterInfo', 'N/A')}")
    if details.get('PDF_URL'):
        st.link_button("Spielbericht (PDF)", details['PDF_URL'])

tab_aufstellung, tab_events, tab_verlauf = st.tabs(["Aufstellungen & Stats", "Ereignis-Ticker", "Spielverlauf (Score)"])

with tab_aufstellung:
    heim_team_id = details.get('Heim_Team_ID')
    gast_team_id = details.get('Gast_Team_ID')
    col_h, col_g = st.columns(2)
    if heim_team_id:
        with col_h:
            display_dataframe_with_title(
                f"{details.get('Heimteam', 'Heim')}",
                get_game_lineup_cached(game_id, heim_team_id),
                remove_cols=[db_queries.COL_SPIELER_ID] # ID ausblenden
            )
    if gast_team_id:
        with col_g:
            display_dataframe_with_title(
                f"{details.get('Gastteam', 'Gast')}",
                get_game_lineup_cached(game_id, gast_team_id),
                remove_cols=[db_queries.COL_SPIELER_ID] # ID ausblenden
            )

with tab_events:
    display_dataframe_with_title("Ereignis-Ticker", get_game_events_cached(game_id), use_container_width=True)

with tab_verlauf:
    st.markdown("#### Spielverlaufsgrafik (Score-Worm)")
    events_for_chart_df = get_game_events_cached(game_id)
    if not events_for_chart_df.empty and 'Score_Heim' in events_for_chart_df.columns and 'Score_Gast' in events_for_chart_df.columns:
        score_events_df = events_for_chart_df.dropna(subset=['Score_Heim', 'Score_Gast']).copy()
        if not score_events_df.empty:
            score_events_df['Score_Heim'] = score_events_df['Score_Heim'].astype(int)
            score_events_df['Score_Gast'] = score_events_df['Score_Gast'].astype(int)
            if not (len(score_events_df) > 0 and score_events_df.iloc[0]['Score_Heim'] == 0 and score_events_df.iloc[0]['Score_Gast'] == 0):
                start_point = pd.DataFrame({'Score_Heim': [0], 'Score_Gast': [0]})
                score_events_df = pd.concat([start_point, score_events_df], ignore_index=True)

            score_chart_data = pd.DataFrame({
                details.get('Heimteam','Heim'): score_events_df['Score_Heim'],
                details.get('Gastteam','Gast'): score_events_df['Score_Gast']
            })
            st.line_chart(score_chart_data)
        else:
            st.info("Nicht genügend Daten für Spielverlaufsgrafik.")
    else:
        st.info("Keine Ereignisdaten für Spielverlaufsgrafik.")