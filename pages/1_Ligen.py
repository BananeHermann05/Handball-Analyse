import streamlit as st
import pandas as pd
import re
import logging
from utils.state import init_session_state
from utils.cached_queries import (
    get_leagues_cached, get_teams_for_league_cached, get_league_table_cached,
    get_schedule_cached, get_points_progression_for_league_cached,
    get_league_top_scorers_cached, get_league_penalty_leaders_cached,
    get_league_home_away_balance_cached, get_league_average_goals_cached,
    get_team_head_to_head_with_stats_cached
)
from utils.ui import display_dataframe_with_title
import db_queries_refactored as db_queries

# --- Logging & Init ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
init_session_state()

# --- Hilfsfunktionen ---
def get_base_league_name_from_display(league_display_name_with_season: str) -> str:
    if not isinstance(league_display_name_with_season, str): return "N/A"
    match = re.match(r"^(.*)\s*\(\d{4}/\d{4}\)$", league_display_name_with_season)
    if match:
        return match.group(1).strip()
    return league_display_name_with_season

def set_team_and_switch(team_id, team_name, league_id, season, base_league_name):
    """Setzt den Team-Kontext und wechselt zur Vereins-Seite."""
    st.session_state.selected_team_id = team_id
    st.session_state.selected_team_name = team_name
    st.session_state.selected_league_id = league_id
    st.session_state.selected_saison_for_league = season
    st.session_state.selected_base_league_name = base_league_name
    # NEU: Wechsle direkt zur Seite
    st.switch_page("pages/2_Vereine.py")

def set_game_and_switch(game_id):
    """Setzt das Spiel und wechselt zur Spiel-Details-Seite."""
    st.session_state.selected_game_id = game_id
    st.session_state.came_from_team_analysis_for_game_details = False
    # NEU: Wechsle direkt zur Seite
    st.switch_page("pages/4_Spiel_Details.py")

# --- Seiteninhalt ---
st.header("🤾 Ligen-Analyse")
leagues_df = get_leagues_cached()
if leagues_df.empty:
    st.warning("Keine Ligadaten in der Datenbank gefunden. Bitte Daten importieren oder Datenbank-Verbindung prüfen.")
    st.stop()

base_league_name_col = 'Base_League_Name'
name_col = db_queries.COL_NAME
saison_col = db_queries.COL_SAISON
liga_id_col = db_queries.COL_LIGA_ID

if base_league_name_col not in leagues_df.columns:
    if name_col in leagues_df.columns:
        leagues_df[base_league_name_col] = leagues_df[name_col].apply(get_base_league_name_from_display)
    else:
        st.error(f"Benötigte Spalte '{name_col}' nicht im Ligen-DataFrame gefunden.")
        st.stop()

unique_base_leagues = sorted(leagues_df[base_league_name_col].unique())
if not unique_base_leagues:
    st.warning("Keine Basis-Liganamen gefunden.")
    st.stop()

st.sidebar.subheader("Ligaauswahl")
try:
    base_league_idx = unique_base_leagues.index(st.session_state.selected_base_league_name) if st.session_state.selected_base_league_name in unique_base_leagues else 0
except ValueError:
    base_league_idx = 0

selected_base_league_name = st.sidebar.selectbox("Liga-Gruppe:", options=unique_base_leagues, index=base_league_idx, key="liga_gruppe_sb")
saisons_for_base_league = sorted(leagues_df[leagues_df[base_league_name_col] == selected_base_league_name][saison_col].unique(), reverse=True)
if not saisons_for_base_league:
    st.sidebar.warning("Keine Saisons für diese Liga-Gruppe.")
    st.stop()

try:
    saison_idx = saisons_for_base_league.index(st.session_state.selected_saison_for_league) if st.session_state.selected_saison_for_league in saisons_for_base_league else 0
except ValueError:
    saison_idx = 0

selected_saison_for_league = st.sidebar.selectbox("Saison:", options=saisons_for_base_league, index=saison_idx, key="liga_saison_sb")

if (selected_base_league_name != st.session_state.selected_base_league_name or
        selected_saison_for_league != st.session_state.selected_saison_for_league):
    st.session_state.selected_base_league_name = selected_base_league_name
    st.session_state.selected_saison_for_league = selected_saison_for_league
    final_selected_league_df = leagues_df[(leagues_df[base_league_name_col] == selected_base_league_name) & (leagues_df[saison_col] == selected_saison_for_league)]
    if not final_selected_league_df.empty:
        st.session_state.selected_league_id = final_selected_league_df[liga_id_col].iloc[0]
        st.session_state.selected_league_name_display = final_selected_league_df[name_col].iloc[0]
    else:
        st.session_state.selected_league_id = None
        st.session_state.selected_league_name_display = None
    st.rerun()

if not st.session_state.selected_league_id:
    st.error(f"Keine Liga für '{selected_base_league_name}' in Saison '{selected_saison_for_league}'.")
    st.stop()

st.subheader(f"{st.session_state.selected_league_name_display}")

tab_titles = ["Tabelle", "Spielplan", "Ranglisten", "Liga-Statistiken", "Punkteverlauf", "Teamvergleich H2H"]
tab_table, tab_schedule, tab_leaderboards, tab_league_stats, tab_points_prog, tab_h2h = st.tabs(tab_titles)

with tab_table:
    league_table_df = get_league_table_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league)
    if not league_table_df.empty:
        cols_to_display_table = [col for col in league_table_df.columns if col not in [db_queries.COL_TEAM_ID, f'"{db_queries.COL_TEAM_ID}"']]
        st.dataframe(league_table_df[cols_to_display_table].style.set_properties(**{'text-align': 'left'}), hide_index=True, use_container_width=True)

        for index, row in league_table_df.iterrows():
            team_name_val = row.get("Team", row.get('"Team"'))
            team_id_val = row.get(db_queries.COL_TEAM_ID, row.get(f'"{db_queries.COL_TEAM_ID}"'))
            if team_name_val and team_id_val:
                if st.button(f"Details zu {team_name_val}", key=f"league_team_btn_{team_id_val}_{index}"):
                    set_team_and_switch(
                        team_id_val, team_name_val,
                        st.session_state.selected_league_id,
                        st.session_state.selected_saison_for_league,
                        st.session_state.selected_base_league_name
                    )
    else:
        st.info("Keine Tabellendaten für die ausgewählte Liga und Saison.")

with tab_schedule:
    schedule_df = get_schedule_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league)
    if not schedule_df.empty:
        for index, row in schedule_df.iterrows():
            # ... (Code für Spalten und Anzeige wie zuvor) ...
            heim_logo_url = row.get("Heim_Logo_URL", "")
            gast_logo_url = row.get("Gast_Logo_URL", "")
            heim_team_name = row.get("Heimteam", "N/A")
            gast_team_name = row.get("Gastteam", "N/A")
            ergebnis = row.get("Ergebnis", "vs")
            spieldatum = row['Spieldatum'].strftime('%d.%m.%Y %H:%M') if pd.notna(row['Spieldatum']) else "N/A"
            halle = row.get("Halle", "N/A")
            spiel_id = row[db_queries.COL_SPIEL_ID]

            st.markdown("---")
            cols = st.columns([1, 3, 1, 3, 2, 1])
            with cols[0]:
                if heim_logo_url and isinstance(heim_logo_url, str):
                    full_logo_url = f"https://www.handball.net/{heim_logo_url.replace('handball-net:', '')}" if heim_logo_url.startswith("handball-net:") else heim_logo_url
                    st.image(full_logo_url, width=35)
                else: st.write("🏠")
            with cols[1]: st.markdown(f"**{heim_team_name}**")
            with cols[2]: st.markdown(f"<div style='text-align: center; font-weight: bold;'>{ergebnis}</div>", unsafe_allow_html=True)
            with cols[3]: st.markdown(f"**{gast_team_name}**")
            with cols[4]:
                if gast_logo_url and isinstance(gast_logo_url, str):
                    full_logo_url = f"https://www.handball.net/{gast_logo_url.replace('handball-net:', '')}" if gast_logo_url.startswith("handball-net:") else gast_logo_url
                    st.image(full_logo_url, width=35)
                else: st.write("✈️")
            st.caption(f"{spieldatum} | {halle if halle else 'Halle nicht bekannt'}")
            with cols[5]:
                if st.button("Details", key=f"league_game_btn_{spiel_id}_{index}"):
                    set_game_and_switch(spiel_id) # NEU: Funktion verwenden
            # ... (Rest des Codes für Tabellen) ...
        st.markdown("---")
    else:
        st.info("Kein Spielplan für diese Liga und Saison.")

# ... (Restliche Tabs wie zuvor) ...
with tab_leaderboards:
    st.markdown("#### Liga-Ranglisten")
    col_scorer, col_penalty_2min, col_penalty_yellow, col_penalty_red = st.columns(4)
    with col_scorer: display_dataframe_with_title("Top Torschützen", get_league_top_scorers_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league))
    with col_penalty_2min: display_dataframe_with_title("Meiste 2-Minuten", get_league_penalty_leaders_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league, "Zwei_Minuten_Strafen", "2-Minuten"))
    with col_penalty_yellow: display_dataframe_with_title("Meiste Gelbe Karten", get_league_penalty_leaders_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league, "Gelbe_Karten", "Gelbe Karten"))
    with col_penalty_red: display_dataframe_with_title("Meiste Rote Karten", get_league_penalty_leaders_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league, "Rote_Karten", "Rote Karten"))

with tab_league_stats:
    st.markdown("#### Allgemeine Liga-Statistiken")
    balance_df = get_league_home_away_balance_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league)
    if not balance_df.empty and not balance_df["Gesamtspiele"].empty and pd.notna(balance_df["Gesamtspiele"].iloc[0]) and balance_df["Gesamtspiele"].iloc[0] > 0:
        col_hs, col_as, col_un, col_ges = st.columns(4); col_hs.metric("Heimsiege", int(balance_df["Heimsiege"].iloc[0])); col_as.metric("Auswärtssiege", int(balance_df["Auswärtssiege"].iloc[0])); col_un.metric("Unentschieden", int(balance_df["Unentschieden"].iloc[0])); col_ges.metric("Gesamtspiele gewertet", int(balance_df["Gesamtspiele"].iloc[0]))
    else: st.info("Keine Daten zur Heim-/Auswärtsbilanz.")
    avg_goals_df = get_league_average_goals_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league)
    if not avg_goals_df.empty:
        col_avg_ges, col_avg_h, col_avg_g = st.columns(3); col_avg_ges.metric("Ø Tore pro Spiel", f"{avg_goals_df['Avg_Gesamttore_pro_Spiel'].iloc[0]:.2f}" if pd.notna(avg_goals_df['Avg_Gesamttore_pro_Spiel'].iloc[0]) else "N/A"); col_avg_h.metric("Ø Heimtore", f"{avg_goals_df['Avg_Heimtore_pro_Spiel'].iloc[0]:.2f}" if pd.notna(avg_goals_df['Avg_Heimtore_pro_Spiel'].iloc[0]) else "N/A"); col_avg_g.metric("Ø Gasttore", f"{avg_goals_df['Avg_Gasttore_pro_Spiel'].iloc[0]:.2f}" if pd.notna(avg_goals_df['Avg_Gasttore_pro_Spiel'].iloc[0]) else "N/A")
    else: st.info("Keine Daten zu durchschnittlichen Toren.")

with tab_points_prog:
    st.markdown("#### Punkteverlauf der Teams")
    prog_data = get_points_progression_for_league_cached(st.session_state.selected_league_id, st.session_state.selected_saison_for_league)
    if not prog_data.empty and 'Team_Name' in prog_data.columns and 'Spiel_Nr' in prog_data.columns and 'Kumulierte_Punkte' in prog_data.columns:
        try: prog_pivot = prog_data.pivot_table(index='Spiel_Nr', columns='Team_Name', values='Kumulierte_Punkte').ffill().fillna(0); st.line_chart(prog_pivot)
        except Exception as e: st.error(f"Fehler: {e}"); logger.error(f"Fehler Pivot: {e}", exc_info=True)
    else: st.info("Keine Daten für Punkteverlauf.")

with tab_h2h:
    st.markdown("#### Direktvergleich zweier Teams")
    teams_in_league = get_teams_for_league_cached(st.session_state.selected_league_id)
    if not teams_in_league.empty:
        team_options = {row[db_queries.COL_TEAM_ID]: row[db_queries.COL_NAME] for index, row in teams_in_league.iterrows()}
        col1, col2 = st.columns(2)
        team1_id = col1.selectbox("Team 1:", options=list(team_options.keys()), format_func=lambda x: team_options.get(x, x), key="h2h_league_team1_sel")
        available_opponents = {tid: name for tid, name in team_options.items() if tid != team1_id}
        team2_id = col2.selectbox("Team 2:", options=list(available_opponents.keys()), format_func=lambda x: available_opponents.get(x,x), key="h2h_league_team2_sel")

        if team1_id and team2_id and team1_id != team2_id:
            h2h_data = get_team_head_to_head_with_stats_cached(team1_id, team2_id, st.session_state.selected_league_id, st.session_state.selected_saison_for_league)
            team1_name = team_options.get(team1_id, "Team 1")
            team2_name = team_options.get(team2_id, "Team 2")
            display_dataframe_with_title(f"Direktvergleich: {team1_name} vs {team2_name}", h2h_data["spiele_df"])
            if h2h_data["stats"]:
                stats = h2h_data["stats"]
                st.markdown(f"""
                **Gesamtstatistik (Liga/Saison):**
                - Siege {team1_name}: {stats.get('Siege_Team1',0)}
                - Siege {team2_name}: {stats.get('Siege_Team2',0)}
                - Unentschieden: {stats.get('Unentschieden',0)}
                - Torverhältnis ({team1_name} : {team2_name}): {stats.get('Torverhaeltnis', 'N/A')}
                """)
        elif team1_id == team2_id and team1_id is not None: st.warning("Bitte zwei unterschiedliche Teams wählen.")
    else: st.info("Keine Teams für Vergleich verfügbar.")