import streamlit as st
import pandas as pd
import logging
from utils.state import init_session_state
from utils.cached_queries import (
    get_club_overview_cached, get_leagues_for_team_cached,
    get_all_teams_simple_cached, get_teams_for_league_cached,
    get_schedule_cached, get_players_for_team_cached,
    get_team_top_scorers_cached, get_team_penalty_leaders_cached,
    get_team_performance_halves_cached, get_team_head_to_head_with_stats_cached
)
from utils.ui import display_dataframe_with_title
import db_queries_refactored as db_queries

# --- Logging & Init ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
init_session_state()

# --- Hilfsfunktionen ---
def get_base_league_name_from_display(league_display_name_with_season: str) -> str:
    import re
    if not isinstance(league_display_name_with_season, str): return "N/A"
    match = re.match(r"^(.*)\s*\(\d{4}/\d{4}\)$", league_display_name_with_season)
    if match: return match.group(1).strip()
    return league_display_name_with_season

def set_player_and_switch(player_id, player_name, team_id, league_id, season, team_name):
    st.session_state.selected_player_id = player_id
    st.session_state.selected_player_name = player_name
    st.session_state.player_context_team_id = team_id
    st.session_state.player_context_league_id = league_id
    st.session_state.player_context_season = season
    st.session_state.player_context_team_name = team_name
    st.switch_page("pages/3_Spieler.py") # Ohne Emoji

def set_game_and_switch(game_id):
    st.session_state.selected_game_id = game_id
    st.session_state.came_from_team_analysis_for_game_details = True
    st.switch_page("pages/4_Spiel_Details.py") # Ohne Emoji

def show_team_analysis_view():
    team_id = st.session_state.selected_team_id
    team_name = st.session_state.selected_team_name

    if st.button("⬅️ Zurück zur Vereinsübersicht"):
        st.session_state.selected_team_id = None
        st.session_state.selected_team_name = None
        st.rerun()

    leagues_for_team_df = get_leagues_for_team_cached(team_id)
    if leagues_for_team_df.empty:
        st.error(f"Keine Ligazugehörigkeiten für Team '{team_name}'.")
        return

    st.sidebar.subheader(f"Kontext für {team_name}")
    if 'Base_League_Name' not in leagues_for_team_df.columns and db_queries.COL_NAME in leagues_for_team_df.columns:
        leagues_for_team_df['Base_League_Name'] = leagues_for_team_df[db_queries.COL_NAME].apply(get_base_league_name_from_display)
    elif 'Base_League_Name' not in leagues_for_team_df.columns:
         leagues_for_team_df['Base_League_Name'] = "Unbekannte Liga"

    league_options_dict = {
        f"{row.get('Base_League_Name', 'N/A')} ({row[db_queries.COL_SAISON]})": {
            "league_id": row[db_queries.COL_LIGA_ID],
            "base_league_name": row.get('Base_League_Name', 'N/A'),
            "saison": row[db_queries.COL_SAISON],
            "league_name_display": row[db_queries.COL_NAME]
        } for _, row in leagues_for_team_df.iterrows()
    }
    league_display_names = list(league_options_dict.keys())
    default_idx = 0
    current_base_league_name = st.session_state.get('selected_base_league_name', '')
    current_saison_for_league = st.session_state.get('selected_saison_for_league', '')
    current_display = f"{current_base_league_name} ({current_saison_for_league})"
    if current_display in league_display_names:
        default_idx = league_display_names.index(current_display)

    selected_league_display = st.sidebar.selectbox(
        "Liga & Saison für Teamkontext:",
        options=league_display_names,
        index=default_idx,
        key="team_context_select_page_vereine" # Eindeutiger Key
    )

    if selected_league_display:
        selected_context = league_options_dict[selected_league_display]
        if (selected_context["league_id"] != st.session_state.get('selected_league_id') or
            selected_context["saison"] != st.session_state.get('selected_saison_for_league')):
            st.session_state.selected_league_id = selected_context["league_id"]
            st.session_state.selected_base_league_name = selected_context["base_league_name"]
            st.session_state.selected_saison_for_league = selected_context["saison"]
            st.session_state.selected_league_name_display = selected_context["league_name_display"]
            st.rerun()
    
    current_league_id = st.session_state.get('selected_league_id')
    current_season = st.session_state.get('selected_saison_for_league')
    current_league_name_display = st.session_state.get('selected_league_name_display', "Unbekannte Liga")


    st.subheader(f"{team_name}")
    st.caption(f"Analyse im Kontext: {current_league_name_display} ({current_season if current_season else 'Keine Saison gewählt'})")

    tab_info, tab_games, tab_players, tab_team_stats, tab_h2h_team = st.tabs(["Info", "Spiele", "Kader", "Team-Statistiken", "Direktvergleich"])

    with tab_info:
        team_details_df_all = get_all_teams_simple_cached() # umbenannt, um Verwechslung zu vermeiden
        specific_team_info = team_details_df_all[team_details_df_all[db_queries.COL_TEAM_ID] == team_id]
        if not specific_team_info.empty:
            team_info_row = specific_team_info.iloc[0]
            st.markdown(f"**Akronym:** {team_info_row.get(db_queries.COL_AKRONYM, 'N/A')}")
            if current_league_id: 
                teams_in_current_league_df = get_teams_for_league_cached(current_league_id)
                team_logo_info = teams_in_current_league_df[teams_in_current_league_df[db_queries.COL_TEAM_ID] == team_id]
                if not team_logo_info.empty:
                    logo_url = team_logo_info.iloc[0].get(db_queries.COL_LOGO_URL)
                    if logo_url and isinstance(logo_url, str):
                        full_logo_url = f"https://www.handball.net/{logo_url.replace('handball-net:', '')}" if logo_url.startswith("handball-net:") else logo_url
                        try: st.image(full_logo_url, width=100, caption="Teamlogo")
                        except Exception as e: logger.warning(f"Logo nicht geladen: {e}")
        else:
            st.warning(f"Basisinformationen für Team {team_name} nicht gefunden.")


    with tab_games:
        if not current_league_id or not current_season:
            st.info("Bitte zuerst einen Liga-Kontext in der Seitenleiste auswählen.")
        else:
            schedule_df = get_schedule_cached(current_league_id, current_season)
            if not schedule_df.empty:
                team_games_df = schedule_df[(schedule_df['Heimteam'] == team_name) | (schedule_df['Gastteam'] == team_name)]
                if not team_games_df.empty:
                    st.markdown(f"**Spiele von {team_name} (Saison {current_season}):**")
                    for index, row in team_games_df.iterrows():
                        date_str = row['Spieldatum'].strftime('%d.%m.%Y %H:%M') if pd.notna(row['Spieldatum']) else "N/A"
                        label = f"{date_str}: {row['Heimteam']} {row['Ergebnis']} {row['Gastteam']}"
                        if st.button(label, key=f"team_game_btn_{row[db_queries.COL_SPIEL_ID]}_{index}", use_container_width=True):
                            set_game_and_switch(row[db_queries.COL_SPIEL_ID])
                else:
                    st.info(f"Keine Spiele für {team_name} in {current_league_name_display} ({current_season}) gefunden.")
            else:
                st.info(f"Kein Spielplan für die Liga {current_league_name_display} ({current_season}) geladen.")


    with tab_players:
        players_df = get_players_for_team_cached(team_id)
        if not players_df.empty:
            st.markdown(f"**Kader von {team_name} (Saison: {current_season if current_season else 'Alle Saisons'}):**")
            for index, row in players_df.iterrows():
                player_display = f"{row.get('Vorname','N/A')} {row.get('Nachname','N/A')}" + (f" ({row.get('Position','N/A')})" if pd.notna(row.get('Position')) else "")
                if st.button(player_display, key=f"team_player_btn_{row[db_queries.COL_SPIELER_ID]}_{index}", use_container_width=True):
                    set_player_and_switch(
                        row[db_queries.COL_SPIELER_ID],
                        f"{row.get('Vorname','N/A')} {row.get('Nachname','N/A')}",
                        team_id, current_league_id, current_season, team_name
                    )
        else:
            st.info(f"Kein Kader für {team_name} gefunden.")

    with tab_team_stats:
        if not current_league_id or not current_season:
            st.info("Bitte zuerst einen Liga-Kontext in der Seitenleiste auswählen, um Team-Statistiken anzuzeigen.")
        else:
            st.markdown("#### Team-Statistiken (intern, Saison)")
            col_ts, col_tp_2, col_tp_y, col_tp_r = st.columns(4)
            with col_ts: display_dataframe_with_title("Top Torschützen (Team)", get_team_top_scorers_cached(team_id, current_league_id, current_season))
            with col_tp_2: display_dataframe_with_title("Meiste 2-Min (Team)", get_team_penalty_leaders_cached(team_id, current_league_id, current_season, "Zwei_Minuten_Strafen", "2-Min"))
            with col_tp_y: display_dataframe_with_title("Meiste Gelbe K. (Team)", get_team_penalty_leaders_cached(team_id, current_league_id, current_season, "Gelbe_Karten", "Gelbe K."))
            with col_tp_r: display_dataframe_with_title("Meiste Rote K. (Team)", get_team_penalty_leaders_cached(team_id, current_league_id, current_season, "Rote_Karten", "Rote K."))
            st.markdown("---"); st.markdown("#### Halbzeit-Performance")
            halves_df = get_team_performance_halves_cached(team_id, current_league_id, current_season)
            if not halves_df.empty and not halves_df.isnull().all().all(): #
                col_h1, col_h2 = st.columns(2); col_h1.metric("Tordifferenz 1. HZ", int(halves_df["Diff_HZ1"].iloc[0]) if pd.notna(halves_df["Diff_HZ1"].iloc[0]) else "N/A"); col_h2.metric("Tordifferenz 2. HZ", int(halves_df["Diff_HZ2"].iloc[0]) if pd.notna(halves_df["Diff_HZ2"].iloc[0]) else "N/A") #
                with st.expander("Details Halbzeit-Tore"): display_dataframe_with_title("Halbzeit-Tore Details", halves_df) #
            else:
                st.info("Keine Daten zur Halbzeit-Performance verfügbar.")


    with tab_h2h_team:
        if not current_league_id:
            st.info("Bitte zuerst einen Liga-Kontext in der Seitenleiste auswählen, um H2H-Vergleiche zu ermöglichen.")
        else:
            st.markdown(f"#### Direktvergleich von {team_name}")
            teams_in_league = get_teams_for_league_cached(current_league_id)
            if not teams_in_league.empty:
                opponent_options = {row[db_queries.COL_TEAM_ID]: row[db_queries.COL_NAME] for _, row in teams_in_league.iterrows() if row[db_queries.COL_TEAM_ID] != team_id}
                if opponent_options:
                    opponent_id = st.selectbox("Gegner auswählen:", options=list(opponent_options.keys()), format_func=lambda x: opponent_options.get(x,x), key="h2h_team_opponent_sel_page_vereine") # Eindeutiger Key
                    if opponent_id:
                        h2h_data = get_team_head_to_head_with_stats_cached(team_id, opponent_id, current_league_id, current_season)
                        opponent_name = opponent_options.get(opponent_id, "Unbekannter Gegner")
                        display_dataframe_with_title(f"Direktvergleich: {team_name} vs {opponent_name}", h2h_data["spiele_df"])
                        if h2h_data["stats"]:
                            stats = h2h_data["stats"]
                            st.markdown(f"""
                            **Gesamtstatistik (Liga/Saison):**
                            - Siege {team_name}: {stats.get('Siege_Team1',0)}
                            - Siege {opponent_name}: {stats.get('Siege_Team2',0)}
                            - Unentschieden: {stats.get('Unentschieden',0)}
                            - Torverhältnis ({team_name} : {opponent_name}): {stats.get('Torverhaeltnis', 'N/A')}
                            """)
                else:
                    st.info("Keine anderen Teams in dieser Liga für einen Direktvergleich verfügbar.")
            else:
                st.info("Keine Teams in der ausgewählten Liga für einen Vergleich geladen.")


def show_clubs_list_view():
    """Zeigt die Such- und Übersichtsseite für Vereine."""
    st.header("🏢 Vereinsübersicht") #
    club_overview_df = get_club_overview_cached() #
    if club_overview_df.empty: #
        st.warning("Keine Vereinsdaten zum Gruppieren gefunden. Bitte Daten importieren.") #
        return

    search_term = st.text_input("Vereinsnamen suchen:", value=st.session_state.club_search_term, key="club_search_input_page_vereine") # Eindeutiger Key #
    st.session_state.club_search_term = search_term 

    if st.session_state.club_search_term:
        # .unique() gibt ein NumPy Array zurück, das .size hat
        filtered_clubs_data = club_overview_df[
            club_overview_df['Vereinsname_Aggregiert'].str.contains(st.session_state.club_search_term, case=False, na=False)
        ]['Vereinsname_Aggregiert'].unique()
    else:
        # sorted(unique()) gibt eine Liste zurück
        filtered_clubs_data = sorted(club_overview_df['Vereinsname_Aggregiert'].dropna().unique())
        st.caption(f"Zeige alle {len(filtered_clubs_data)} erkannten Vereine.")
    
    # KORRIGIERTE Fehlerbehandlung:
    # Prüfe, ob gesucht wurde UND die Ergebnisliste (egal ob Array oder Liste) leer ist
    if st.session_state.club_search_term and len(filtered_clubs_data) == 0: #
        st.info(f"Kein Verein passend zu '{st.session_state.club_search_term}' gefunden.") #
        return
    
    # Prüfe, ob NICHT gesucht wurde UND die Gesamtliste (vor der Suche) leer wäre
    # Dies ist relevant, falls club_overview_df initial leer ist und keine Suche stattfindet.
    if not st.session_state.club_search_term and len(filtered_clubs_data) == 0 and club_overview_df.empty:
        st.info("Keine Vereine in der Datenbank gefunden.")
        return

    for club_name in filtered_clubs_data:
        if not isinstance(club_name, str): # Sicherstellen, dass club_name ein String ist
            continue
        with st.expander(f"{club_name}", expanded=(st.session_state.selected_club_aggregated_name == club_name)): #
            teams_of_club_df = club_overview_df[club_overview_df['Vereinsname_Aggregiert'] == club_name] #
            if not teams_of_club_df.empty: #
                for altersgruppe, teams_in_agegroup in teams_of_club_df.groupby(db_queries.COL_ALTERSGRUPPE, dropna=False): #
                    altersgruppe_display = altersgruppe if pd.notna(altersgruppe) else 'Unbekannte Altersgruppe' #
                    st.markdown(f"**{altersgruppe_display}**") #
                    for index, team_row in teams_in_agegroup.sort_values(by="Team_Name").iterrows(): #
                        team_id = team_row[db_queries.COL_TEAM_ID] #
                        team_name_display = team_row["Team_Name"] #
                        liga_name_display = team_row.get("Liga_Name", "N/A") #
                        saison_display = team_row.get(db_queries.COL_SAISON, "N/A") #
                        
                        button_label = f"{team_name_display} ({liga_name_display} - {saison_display})" #
                        key_suffix = str(team_row.get(db_queries.COL_LIGA_ID, 'noliga')).replace('-', '_') # Eindeutigkeit verbessern
                        if st.button(button_label, key=f"club_team_btn_{team_id}_{index}_{key_suffix}", use_container_width=True): #
                            st.session_state.selected_team_id = team_id #
                            st.session_state.selected_team_name = team_name_display #
                            st.session_state.selected_league_id = team_row.get(db_queries.COL_LIGA_ID)  #
                            st.session_state.selected_saison_for_league = saison_display if saison_display != "N/A" else None #
                            st.session_state.selected_base_league_name = get_base_league_name_from_display(liga_name_display) if liga_name_display != "N/A" else None #
                            st.session_state.selected_league_name_display = liga_name_display if liga_name_display != "N/A" else None #
                            st.session_state.selected_club_aggregated_name = club_name 
                            st.rerun() 
            else: #
                st.info(f"Keine Teams für Verein '{club_name}' gefunden (sollte nicht passieren).") #

# --- Main Logic ---
if st.session_state.get('selected_team_id'):
    show_team_analysis_view()
else:
    show_clubs_list_view()