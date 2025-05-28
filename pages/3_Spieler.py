import streamlit as st
import pandas as pd
import logging
from utils.state import init_session_state
from utils.cached_queries import (
    get_players_by_name_search_cached, get_leagues_cached,
    get_player_season_stats_cached, get_player_game_log_cached,
    get_player_goal_timing_stats_cached, get_opponents_for_player_cached,
    get_player_stats_vs_opponent_cached, get_player_all_time_stats_cached,
    get_player_goal_contribution_to_team_cached
)
from utils.ui import display_dataframe_with_title
import db_queries_refactored as db_queries #

# --- Logging & Init ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
init_session_state()

def show_player_detail_analysis():
    """Zeigt die Detailansicht für einen ausgewählten Spieler."""
    player_id = st.session_state.selected_player_id
    player_name = st.session_state.selected_player_name
    
    # Kontext, aus dem der Spieler aufgerufen wurde
    context_team_id = st.session_state.get("player_context_team_id") #
    context_team_name = st.session_state.get("player_context_team_name") #
    context_league_id = st.session_state.get("player_context_league_id") #
    context_season = st.session_state.get("player_context_season") #
    
    st.subheader(f"Detailanalyse für: {player_name}") #

    # NEU: Kontextsensitiver Zurück-Button
    if context_team_id and context_team_name: #
        if st.button(f"⬅️ Zurück zu Team: {context_team_name}", key="player_detail_back_to_team_btn"): #
            # Team-Kontext für die Vereine-Seite wiederherstellen
            st.session_state.selected_team_id = context_team_id #
            st.session_state.selected_team_name = context_team_name #
            st.session_state.selected_league_id = context_league_id #
            st.session_state.selected_saison_for_league = context_season #
            
            # Die Vereine-Seite wird den Base_League_Name und League_Name_Display
            # bei Bedarf aus den oben gesetzten IDs neu laden/ableiten.

            # Aktuelle Spieler-Auswahl und Spieler-Kontext zurücksetzen
            st.session_state.selected_player_id = None #
            st.session_state.selected_player_name = None #
            st.session_state.player_context_team_id = None #
            st.session_state.player_context_league_id = None #
            st.session_state.player_context_season = None #
            st.session_state.player_context_team_name = None #
            
            st.switch_page("pages/2_Vereine.py")
    else:
        if st.button("⬅️ Zurück zur Spielersuche", key="player_detail_back_to_search_btn"): #
            st.session_state.selected_player_id = None #
            st.session_state.selected_player_name = None #
            st.rerun() 

    if context_team_name: #
        st.caption(f"Kontext (falls über Team aufgerufen): Team {context_team_name}, Saison {context_season}") #

    all_leagues_df = get_leagues_cached() #
    saison_optionen_all = ["Alle Saisons"] + (sorted(list(all_leagues_df[db_queries.COL_SAISON].unique()), reverse=True) if not all_leagues_df.empty else ["2023/2024"]) #
    saison_optionen_specific = [s for s in saison_optionen_all if s != "Alle Saisons"] #
    if not saison_optionen_specific : saison_optionen_specific = sorted(list(all_leagues_df[db_queries.COL_SAISON].unique()), reverse=True) if not all_leagues_df.empty else ["2023/2024"] #


    tab_saison, tab_gamelog, tab_goal_timing, tab_vs_opp, tab_alltime, tab_contribution = st.tabs(
        ["Saison Gesamt", "Spiel-Log", "Torverteilung", "Gegen Gegner", "All-Time", "Team-Beitrag"]
    ) #

    with tab_saison: #
        default_saison_idx = 0 #
        if context_season and context_season in saison_optionen_specific: #
            try: default_saison_idx = saison_optionen_specific.index(context_season) #
            except ValueError: default_saison_idx = 0  #
        selected_s_season = st.selectbox("Saison für Gesamtstatistik:", saison_optionen_specific, index=default_saison_idx, key="player_stats_season_sel_page") #
        if selected_s_season: #
            stats_df = get_player_season_stats_cached(player_id, selected_s_season) #
            display_dataframe_with_title(f"Saisonstatistik {selected_s_season}", stats_df) #
            if not stats_df.empty and '7m_Quote_Prozent' in stats_df.columns and pd.notna(stats_df['7m_Quote_Prozent'].iloc[0]): #
                st.metric("7m-Quote (Saison)", f"{stats_df['7m_Quote_Prozent'].iloc[0]:.1f}%") #

    with tab_gamelog: #
        default_gl_idx = 0 #
        if context_season and context_season in saison_optionen_specific: #
            try: default_gl_idx = saison_optionen_specific.index(context_season) #
            except ValueError: default_gl_idx = 0 #
        selected_gl_season = st.selectbox("Saison für Spiel-Log:", saison_optionen_specific, index=default_gl_idx, key="player_gamelog_season_sel_page") #
        if selected_gl_season: #
            gamelog_df = get_player_game_log_cached(player_id, selected_gl_season) #
            if not gamelog_df.empty: #
                df_display = gamelog_df.copy() #
                if 'Spieldatum' in df_display.columns: #
                     df_display['Spieldatum_Anzeige'] = pd.to_datetime(df_display['Spieldatum']).dt.strftime('%d.%m.%Y %H:%M') #
                     df_display = df_display.drop(columns=['Spieldatum']) #
                     df_display = df_display.rename(columns={'Spieldatum_Anzeige': 'Spieldatum'}) #
                display_dataframe_with_title(f"Spiel-Log {selected_gl_season}", df_display, remove_cols=[db_queries.COL_SPIEL_ID]) #
                if db_queries.COL_TORE_GESAMT in gamelog_df.columns and 'Spieldatum' in gamelog_df.columns: #
                    # Ensure original 'Spieldatum' (datetime) is used for charting
                    form_data = gamelog_df[['Spieldatum', db_queries.COL_TORE_GESAMT]].copy() 
                    form_data['Spieldatum'] = pd.to_datetime(form_data['Spieldatum'], errors='coerce') 
                    form_data = form_data.sort_values(by='Spieldatum').dropna(subset=['Spieldatum']) 
                    if not form_data.empty: st.line_chart(form_data.set_index('Spieldatum')[db_queries.COL_TORE_GESAMT]) #

    with tab_goal_timing: #
        default_timing_idx = 0 #
        if context_season and context_season in saison_optionen_specific: #
            try: default_timing_idx = saison_optionen_specific.index(context_season) #
            except ValueError: default_timing_idx = 0 #
        selected_timing_season = st.selectbox("Saison für Torverteilung:", saison_optionen_specific, index=default_timing_idx, key="player_goaltiming_season_sel_page") #
        if selected_timing_season: #
            goal_timing_df = get_player_goal_timing_stats_cached(player_id, selected_timing_season) #
            if not goal_timing_df.empty and 'Spielminute' in goal_timing_df.columns and 'Anzahl_Tore' in goal_timing_df.columns: #
                all_minutes_df = pd.DataFrame({'Spielminute': range(1, 61)}) #
                merged_df = pd.merge(all_minutes_df, goal_timing_df, on='Spielminute', how='left').fillna(0) #
                merged_df['Anzahl_Tore'] = merged_df['Anzahl_Tore'].astype(int) #
                st.bar_chart(merged_df.set_index('Spielminute')['Anzahl_Tore']) #
            else: #
                st.info(f"Keine Daten zur Torverteilung für Saison {selected_timing_season} verfügbar.") #


    with tab_vs_opp: #
        selected_vo_season = st.selectbox("Saison für Gegnerstatistik:", saison_optionen_all, key="player_vs_opp_season_sel_page") #
        season_filter = selected_vo_season if selected_vo_season != "Alle Saisons" else None #
        opponent_teams_df = get_opponents_for_player_cached(player_id, season=season_filter) #
        if not opponent_teams_df.empty: #
            opponent_options = {row[db_queries.COL_TEAM_ID]: row[db_queries.COL_NAME] for _, row in opponent_teams_df.iterrows()} #
            selected_opponent_id = st.selectbox("Gegner auswählen:", options=list(opponent_options.keys()), format_func=lambda x: opponent_options.get(x,x), key="player_opponent_sel_page") #
            if selected_opponent_id: #
                stats_vs_opp_df = get_player_stats_vs_opponent_cached(player_id, selected_opponent_id, season=season_filter) #
                display_dataframe_with_title(f"Statistik gegen {opponent_options.get(selected_opponent_id)}", stats_vs_opp_df) #
        else: #
            st.info(f"Keine Gegner für Spieler in Saison '{selected_vo_season}' gefunden.") #


    with tab_alltime: #
        all_time_stats_df = get_player_all_time_stats_cached(player_id) #
        display_dataframe_with_title("All-Time Statistiken", all_time_stats_df) #
        if not all_time_stats_df.empty and 'Statistik' in all_time_stats_df.columns : #
            quote_row = all_time_stats_df[all_time_stats_df['Statistik'] == '7m_Quote_Prozent'] #
            if not quote_row.empty and pd.notna(quote_row['Wert'].iloc[0]): #
                 try: #
                    st.metric("7m-Quote (All-Time)", f"{float(quote_row['Wert'].iloc[0]):.1f}%") #
                 except ValueError: #
                    st.metric("7m-Quote (All-Time)", quote_row['Wert'].iloc[0]) #


    with tab_contribution: #
        st.markdown("#### Toranteil am Team") #
        if context_team_id and context_league_id and context_season: #
            st.caption(f"Analyse basiert auf Kontext: Team {context_team_name or context_team_id}, Liga {context_league_id}, Saison {context_season}") #
            contribution_df = get_player_goal_contribution_to_team_cached(player_id, context_team_id, context_league_id, context_season) #
            display_dataframe_with_title("Toranteil", contribution_df) #
            if not contribution_df.empty and 'Wert' in contribution_df.columns: #
                 anteil_row = contribution_df[contribution_df['Statistik'] == 'Anteil Spieler an Teamtoren (%)'] #
                 if not anteil_row.empty: #
                    try: #
                        st.progress(int(float(anteil_row['Wert'].iloc[0])) / 100) #
                    except (ValueError, TypeError) as e_prog: #
                        logger.debug(f"Konnte Toranteil nicht als Progress darstellen: {anteil_row['Wert'].iloc[0]}, Fehler: {e_prog}") #
        else: #
            st.info("Für diese Ansicht muss der Spieler über einen Team-Kader ausgewählt worden sein, um den Toranteil im Teamkontext zu berechnen.") #


def show_players_search_view():
    """Zeigt die Suchseite für Spieler."""
    st.header("👤 Spieler-Analyse & -Suche") #
    search_term = st.text_input("Spielernamen suchen (min. 3 Zeichen):", value=st.session_state.player_search_term, key="player_search_input_page") #
    st.session_state.player_search_term = search_term 

    if len(search_term) >= 3: #
        matching_players_df = get_players_by_name_search_cached(search_term) #
        if not matching_players_df.empty: #
            st.markdown(f"**{len(matching_players_df)} passende Spieler gefunden:**") #
            for index, player_row in matching_players_df.iterrows(): #
                player_fullname = f"{player_row['Vorname']} {player_row['Nachname']}" #
                if st.button(player_fullname, key=f"player_search_btn_{player_row[db_queries.COL_SPIELER_ID]}_{index}", use_container_width=True): #
                    st.session_state.selected_player_id = player_row[db_queries.COL_SPIELER_ID] #
                    st.session_state.selected_player_name = player_fullname #
                    # Kontext zurücksetzen, da von globaler Suche kommend
                    st.session_state.player_context_team_id = None #
                    st.session_state.player_context_league_id = None #
                    st.session_state.player_context_season = None #
                    st.session_state.player_context_team_name = None #
                    st.rerun() 
        else: #
            st.info(f"Keine Spieler passend zu '{search_term}' gefunden.") #
    elif search_term: #
        st.caption("Bitte mindestens 3 Zeichen eingeben.") #
    else: #
        st.info("Gib einen Namen ein oder wähle einen Spieler aus einem Team-Kader, um Details anzuzeigen.") #


# --- Main Logic ---
if st.session_state.get('selected_player_id'):
    show_player_detail_analysis()
else:
    show_players_search_view()