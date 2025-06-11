import streamlit as st
import pandas as pd
import os
import logging
import psycopg2
import time
from utils.state import init_session_state
import db_queries_refactored as db_queries
from utils.club_importer import get_all_game_ids_for_club

try:
    from fetch_html_game_ids import fetch_game_ids_from_html_page #
except ImportError:
    logging.warning("Modul 'fetch_html_game_ids.py' nicht gefunden.") #
    fetch_game_ids_from_html_page = None

try:
    # NEU: Importiere main_batched anstatt process_single_game
    from analyse_game_json import main_batched #
except ImportError:
    logging.warning("Modul 'analyse_game_json.py' oder Funktion 'main_batched' nicht gefunden.") #
    main_batched = None


# --- Logging & Init ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
logger = logging.getLogger(__name__)
init_session_state()

# --- Admin View ---
st.header("🛠️ Admin-Bereich")

# --- Authentifizierung ---
if not st.session_state.get("admin_authenticated", False):
    password = st.text_input("Admin-Passwort:", type="password", key="admin_password_input_page")
    
    ADMIN_PASSWORD = None
    try:
        if "HANDBALL_ADMIN_PASSWORD" in st.secrets:
            ADMIN_PASSWORD = st.secrets["HANDBALL_ADMIN_PASSWORD"]
            logger.info("Admin-Passwort aus Streamlit Secrets geladen.")
        else:
            ADMIN_PASSWORD = os.environ.get("HANDBALL_ADMIN_PASSWORD")
            if ADMIN_PASSWORD:
                logger.info("Admin-Passwort aus Umgebungsvariable geladen (nicht in st.secrets gefunden).")
            else:
                 logger.warning("Admin-Passwort weder in st.secrets noch als Umgebungsvariable HANDBALL_ADMIN_PASSWORD gefunden.")
    except Exception as e: 
        logger.warning(f"Fehler beim Zugriff auf st.secrets: {e}. Versuche Fallback auf Umgebungsvariable.")
        ADMIN_PASSWORD = os.environ.get("HANDBALL_ADMIN_PASSWORD")
        if ADMIN_PASSWORD:
            logger.info("Admin-Passwort aus Umgebungsvariable geladen (nach st.secrets Fehler).")
        else:
            logger.error("Admin-Passwort konnte nach st.secrets Fehler nicht aus Umgebungsvariable geladen werden.")

    if not ADMIN_PASSWORD: 
        st.error("Admin-Passwort nicht konfiguriert. Bitte setze es in Streamlit Secrets (secrets.toml) oder als Umgebungsvariable 'HANDBALL_ADMIN_PASSWORD'.")
        st.info("""
        Beispiel für `.streamlit/secrets.toml`:
        ```toml
        HANDBALL_ADMIN_PASSWORD = "dein_sicheres_passwort_hier"
        ```
        Oder setze die Umgebungsvariable `HANDBALL_ADMIN_PASSWORD`.
        """)
        st.stop()

    if st.button("Login", key="admin_login_btn_page"):
        if password == ADMIN_PASSWORD:
            st.session_state.admin_authenticated = True
            st.rerun()
        else:
            st.error("Falsches Passwort.")
    st.stop()

# --- Authenticated Content ---
st.success("Admin-Bereich erfolgreich entsperrt.")
if st.button("Admin Logout", key="admin_logout_btn_page"):
    st.session_state.admin_authenticated = False
    st.rerun()

st.markdown("---")

# --- Datenbank Management ---
st.subheader("Datenbank-Management")
if st.button("Gesamte Datenbank löschen (VORSICHT!)", key="admin_delete_db_btn_page", type="primary"):
    # Bestätigungslogik
    if 'confirm_db_delete_step' not in st.session_state:
        st.session_state.confirm_db_delete_step = 0

    if st.session_state.confirm_db_delete_step == 0:
        st.warning("Bist du absolut sicher, dass du alle Daten löschen möchtest? Dieser Vorgang kann nicht rückgängig gemacht werden.")
        st.session_state.confirm_db_delete_step = 1
        # Erneutes Ausführen, um den Button unten anzuzeigen (Workaround für Button-Status)
        # st.experimental_rerun() # Ältere Streamlit Version
        st.rerun()


    elif st.session_state.confirm_db_delete_step == 1:
        # Erneuter Klick auf denselben Button (der jetzt als Bestätigung dient)
        # Die UI wird nicht explizit sagen "klicke erneut", aber der State ist gesetzt.
        # Eine bessere UI wäre ein separater Bestätigungsbutton.
        # Für diese Logik: Zweiter Klick auf den "Löschen"-Button führt die Aktion aus.
        try:
            conn = db_queries.get_db_connection()
            if conn:
                cursor = conn.cursor()
                tables = ["Ereignisse", "Spiel_Kader_Statistiken", "Spiele", "Ligen", "Teams", "Spieler", "Hallen"]
                with st.spinner("Lösche Datenbank-Tabellen..."):
                    for table in tables:
                        logger.info(f"Leere Tabelle {table}...")
                        cursor.execute(f'TRUNCATE TABLE "{table}" RESTART IDENTITY CASCADE;')
                    conn.commit()
                cursor.close()
                conn.close()
                st.success("Alle Tabellen erfolgreich geleert.")
                st.cache_data.clear() 
                st.session_state.confirm_db_delete_step = 0 # Zurücksetzen für nächsten Versuch
                time.sleep(1)
                st.rerun()
            else:
                st.error("Keine DB-Verbindung für Löschvorgang.")
                st.session_state.confirm_db_delete_step = 0
        except Exception as e:
            st.error(f"Fehler beim Leeren der DB: {e}")
            logger.error(f"Fehler DB leeren: {e}", exc_info=True)
            if 'conn' in locals() and conn: conn.rollback()
            st.session_state.confirm_db_delete_step = 0 # Zurücksetzen
    # Wenn der Button nicht geklickt wurde, aber der State > 0 ist, zurücksetzen, falls User wegnavigiert
    elif st.session_state.confirm_db_delete_step > 0 :
         st.session_state.confirm_db_delete_step = 0


st.markdown("---")

# --- SQL Ausführung ---
st.subheader("SQL-Befehl ausführen")
st.warning("**VORSICHT:** Nur für erfahrene Benutzer! Kann Daten beschädigen oder löschen.")
sql_query_input = st.text_area("SQL-Befehl:", height=150, key="admin_sql_query_area_page")
if st.button("SQL ausführen", key="admin_execute_sql_btn_page"):
    if sql_query_input:
        conn_sql = None
        try:
            conn_sql = db_queries.get_db_connection()
            if conn_sql:
                cursor_sql = conn_sql.cursor()
                with st.spinner("Führe SQL aus..."):
                    cursor_sql.execute(sql_query_input)
                    if sql_query_input.strip().upper().startswith("SELECT"):
                        results = cursor_sql.fetchall()
                        if results:
                            cols = [desc[0] for desc in cursor_sql.description]
                            df_results = pd.DataFrame(results, columns=cols)
                            st.dataframe(df_results)
                            st.success(f"{len(results)} Zeilen zurückgegeben.")
                        else:
                            st.info("Abfrage erfolgreich, keine Zeilen zurückgegeben.")
                    else:
                        conn_sql.commit()
                        st.success("SQL-Befehl erfolgreich ausgeführt (COMMIT).")
                st.cache_data.clear() 
        except psycopg2.Error as e_sql:
            st.error(f"SQL-Fehler: {e_sql}")
            logger.error(f"SQL-Fehler Admin: {e_sql}", exc_info=True)
            if conn_sql: conn_sql.rollback()
        except Exception as e_gen_sql:
            st.error(f"Allgemeiner Fehler bei SQL-Ausführung: {e_gen_sql}")
            logger.error(f"Allg. Fehler Admin-SQL: {e_gen_sql}", exc_info=True)
            if conn_sql: conn_sql.rollback()
        finally:
            if 'cursor_sql' in locals() and cursor_sql: cursor_sql.close()
            if conn_sql: conn_sql.close()
    else:
        st.warning("Bitte SQL-Befehl eingeben.")

st.markdown("---")

# --- Daten Import ---
st.subheader("Einzelne Liga von URL hinzufügen")
league_url_input = st.text_input("Spielplan-URL:", key="admin_league_url_input_page", placeholder="z.B. https://www.handball.net/ligen/...")
id_prefix_input = st.text_input("ID-Präfix (z.B. handball4all.westfalen.):", key="admin_id_prefix_input", placeholder="Wird meist aus URL extrahiert")
batch_size_input = st.number_input("Batch-Größe für Import:", min_value=1, max_value=100, value=20, step=1, key="admin_batch_size_input")


if st.button("Liga-Daten importieren", key="admin_add_league_btn_page"):
    if not league_url_input:
        st.warning("Bitte URL eingeben.")
    # NEU: Überprüfe main_batched
    elif fetch_game_ids_from_html_page is None or main_batched is None:
         st.error("Importfunktionen (fetch_html_game_ids oder analyse_game_json.main_batched) nicht verfügbar.")
    else:
        prefix_to_use_import = id_prefix_input
        if not prefix_to_use_import:
            import re
            match = re.search(r"handball\.net/ligen/((?:handball4all\.[^./]+)\.)", league_url_input)
            if match:
                prefix_to_use_import = match.group(1)
                st.info(f"ID-Präfix '{prefix_to_use_import}' aus URL extrahiert.")
            else:
                st.error("ID-Präfix konnte nicht aus URL extrahiert werden. Bitte manuell eingeben.")
                st.stop()
        
        status_text = st.empty() # Für Statusmeldungen während des Imports

        try:
            status_text.info(f"Extrahiere Spiel-IDs von {league_url_input}...")
            game_ids_import = fetch_game_ids_from_html_page(league_url_input, prefix_to_use_import) #

            if game_ids_import:
                status_text.info(f"{len(game_ids_import)} Spiel-IDs extrahiert. Starte Batch-Import (Batch-Größe: {batch_size_input})...")
                
                # NEU: Rufe main_batched auf
                with st.spinner(f"Importiere {len(game_ids_import)} Spiele in Batches... Dies kann einige Zeit dauern."):
                    # Die main_batched Funktion gibt nun ein Dictionary mit den Ergebnissen zurück
                    import_results = main_batched(game_ids_import, batch_size=batch_size_input) #
                
                if import_results:
                    success_count = import_results.get("success", 0)
                    error_count = import_results.get("error", 0)
                    total_count = import_results.get("total", len(game_ids_import))
                    
                    status_text.success(f"Import abgeschlossen: {success_count}/{total_count} erfolgreich, {error_count} fehlerhaft.")
                    if error_count > 0:
                        st.warning(f"Bei {error_count} Spielen gab es Probleme. Bitte überprüfe die Logs für Details.")
                else:
                    status_text.error("Der Batch-Import hat keine Ergebnisse zurückgegeben.")
                
                st.cache_data.clear() # Cache leeren nach Import

            else:
                status_text.warning("Keine Spiel-IDs von der URL extrahiert. Prüfe URL und Präfix.")
        except Exception as e_import:
            status_text.error(f"Fehler bei der URL-Verarbeitung oder dem Import: {e_import}")
            logger.error(f"Fehler URL {league_url_input}: {e_import}", exc_info=True)

st.markdown("---") # Trennlinie für die Übersichtlichkeit

# --- DATEN IMPORT (GANZER VEREIN) ---
st.subheader("Ganzen Verein von URL importieren")
club_url_input = st.text_input(
    "Vereins-URL:",
    key="admin_club_url_input_page",
    placeholder="z.B. https://www.handball.net/mannschaften/..."
)
club_id_prefix_input = st.text_input(
    "ID-Präfix für den Verein:",
    key="admin_club_id_prefix_input_page",
    value="handball4all.westfalen." # Beispiel-Präfix
)

if st.button("Vereins-Daten importieren", key="admin_add_club_btn_page", type="primary"):
    if not club_url_input or not club_id_prefix_input:
        st.warning("Bitte eine Vereins-URL und den zugehörigen ID-Präfix eingeben.")
    elif main_batched is None:
        st.error("Importfunktion (main_batched) nicht verfügbar.")
    else:
        try:
            # Schritt 1: Alle Spiel-IDs mit der neuen Helfer-Funktion sammeln.
            # Die Funktion gibt den Fortschritt direkt auf der Seite aus.
            final_game_ids_list = get_all_game_ids_for_club(club_url_input, club_id_prefix_input)

            if final_game_ids_list:
                # Schritt 2: Den bekannten Batch-Prozess mit allen gesammelten IDs ausführen
                with st.spinner(f"Importiere {len(final_game_ids_list)} Spiele... Dies kann einige Minuten dauern."):
                    import_results = main_batched(final_game_ids_list, batch_size=batch_size_input)

                if import_results:
                    success_count = import_results.get("success", 0)
                    error_count = import_results.get("error", 0)
                    st.success(f"Vereins-Import abgeschlossen: {success_count} erfolgreich, {error_count} fehlerhaft.")
                    if error_count > 0:
                        st.warning(f"Bei {error_count} Spielen gab es Probleme. Details siehe Server-Log.")
                else:
                    st.error("Der Batch-Import hat keine Ergebnisse zurückgegeben.")

                st.cache_data.clear()

        except Exception as e_club_import:
            st.error(f"Ein schwerwiegender Fehler ist beim Vereins-Import aufgetreten: {e_club_import}")
            logger.error(f"Fehler bei Vereins-Import von URL {club_url_input}: {e_club_import}", exc_info=True)