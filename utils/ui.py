import streamlit as st
import pandas as pd
from typing import Optional, List

def display_dataframe_with_title(
    title: str,
    df: pd.DataFrame,
    hide_index: bool = True,
    use_container_width: bool = True,
    remove_cols: Optional[List[str]] = None
):
    """Zeigt einen Titel und ein DataFrame an, mit optionalem Spaltenentfernen."""
    st.markdown(f"##### {title}")
    if not df.empty:
        display_df = df.copy()
        if remove_cols:
            cols_to_drop = [col for col in remove_cols if col in display_df.columns]
            display_df = display_df.drop(columns=cols_to_drop)
        st.dataframe(display_df, hide_index=hide_index, use_container_width=use_container_width)
    else:
        st.info(f"Keine Daten für '{title}' verfügbar.")

# --- KORREKTUR: Die Funktion muss hier auf der obersten Ebene stehen ---
def translate_age_group(league_name: Optional[str], age_group_api: Optional[str]) -> str:
    """Übersetzt die englischen Jugendbezeichnungen ins Deutsche (mit m/w)."""
    if not isinstance(age_group_api, str):
        return age_group_api or "Senioren/Andere"
    if not isinstance(league_name, str):
        league_name = "" # Fallback für den Liganamen

    # Mapping von API-Wert zu deutscher Bezeichnung
    translations = {
        "AYouth": "A-Jugend", "BYouth": "B-Jugend", "CYouth": "C-Jugend",
        "DYouth": "D-Jugend", "EYouth": "E-Jugend", "FYouth": "F-Jugend",
        "Adults": "Senioren", "Mini": "Minis"
    }
    
    # Bestimme das Geschlecht aus dem Liganamen
    ln_lower = league_name.lower()
    prefix = ""
    if "männlich" in ln_lower or "herren" in ln_lower or "jungen" in ln_lower:
        prefix = "m"
    elif "weiblich" in ln_lower or "frauen" in ln_lower or "mädchen" in ln_lower:
        prefix = "w"
        
    # Standardübersetzung holen
    german_base = translations.get(age_group_api, age_group_api)
    
    # Kombinieren, wenn es eine Jugend ist
    if "Jugend" in german_base and prefix:
        return f"{prefix}{german_base}"
    
    return german_base
