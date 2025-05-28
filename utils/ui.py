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