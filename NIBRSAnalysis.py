import streamlit as st
import pandas as pd
from sqlalchemy import text
import plotly.express as px

from analysis import DATE_FILTER


def get_data(engine):
    # Get total records
    
    query = text("select * from victim_offender_rel_analysis;" )
    
    try:
        with engine.connect() as conn:
           df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error fetching total records: {e}")
        return pd.DataFrame()
