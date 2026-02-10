import streamlit as st
import pandas as pd
import plotly.express as px

from analysis import DATE_FILTER


def get_data(engine):
    # Get total records
    
    query = """
    select * from victim_offender_rel_analysis;
    """ 
    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
            return result
    except Exception as e:
        st.error(f"Error fetching total records: {e}")
        return 0
