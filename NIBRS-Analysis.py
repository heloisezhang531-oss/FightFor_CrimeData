import streamlit as st
import pandas as pd
import plotly.express as px

from analysis import DATE_FILTER


def get_data(engine):
    # Get total records
    
    query = """
    SELECT 
        vic.age_num, vic.sex_code, 
        vt.VICTIM_TYPE_NAME,
        rel_type.RELATIONSHIP_NAME,
        act.ACTIVITY_TYPE_NAME,
        ofst.offense_category_name,
        inc.INCIDENT_DATE 
    FROM NIBRS_VICTIM vic 
    INNER JOIN NIBRS_INCIDENT inc ON vic.INCIDENT_ID = inc.INCIDENT_ID 
    LEFT JOIN NIBRS_VICTIM_TYPE vt ON vic.VICTIM_TYPE_ID = vt.VICTIM_TYPE_ID 
    LEFT JOIN NIBRS_VICTIM_OFFENDER_REL vor ON vic.VICTIM_ID = vor.VICTIM_ID 
    LEFT JOIN NIBRS_RELATIONSHIP rel_type ON vor.RELATIONSHIP_ID = rel_type.RELATIONSHIP_ID 
    LEFT JOIN NIBRS_ACTIVITY_TYPE act ON vic.ACTIVITY_TYPE_ID = act.ACTIVITY_TYPE_ID 
    LEFT JOIN nibrs_offense ofs ON inc.incident_id = ofs.incident_id 
    LEFT JOIN nibrs_offense_type ofst ON ofst.offense_type_id = ofs.offense_type_id 
    LIMIT 10000;
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(query).scalar()
            return result
    except Exception as e:
        st.error(f"Error fetching total records: {e}")
        return 0
