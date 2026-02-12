import streamlit as st
import pandas as pd
from sqlalchemy import text
import plotly.express as px

# Common filter construction
def _build_where_clause(age_range, selected_cats):
    conditions = []
    if age_range:
        conditions.append(f"age_num BETWEEN {age_range[0]} AND {age_range[1]}")
    if selected_cats:
        cats_list = "', '".join([c.replace("'", "''") for c in selected_cats]) # simple SQL injection prevention for names with quotes
        conditions.append(f"offense_category_name IN ('{cats_list}')")
    
    clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    return clause

def get_filter_metadata(engine):
    """Fetch distinct categories and age range for initial UI setup."""
    try:
        with engine.connect() as conn:
            # Optimized to get metadata only
            # Get min/max age
            age_query = text("SELECT MIN(age_num), MAX(age_num) FROM victim_offender_rel_analysis")
            min_age, max_age = conn.execute(age_query).fetchone()
            
            # Get unique categories
            cat_query = text("SELECT DISTINCT offense_category_name FROM victim_offender_rel_analysis ORDER BY offense_category_name")
            categories = [row[0] for row in conn.execute(cat_query).fetchall() if row[0]]
            
            return int(min_age) if min_age is not None else 0, int(max_age) if max_age is not None else 100, categories
    except Exception as e:
        st.error(f"Error fetching filter options: {e}")
        return 0, 100, []

def get_kpi_data(engine, age_range, selected_cats):
    """Fetch aggregated KPI metrics directly from DB."""
    where_clause = _build_where_clause(age_range, selected_cats)
    query = f"""
        SELECT 
            COUNT(*) as total_victims,
            SUM(CASE WHEN RELATIONSHIP_NAME IN ('Victim Was Boyfriend/Girlfriend', 'Victim Was Child', 'Victim Was Common-Law Spouse', 'Victim Was Spouse') THEN 1 ELSE 0 END) as domestic_cases,
            AVG(age_num) as avg_age
        FROM victim_offender_rel_analysis
        {where_clause}
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
            return result
    except Exception as e:
        st.error(f"Error fetching KPIs: {e}")
        return (0, 0, 0)

def get_demographics_data(engine, age_range, selected_cats):
    """Fetch age and gender distribution."""
    where_clause = _build_where_clause(age_range, selected_cats)
    query = f"""
        SELECT age_num, sex_code, COUNT(*) as count 
        FROM victim_offender_rel_analysis
        {where_clause}
        GROUP BY age_num, sex_code
        ORDER BY age_num
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        st.error(f"Error fetching demographics: {e}")
        return pd.DataFrame()

def get_relationship_data(engine, age_range, selected_cats, limit=10):
    """Fetch top victim-offender relationships."""
    where_clause = _build_where_clause(age_range, selected_cats)
    query = f"""
        SELECT RELATIONSHIP_NAME, COUNT(*) as count 
        FROM victim_offender_rel_analysis
        {where_clause}
        GROUP BY RELATIONSHIP_NAME
        ORDER BY count DESC
        LIMIT {limit}
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        st.error(f"Error fetching relationships: {e}")
        return pd.DataFrame()

def get_heatmap_data(engine, age_range, selected_cats):
    """Fetch activity vs offense category heatmap data."""
    where_clause = _build_where_clause(age_range, selected_cats)
    query = f"""
        SELECT victim_activity_at_incident, offense_category_name, COUNT(*) as count 
        FROM victim_offender_rel_analysis
        {where_clause}
        GROUP BY victim_activity_at_incident, offense_category_name
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        st.error(f"Error fetching heatmap data: {e}")
        return pd.DataFrame()

def get_raw_sample(engine, age_range, selected_cats, limit=100):
    """Fetch raw data sample."""
    where_clause = _build_where_clause(age_range, selected_cats)
    query = f"""
        SELECT * 
        FROM victim_offender_rel_analysis
        {where_clause}
        LIMIT {limit}
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(query), conn)
    except Exception as e:
        st.error(f"Error fetching raw sample: {e}")
        return pd.DataFrame()
