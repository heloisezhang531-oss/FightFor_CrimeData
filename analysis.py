import pandas as pd
from sqlalchemy import Engine, text
import streamlit as st
import plotly.express as px
import requests
import json

# Common date filter clause
DATE_FILTER = "year >= 2015 AND year <= 2024"

def get_total_records(engine):
    """Fetches the total number of records in the chicago_crimes table for 2015-2024."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT COUNT(*) FROM chicago_crimes WHERE {DATE_FILTER}")
            result = conn.execute(query).scalar()
            return result
    except Exception as e:
        st.error(f"Error fetching total records: {e}")
        return 0

def get_missing_values_summary(engine):
    """
    Approximates missing values for key columns. 
    """
    try:
        cols = ['x_coordinate', 'y_coordinate', 'latitude', 'longitude', 'location', 'location_description', 'ward', 'district']
        
        # We only check missing values for the relevant period to be accurate
        selects = [f"COUNT(*) - COUNT({col}) as {col}_missing" for col in cols]
        select_str = ", ".join(selects)
        
        with engine.connect() as conn:
            query = text(f"SELECT COUNT(*) as total_rows, {select_str} FROM chicago_crimes WHERE {DATE_FILTER}")
            result = conn.execute(query).fetchone()
            
            if result:
                if hasattr(result, '_mapping'):
                    data = dict(result._mapping)
                else:
                    keys = result.keys()
                    data = {k: v for k, v in zip(keys, result)}
                
                total = data.pop('total_rows')
                
                summary_data = []
                for col_key, missing_count in data.items():
                    col_name = col_key.replace('_missing', '')
                    summary_data.append({
                        'Column': col_name,
                        'Missing Count': missing_count,
                        'Missing Rate (%)': round((missing_count / total * 100), 2) if total > 0 else 0
                    })
                
                return pd.DataFrame(summary_data).sort_values('Missing Rate (%)', ascending=False)
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error fetching missing values: {e}")
        return pd.DataFrame()

def get_arrest_domestic_stats(engine):
    """Fetches counts for Arrest and Domestic columns."""
    stats = {}
    try:
        with engine.connect() as conn:
            # Arrest Stats
            query_arrest = text(f"SELECT arrest, COUNT(*) as count FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY arrest")
            arrest_res = conn.execute(query_arrest).fetchall()
            stats['arrest'] = pd.DataFrame(arrest_res, columns=['Arrest', 'Count'])
            if not stats['arrest'].empty:
                stats['arrest']['Arrest'] = stats['arrest']['Arrest'].map({1: 'True', 0: 'False', 'true': 'True', 'false': 'False', True: 'True', False: 'False'})

            # Domestic Stats
            query_domestic = text(f"SELECT domestic, COUNT(*) as count FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY domestic")
            domestic_res = conn.execute(query_domestic).fetchall()
            stats['domestic'] = pd.DataFrame(domestic_res, columns=['Domestic', 'Count'])
            if not stats['domestic'].empty:
                stats['domestic']['Domestic'] = stats['domestic']['Domestic'].map({1: 'True', 0: 'False', 'true': 'True', 'false': 'False', True: 'True', False: 'False'})
                
        return stats
    except Exception as e:
        st.error(f"Error fetching arrest/domestic stats: {e}")
        return {'arrest': pd.DataFrame(), 'domestic': pd.DataFrame()}

def get_yearly_trends(engine):
    """Fetches crime counts grouped by year."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT year, COUNT(*) as count FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY year ORDER BY year")
            result = pd.read_sql(query, conn)
            return result
    except Exception as e:
        st.error(f"Error fetching yearly trends: {e}")
        return pd.DataFrame()

def get_monthly_trends(engine):
    """Fetches crime counts grouped by month (across all years)."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT EXTRACT(MONTH FROM date) as month, COUNT(*) as count FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY month ORDER BY month")
            result = pd.read_sql(query, conn)
            return result
    except Exception as e:
        st.error(f"Error fetching monthly trends: {e}")
        return pd.DataFrame()

def get_day_of_week_counts(engine):
     """Fetches crime counts grouped by Day of Week."""
     try:
        with engine.connect() as conn:
            query = text(f"""
                SELECT DAYOFWEEK(date) as day_num, COUNT(*) as count 
                FROM chicago_crimes 
                WHERE {DATE_FILTER}
                GROUP BY day_num
            """)
            df = pd.read_sql(query, conn)
            
            day_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
            df['Day'] = df['day_num'].map(day_map)
            
            # Sort Mon-Sun
            days_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            df = df.set_index('Day').reindex(days_order).reset_index()
            return df
     except Exception as e:
        st.error(f"Error fetching day of week trends: {e}")
        return pd.DataFrame()

def get_heatmap_data(engine):
    """Fetches crime counts grouped by Day of Week and Hour."""
    try:
        with engine.connect() as conn:
            query = text(f"""
                SELECT 
                    DAYOFWEEK(date) as day_of_week, 
                    HOUR(date) as hour, 
                    COUNT(*) as crime_count 
                FROM chicago_crimes 
                WHERE {DATE_FILTER}
                GROUP BY day_of_week, hour
            """)
            df = pd.read_sql(query, conn)
            
            day_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
            df['Day'] = df['day_of_week'].map(day_map)
            
            heatmap_data = df.pivot(index='Day', columns='hour', values='crime_count').fillna(0)
            days_order = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
            heatmap_data = heatmap_data.reindex(days_order)
            
            return heatmap_data
    except Exception as e:
        st.error(f"Error fetching heatmap data: {e}")
        return pd.DataFrame()

def get_top_crime_types(engine, limit=10):
    """Fetches top N primary crime types."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT primary_type, COUNT(*) as count FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY primary_type ORDER BY count DESC LIMIT {limit}")
            result = pd.read_sql(query, conn)
            return result
    except Exception as e:
        st.error(f"Error fetching top crime types: {e}")
        return pd.DataFrame()

def get_top_locations(engine, limit=10):
    """Fetches top N location descriptions."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT location_description, COUNT(*) as count FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY location_description ORDER BY count DESC LIMIT {limit}")
            result = pd.read_sql(query, conn)
            return result
    except Exception as e:
        st.error(f"Error fetching top locations: {e}")
        return pd.DataFrame()

def get_recent_data(engine, limit=1000):
    """Fetches a sample of recent data."""
    try:
        with engine.connect() as conn:
            # We assume user wants to see the latest data available, even if outside analysis range?
            # Or should we strictly show 2024 data?
            # Let's show recent data from the analyzed period (end of 2024)
            query = text(f"SELECT * FROM chicago_crimes WHERE {DATE_FILTER} ORDER BY date DESC LIMIT {limit}")
            result = pd.read_sql(query, conn)
            return result
    except Exception as e:
        st.error(f"Error fetching recent data: {e}")
        return pd.DataFrame()

def get_map_data(engine,selected_year, limit=100000):
    """Fetches latitude and longitude for a sample of crimes."""
    #Show data group by the years - zyh 2026.02.10
    try:
        with engine.connect() as conn:
            query = text(f"SELECT latitude, longitude FROM chicago_crimes WHERE YEAR = {selected_year} AND latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY date DESC LIMIT {limit}")
            result = pd.read_sql(query, conn)
            # Ensure numeric types for map
            result['latitude'] = pd.to_numeric(result['latitude'], errors='coerce')
            result['longitude'] = pd.to_numeric(result['longitude'], errors='coerce')
            result = result.dropna(subset=['latitude', 'longitude'])
            return result
    except Exception as e:
        st.error(f"Error fetching map data: {e}")
        return pd.DataFrame()
    

def get_geojson1():
    url = "https://data.cityofchicago.org/resource/igwz-8jzy.geojson"
    try:
        resp = requests.get(url)
        return resp.json()
    except Exception as e:
        st.error(f"GeoJSON Error: {e}")
        return None

def draw_choropleth(engine,selected_year,limit=100000):
    """draw choropleth map for crimes by community area."""
    try:
        with engine.connect() as conn:
            query = f"""
                SELECT community_area, COUNT(*) as crime_count 
                FROM chicago_crimes 
                WHERE YEAR = {selected_year}
                AND community_area IS NOT NULL 
                GROUP BY community_area
            """
            df = pd.read_sql(text(query), conn)
            df['community_area'] = df['community_area'].astype(str)
            return df
    except Exception as e:
        st.error(f"Error fetching choropleth data: {e}")
        return pd.DataFrame()
    if df.empty:
        st.warning("No data available for the selected year.")
        return pd.DataFrame()