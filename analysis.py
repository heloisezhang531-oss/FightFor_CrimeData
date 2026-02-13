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

def get_top_crime_types_stacked(engine, limit=10):
    """Fetches top N primary crime types, broken down by arrest status."""
    try:
        with engine.connect() as conn:
            # 1. Provide subquery for top types to avoid errors with limit inside IN clause
            # MySQL 5.7+ / TiDB usually supports this, but nested select is safer
            subquery = text(f"SELECT primary_type FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY primary_type ORDER BY COUNT(*) DESC LIMIT {limit}")
            top_types = [row[0] for row in conn.execute(subquery).fetchall()]
            
            if not top_types:
                return pd.DataFrame()
            
            # 2. Get breakdown for these types
            types_list = "', '".join([str(t).replace("'", "''") for t in top_types])
            query = text(f"""
                SELECT primary_type, arrest, COUNT(*) as count 
                FROM chicago_crimes 
                WHERE {DATE_FILTER} AND primary_type IN ('{types_list}')
                GROUP BY primary_type, arrest
                ORDER BY count DESC
            """)
            result = pd.read_sql(query, conn)
            
            # Ensure 'arrest' is a readable string label
            result['arrest'] = result['arrest'].astype(str).replace({'1': 'True', '0': 'False', 'true': 'True', 'false': 'False', 'True': 'True', 'False': 'False'})
            return result
    except Exception as e:
        st.error(f"Error fetching top crime types: {e}")
        return pd.DataFrame()

def get_top_locations_stacked(engine, limit=10):
    """Fetches top N locations, broken down by arrest status."""
    try:
        with engine.connect() as conn:
            # 1. Get top locations
            subquery = text(f"SELECT location_description FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY location_description ORDER BY COUNT(*) DESC LIMIT {limit}")
            top_locs = [row[0] for row in conn.execute(subquery).fetchall()]
            
            if not top_locs:
                return pd.DataFrame()

            # 2. Get breakdown
            locs_list = "', '".join([str(l).replace("'", "''") for l in top_locs])
            query = text(f"""
                SELECT location_description, arrest, COUNT(*) as count 
                FROM chicago_crimes 
                WHERE {DATE_FILTER} AND location_description IN ('{locs_list}')
                GROUP BY location_description, arrest
                ORDER BY count DESC
            """)
            result = pd.read_sql(query, conn)
            
             # Ensure 'arrest' is a readable string label
            result['arrest'] = result['arrest'].astype(str).replace({'1': 'True', '0': 'False', 'true': 'True', 'false': 'False', 'True': 'True', 'False': 'False'})
            return result
    except Exception as e:
        st.error(f"Error fetching top locations: {e}")
        return pd.DataFrame()

def get_crime_location_heatmap(engine, top_types, top_locations):
    """Fetches heatmap data for Top Crimes vs Top Locations."""
    try:
        if not top_types or not top_locations:
             return pd.DataFrame()
        
        # Safety for SQL IN clause
        types_list = "', '".join([str(t).replace("'", "''") for t in top_types])
        locs_list = "', '".join([str(l).replace("'", "''") for l in top_locations])
        
        with engine.connect() as conn:
             query = text(f"""
                SELECT primary_type, location_description, COUNT(*) as count
                FROM chicago_crimes
                WHERE {DATE_FILTER}
                AND primary_type IN ('{types_list}')
                AND location_description IN ('{locs_list}')
                GROUP BY primary_type, location_description
             """)
             df = pd.read_sql(query, conn)
             if df.empty:
                 return pd.DataFrame()
             
             heatmap = df.pivot(index='primary_type', columns='location_description', values='count').fillna(0)
             # Reorder to match input order if possible, roughly
             return heatmap
    except Exception as e:
        st.error(f"Error fetching crime-location heatmap: {e}")
        return pd.DataFrame()

def get_top_crime_types_yearly(engine, limit=10):
    """Fetches yearly counts for crime types. If limit is None, fetches all."""
    try:
        with engine.connect() as conn:
            # 1. Provide subquery for top types
            if limit:
                subquery = text(f"SELECT primary_type FROM chicago_crimes WHERE {DATE_FILTER} GROUP BY primary_type ORDER BY COUNT(*) DESC LIMIT {limit}")
                top_types_res = conn.execute(subquery).fetchall()
                top_types = [row[0] for row in top_types_res]
                
                if not top_types:
                    return pd.DataFrame()
                    
                types_list = "', '".join([str(t).replace("'", "''") for t in top_types])
                where_clause = f"AND primary_type IN ('{types_list}')"
            else:
                where_clause = ""

            # 2. Get yearly breakdown
            query = text(f"""
                SELECT year, primary_type, COUNT(*) as count
                FROM chicago_crimes
                WHERE {DATE_FILTER} {where_clause}
                GROUP BY year, primary_type
                ORDER BY primary_type, year
            """)
            result = pd.read_sql(query, conn)
            return result
    except Exception as e:
        st.error(f"Error fetching crime types yearly trends: {e}")
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

def draw_choropleth(engine, selected_year, limit=100000):
    """
    Draw choropleth map for crimes by community area.
    Now also includes Top 3 Crime Types for each area.
    """
    try:
        with engine.connect() as conn:
            # Fetch detailed counts by area and type
            query = f"""
                SELECT community_area, primary_type, COUNT(*) as type_count 
                FROM chicago_crimes 
                WHERE YEAR = {selected_year}
                AND community_area IS NOT NULL 
                GROUP BY community_area, primary_type
            """
            df_detail = pd.read_sql(text(query), conn)
            
            if df_detail.empty:
                return pd.DataFrame()

            # 1. Calculate Total count per area
            df_total = df_detail.groupby('community_area')['type_count'].sum().reset_index(name='crime_count')

            # 2. Identify Top 5 Crime Types per area with Counts
            # Sort by area (asc) and count (desc)
            df_detail = df_detail.sort_values(['community_area', 'type_count'], ascending=[True, False])
            
            # Take top 5
            df_top5 = df_detail.groupby('community_area').head(5)
            
            # Create formatted string like "Theft (500)<br>Battery (300)..."
            # Using <br> for HTML tooltip if supported, or comma separated
            df_top5['formatted'] = df_top5.apply(lambda x: f"{x['primary_type']} ({x['type_count']})", axis=1)
            
            df_str = df_top5.groupby('community_area')['formatted'].apply(lambda x: '<br>'.join(x)).reset_index(name='top_types')
            
            # 3. Merge results
            final_df = pd.merge(df_total, df_str, on='community_area', how='left')
            
            # Ensure proper type for GeoJSON matching
            final_df['community_area'] = final_df['community_area'].astype(int).astype(str)
            
            # Map Area Number to Name
            # Source: Chicago Data Portal / Wikipedia
            area_names = {
                '1': 'Rogers Park', '2': 'West Ridge', '3': 'Uptown', '4': 'Lincoln Square', '5': 'North Center',
                '6': 'Lake View', '7': 'Lincoln Park', '8': 'Near North Side', '9': 'Edison Park', '10': 'Norwood Park',
                '11': 'Jefferson Park', '12': 'Forest Glen', '13': 'North Park', '14': 'Albany Park', '15': 'Portage Park',
                '16': 'Irving Park', '17': 'Dunning', '18': 'Montclare', '19': 'Belmont Cragin', '20': 'Hermosa',
                '21': 'Avondale', '22': 'Logan Square', '23': 'Humboldt Park', '24': 'West Town', '25': 'Austin',
                '26': 'West Garfield Park', '27': 'East Garfield Park', '28': 'Near West Side', '29': 'North Lawndale', '30': 'South Lawndale',
                '31': 'Lower West Side', '32': 'Loop', '33': 'Near South Side', '34': 'Armour Square', '35': 'Douglas',
                '36': 'Oakland', '37': 'Fuller Park', '38': 'Grand Boulevard', '39': 'Kenwood', '40': 'Washington Park',
                '41': 'Hyde Park', '42': 'Woodlawn', '43': 'South Shore', '44': 'Chatham', '45': 'Avalon Park',
                '46': 'South Chicago', '47': 'Burnside', '48': 'Calumet Heights', '49': 'Roseland', '50': 'Pullman',
                '51': 'South Deering', '52': 'East Side', '53': 'West Pullman', '54': 'Riverdale', '55': 'Hegewisch',
                '56': 'Garfield Ridge', '57': 'archer Heights', '58': 'Brighton Park', '59': 'McKinley Park', '60': 'Bridgeport',
                '61': 'New City', '62': 'West Elsdon', '63': 'Gage Park', '64': 'Clearing', '65': 'West Lawn',
                '66': 'Chicago Lawn', '67': 'West Englewood', '68': 'Englewood', '69': 'Greater Grand Crossing', '70': 'Ashburn',
                '71': 'Auburn Gresham', '72': 'Beverly', '73': 'Washington Heights', '74': 'Mount Greenwood', '75': 'Morgan Park',
                '76': 'O\'Hare', '77': 'Edgewater'
            }
            final_df['community_name'] = final_df['community_area'].map(area_names).fillna('Unknown')
            
            return final_df

    except Exception as e:
        st.error(f"Error fetching choropleth data: {e}")
        return pd.DataFrame()