import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import os
from dotenv import load_dotenv

# Import our analysis module
import analysis

# Load environment variables
load_dotenv()

# TiDB Connection Details
TIDB_USER = os.getenv("TIDB_USER")
TIDB_PASSWORD = os.getenv("TIDB_PASSWORD")
TIDB_HOST = os.getenv("TIDB_HOST")
TIDB_PORT = os.getenv("TIDB_PORT")
TID_CA_PATH = os.getenv("TID_CA_PATH")
TIDB_DB_NAME = os.getenv("TIDB_DB_NAME") or "Chicago_data"

@st.cache_resource
def get_db_connection():
    url = f"mysql+pymysql://{TIDB_USER}:{TIDB_PASSWORD}@{TIDB_HOST}:{TIDB_PORT}/{TIDB_DB_NAME}?ssl_ca={TID_CA_PATH}&ssl_verify_cert=true&ssl_verify_identity=true"
    engine = create_engine(url, pool_recycle=3600)
    return engine

def main():
    st.set_page_config(page_title="Chicago Crime Analysis (2015-2024)", page_icon="📊", layout="wide")
    
    st.title("📊 Chicago Crime Data Analysis (2015 - 2024)")
    st.markdown("""
        Comprehensive analysis of crime in Chicago over a decade. Data sourced from TiDB Cloud.
        **Time Range:** Jan 1, 2015 - Dec 31, 2024.
    """)

    try:
        engine = get_db_connection()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return

    # --- Tabs Layout ---
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Overview", 
        "Raw Data", 
        "Key Statistics", 
        "Temporal Trends", 
        "Categorical Analysis"
    ])

    # --- TAB 1: OVERVIEW ---
    with tab1:
        st.header("Dataset Overview")
        
        st.markdown("""
        ### 📌 Executive Summary
        Based on the descriptive statistical analysis of the 2015-2024 dataset, key findings include:
        *   **Crime Concentration:** The top 3 crimes (**Theft, Battery, Criminal Damage**) account for **~52%** of all reported incidents.
        *   **High-Risk Zones:** **Streets** are the most common location for crimes (~24%), followed by residences.
        *   **2020 Anomaly:** A significant drop in overall crime rates was observed in 2020 due to the pandemic, followed by a gradual recovery.
        """)
        
        col1, col2 = st.columns(2)
        with col1:
            with st.spinner("Fetching total record count..."):
                total_records = analysis.get_total_records(engine)
                st.metric("Total Crime Records (2015-2024)", f"{total_records:,}")
        
        with col2:
            st.info("Analyzing data from **2015 to 2024**")

        st.subheader("Missing Values Analysis")
        with st.spinner("Analyzing missing data..."):
            missing_df = analysis.get_missing_values_summary(engine)
            if not missing_df.empty:
                st.dataframe(missing_df, use_container_width=True)
                
                # Missing Values Plot (Color: coral as per request's theme feel, or just default)
                fig = px.bar(missing_df, x='Missing Rate (%)', y='Column', orientation='h', 
                             title="Missing Data Percentage by Column",
                             color='Missing Rate (%)', color_continuous_scale='RdYlGn_r')
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.write("No missing value data available.")

    # --- TAB 2: RAW DATA ---
    with tab2:
        st.header("Raw Data Sample (2015-2024)")
        st.write("Showing the most recent 1000 records from the analysis period.")
        
        with st.spinner("Fetching data..."):
            raw_df = analysis.get_recent_data(engine, limit=1000)
            if 'DATE' in raw_df.columns:
                 raw_df['DATE'] = pd.to_datetime(raw_df['DATE'])
            st.dataframe(raw_df, use_container_width=True)

    # --- TAB 3: KEY STATISTICS ---
    with tab3:
        st.header("Key Statistics")
        
        with st.spinner("Fetching breakdown..."):
            stats = analysis.get_arrest_domestic_stats(engine)
            arrest_counts = stats['arrest']
            domestic_counts = stats['domestic']

        col1, col2 = st.columns(2)
        
        # Colors from notebook: 
        # Arrest: #FF6B6B (True?), #4ECDC4 (False?) 
        # Note: Notebook label "Arrested" corresponded to True, "Not Arrested" to False
        # We need to ensure mapping is correct. 
        # Notebook: values=[True, False], colors=['#FF6B6B', '#4ECDC4']
        # Typically Red (#FF6B6B) is "Bad/Arrested"? Or "Arrested" count? 
        # Wait, usually "Arrested" is good for police stats? 
        # Let's check notebook logic: 
        # axes[0].pie(values, labels=labels, colors=['#FF6B6B', '#4ECDC4'])
        # labels=[Arrested, Not Arrested]
        # So Arrested -> #FF6B6B, Not Arrested -> #4ECDC4 
        
        colors_map = {'Arrested': '#FF6B6B', 'Not Arrested': '#4ECDC4', 
                      'True': '#FF6B6B', 'False': '#4ECDC4',
                      'Domestic': '#FF6B6B', 'Non-Domestic': '#4ECDC4'}

        with col1:
            st.subheader("Arrest Distribution")
            st.info("""
            **Analyst Insight:**
            *   **High Arrest Rates (>99%):** Gambling, Narcotics, Prostitution.
            *   **Low Arrest Rates (<10%):** Burglary, Motor Vehicle Theft, Robbery.
            *   **Seasonal Trend:** Arrest efficiency is slightly higher in winter months (Jan/Feb ~20%) compared to summer (~16%).
            """)
            if not arrest_counts.empty:
                 arrest_counts['Status'] = arrest_counts['Arrest'].apply(lambda x: 'Arrested' if x=='True' else 'Not Arrested')
                 # Enforce specific color mapping based on Status value
                 fig_arrest = px.pie(arrest_counts, values='Count', names='Status', 
                                     title="Distribution of Arrest Status",
                                     color='Status',
                                     color_discrete_map=colors_map)
                 st.plotly_chart(fig_arrest, use_container_width=True)
        
        with col2:
            st.subheader("Domestic Violence Distribution")
            st.info("""
            **Analyst Insight:**
            *   **Key Offenses:** Battery, Other Offense, and Assault are the primary contributors to domestic violence incidents.
            """)
            if not domestic_counts.empty:
                 domestic_counts['Type'] = domestic_counts['Domestic'].apply(lambda x: 'Domestic' if x=='True' else 'Non-Domestic')
                 # Notebook used same colors for Domestic vs No Domestic
                 fig_domestic = px.pie(domestic_counts, values='Count', names='Type', 
                                       title="Domestic Violence Incidents",
                                       color='Type',
                                       color_discrete_map=colors_map)
                 st.plotly_chart(fig_domestic, use_container_width=True)

    # --- TAB 4: TEMPORAL TRENDS ---
    with tab4:
        st.header("Temporal Trends")

        # Yearly Trend
        st.subheader("Annual Crime Trend (2015-2024)")
        with st.spinner("Loading yearly data..."):
            yearly_df = analysis.get_yearly_trends(engine)
            if not yearly_df.empty:
                 # Notebook: color='steelblue', marker='o'
                 fig_year = px.line(yearly_df, x='year', y='count', markers=True,
                                    title="Annual Number of Crime Cases",
                                    labels={'count': 'Number of Cases', 'year': 'Year'})
                 fig_year.update_traces(line_color='steelblue', marker=dict(size=8))
                 st.plotly_chart(fig_year, use_container_width=True)

        # Monthly Seasonality
        st.subheader("Monthly Distribution")
        with st.spinner("Loading monthly data..."):
            monthly_df = analysis.get_monthly_trends(engine)
            if not monthly_df.empty:
                 # Map month number to Name
                 import calendar
                 monthly_df['Month Name'] = monthly_df['month'].apply(lambda x: calendar.month_abbr[int(x)])
                 
                 # Notebook: color='coral'
                 fig_month = px.bar(monthly_df, x='Month Name', y='count',
                                    title="Monthly Distribution of Crime Cases",
                                    labels={'count': 'Number of Cases', 'Month Name': 'Month'})
                 fig_month.update_traces(marker_color='coral')
                 st.plotly_chart(fig_month, use_container_width=True)

        # Day of Week Distribution
        st.subheader("Day of Week Distribution")
        with st.spinner("Loading weekly data..."):
            dow_df = analysis.get_day_of_week_counts(engine)
            if not dow_df.empty:
                # Notebook colors: Weekdays (Mon-Fri) #FF6B6B (Red), Weekend (Sat-Sun) #4ECDC4 (Green)
                # Note: list order in notebook was Mon, Tue, Wed, Thu, Fri, Sat, Sun
                # colors = ['#FF6B6B']*5 + ['#4ECDC4']*2
                
                # We can create a color column
                dow_df['Color'] = dow_df['Day'].apply(lambda x: '#4ECDC4' if x in ['Sat', 'Sun'] else '#FF6B6B')
                
                fig_dow = go.Figure(data=[go.Bar(
                    x=dow_df['Day'],
                    y=dow_df['count'],
                    marker_color=dow_df['Color']
                )])
                fig_dow.update_layout(title="Crime Cases by Day of Week",
                                      xaxis_title="Day of Week",
                                      yaxis_title="Number of Cases")
                st.plotly_chart(fig_dow, use_container_width=True)

        # Heatmap
        st.subheader("Crime Heatmap: Hour vs Day")
        with st.spinner("Generating heatmap..."):
            heatmap_data = analysis.get_heatmap_data(engine)
            if not heatmap_data.empty:
                 fig_heat = px.imshow(heatmap_data, 
                                      labels=dict(x="Hour of Day", y="Day of Week", color="Crime Count"),
                                      title="Crime Heatmap: Hour vs Day of Week",
                                      aspect="auto",
                                      color_continuous_scale='Viridis') # Notebook used Viridis
                 st.plotly_chart(fig_heat, use_container_width=True)

    # --- TAB 5: CATEGORICAL ANALYSIS ---
    with tab5:
        st.header("Categorical Analysis")

        col1, col2 = st.columns(2)

        with col1:
             st.subheader("Top 10 Crime Types")
             with st.spinner("Fetching crime types..."):
                 top_crimes = analysis.get_top_crime_types(engine, limit=10)
                 if not top_crimes.empty:
                     # Default Plolty colors or steelblue for consistency
                     fig_type = px.bar(top_crimes, x='count', y='primary_type', orientation='h',
                                       title="Most Frequent Crime Types",
                                       labels={'count': 'Count', 'primary_type': 'Crime Type'})
                     fig_type.update_traces(marker_color='steelblue')
                     fig_type.update_layout(yaxis={'categoryorder':'total ascending'})
                     st.plotly_chart(fig_type, use_container_width=True)

        with col2:
             st.subheader("Top 10 Locations")
             st.info("""
             **Analyst Insight:**
             *   **Police Facilities** have the highest arrest rates (~70%), likely due to surrender or immediate processing.
             *   **Streets** and **Residences** see the highest volume but lower immediate arrest rates.
             """)
             with st.spinner("Fetching locations..."):
                 top_locs = analysis.get_top_locations(engine, limit=10)
                 if not top_locs.empty:
                     fig_loc = px.bar(top_locs, x='count', y='location_description', orientation='h',
                                      title="Common Crime Locations",
                                      labels={'count': 'Count', 'location_description': 'Location'})
                     fig_loc.update_traces(marker_color='steelblue')
                     fig_loc.update_layout(yaxis={'categoryorder':'total ascending'})
                     st.plotly_chart(fig_loc, use_container_width=True)

if __name__ == "__main__":
    main()