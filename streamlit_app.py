import pandas as pd
from sqlalchemy import create_engine, text
import streamlit as st
import plotly.express as px
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# TiDB Connection Details
TIDB_USER = os.getenv("TIDB_USER")
TIDB_PASSWORD = os.getenv("TIDB_PASSWORD")
TIDB_HOST = os.getenv("TIDB_HOST")
TIDB_PORT = os.getenv("TIDB_PORT")
TID_CA_PATH = os.getenv("TID_CA_PATH")
TIDB_DB_NAME = os.getenv("TIDB_DB_NAME") or "Chicago_data"

def get_db_connection():
    # Construct connection URL
    url = f"mysql+pymysql://{TIDB_USER}:{TIDB_PASSWORD}@{TIDB_HOST}:{TIDB_PORT}/{TIDB_DB_NAME}?ssl_ca={TID_CA_PATH}&ssl_verify_cert=true&ssl_verify_identity=true"
    engine = create_engine(url)
    return engine

def sync_data():
    # --- TiDB Database Query ---
    try:
        engine = get_db_connection()
        year = 2024
        # Query matching the original logic: entire year 2024
        query = text(f"""
            SELECT * 
            FROM chicago_crimes 
            WHERE DATE >= '{year}-01-01 00:00:00' 
              AND DATE <= '{year}-12-31 23:59:59'
            ORDER BY DATE ASC
            LIMIT 1000
        """)
        
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
            
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return

    # Ensure date column is datetime
    if 'DATE' in df.columns:
        df['DATE'] = pd.to_datetime(df['DATE'])
    elif 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])

    # Handle numeric columns
    # Note: TiDB columns might already be numeric, but good to ensure
    num_cols = ['latitude', 'longitude', 'x_coordinate', 'y_coordinate']
    for col in num_cols:
         # Check both lower and upper case as DB column names case sensitivity can vary
        upper_col = col.upper()
        if upper_col in df.columns:
            df[upper_col] = pd.to_numeric(df[upper_col], errors='coerce')
        elif col in df.columns:
             df[col] = pd.to_numeric(df[col], errors='coerce')


    # Standardize column names to uppercase
    df.columns = [col.upper() for col in df.columns]


    # Show app title and description.
    st.set_page_config(page_title="Crime Data Analysis", page_icon="📊")
    st.title(" 📊 Crime Data Analysis")
    st.write(
        """
        Look what you make me do!.
        """
    )

    st.session_state.df = df

    st.header("Existing data")
    st.write(f"Number of data: `{len(st.session_state.df)}`")

    st.info(
        "Balabala",
        icon="✍️",
    )

    show_df = st.dataframe(st.session_state.df, 
                           use_container_width=True, hide_index=True,
                            column_config={
                                 "ID": st.column_config.Column(
                                      "ID",
                                      help="Unique identifier for each crime record.",
                                      width="medium"
                                 ),
                                 "DATE": st.column_config.Column(
                                      "Date",
                                      help="Date and time when the crime occurred.",
                                      width="medium"
                                 ),
                                 "PRIMARY_TYPE": st.column_config.Column(
                                      "Primary Type",
                                      help="Primary classification of the crime (e.g., THEFT, ASSAULT).",
                                      width="medium"
                                 ),
                                 "DESCRIPTION": st.column_config.Column(
                                      "Description",
                                      help="Detailed description of the crime.",
                                      width="medium"
                                 ),
                                 "LOCATION_DESCRIPTION": st.column_config.Column(
                                      "Location Description",
                                      help="Description of the location where the crime occurred.",
                                      width="medium"
                                 ),
                                 "ARREST": st.column_config.Column(
                                      "Arrest Made",
                                      help="Indicates whether an arrest was made (True/False).",
                                      width="small"
                                 ),
                                 "LOCATION": None  # Hide the LOCATION column for better readability                        ),
                            }
                           )


    def plot_yearly_trend(df):
        # Ensure YEAR column exists or derive it
        if 'YEAR' not in df.columns:
             df['YEAR'] = df['DATE'].dt.year
        
        yearly_counts = df.groupby('YEAR').size().reset_index(name='Crime Count')
        x = st.dataframe(yearly_counts)
        fig = px.line(yearly_counts, x='YEAR', y='Crime Count', title='Annual Crime Trend (2014-2024)')
        st.plotly_chart(fig, use_container_width=True)

    
    def plot_hour_day_heatmap(df):
        # df['Date'] = pd.to_datetime(df['DATE'])
        df['Hour'] = df['DATE'].dt.hour
        df['DayOfWeek'] = df['DATE'].dt.dayofweek
        df['Month'] = df['DATE'].dt.month

        # print(df[['DATE', 'Hour', 'DayOfWeek']].head())
        heatmap_data = df.groupby(['DayOfWeek', 'Hour']).size().unstack()
        fig = px.imshow(heatmap_data, labels=dict(x="Hour of Day", y="Day of Week", color="Crime Frequency"),
                        title="Crime Heatmap: Hour vs Day of Week")
        st.plotly_chart(fig, use_container_width=True)



    # Additional visualizations
    st.subheader("📈 Annual Crime Trend")
    plot_yearly_trend(st.session_state.df)

    st.subheader("⏰ Crime Heatmap: Hour vs Day of Week")
    plot_hour_day_heatmap(st.session_state.df)

    st.subheader("🗺️ Crime Hotspots in Chicago")
    # Check if LATITUDE and LONGITUDE exist
    if 'LATITUDE' in st.session_state.df.columns and 'LONGITUDE' in st.session_state.df.columns:
        map_df = st.session_state.df[['LATITUDE', 'LONGITUDE']].dropna()
        st.map(map_df)
    else:
        st.warning("Latitude/Longitude data not available for mapping.")

    
    



if __name__ == "__main__":
    sync_data()