import pandas as pd
from sodapy import Socrata
import streamlit as st
import plotly.express as px

SOCRATA_DOMAIN = "data.cityofchicago.org"
DATASET_ID = "ijzp-q8t2"
# APP_TOKEN = "SOCRATA_TOKEN_NAME"  


def sync_data():
    # --- SODA API requests crime data ---
    client = Socrata(SOCRATA_DOMAIN,None)

    
    results = client.get(DATASET_ID,where="date > '2025-01-01T00:00:00'", 
                         limit=10000, order="date ASC")
    
    df = pd.DataFrame.from_records(results)

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    

    num_cols = ['latitude', 'longitude', 'x_coordinate', 'y_coordinate']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')


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
        
        yearly_counts = df.groupby('YEAR').size().reset_index(name='Crime Count')
        x = st.dataframe(yearly_counts)
        fig = px.line(yearly_counts, x='YEAR', y='Crime Count', title='Annual Crime Trend (2014-2024)')
        st.plotly_chart(fig, use_container_width=True)

    
    def plot_hour_day_heatmap(df):
        # df['Date'] = pd.to_datetime(df['DATE'])
        df['Hour'] = df['DATE'].dt.hour
        df['DayOfWeek'] = df['DATE'].dt.dayofweek
        df['Month'] = df['DATE'].dt.month

        print(df[['DATE', 'Hour', 'DayOfWeek']].head())
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
    map_df = st.session_state.df[['LATITUDE', 'LONGITUDE']].dropna()
    st.map(map_df)

    
    



if __name__ == "__main__":
    sync_data()