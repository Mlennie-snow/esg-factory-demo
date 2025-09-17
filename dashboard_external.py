#!/usr/bin/env python3
"""
ESG Factory Monitoring Dashboard with Spatial Intelligence - External Version
A comprehensive Streamlit application for visualizing factory ESG data with factory floor mapping,
sensor location intelligence, and zone-based analytics.

This version is designed for external deployment (Streamlit Cloud, Heroku, etc.)
with proper authentication and connection management.
"""

import streamlit as st
import pandas as pd
import altair as alt
import snowflake.connector
from datetime import datetime, timedelta, date
import numpy as np
import os

# Page configuration
st.set_page_config(
    page_title="ESG Factory Monitoring with Spatial Intelligence",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced CSS for better styling
st.markdown("""
<style>
.main-header {
    font-size: 2.5rem;
    font-weight: bold;
    color: #1e3a8a;
    text-align: center;
    margin-bottom: 2rem;
}
.metric-container {
    background-color: #f8fafc;
    padding: 1rem;
    border-radius: 0.5rem;
    border: 1px solid #e2e8f0;
}
.section-header {
    font-size: 1.5rem;
    font-weight: bold;
    color: #374151;
    margin-top: 2rem;
    margin-bottom: 1rem;
}
.zone-info {
    background-color: #f0f9ff;
    padding: 0.5rem;
    border-radius: 0.25rem;
    border-left: 4px solid #0ea5e9;
    margin: 0.5rem 0;
}
.alert-critical {
    background-color: #fef2f2;
    color: #dc2626;
    padding: 0.5rem;
    border-radius: 0.25rem;
    border-left: 4px solid #dc2626;
}
.alert-warning {
    background-color: #fffbeb;
    color: #d97706;
    padding: 0.5rem;
    border-radius: 0.25rem;
    border-left: 4px solid #d97706;
}
.alert-good {
    background-color: #f0fdf4;
    color: #059669;
    padding: 0.5rem;
    border-radius: 0.25rem;
    border-left: 4px solid #059669;
}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_snowflake_connection():
    """Get Snowflake connection using credentials from secrets or environment variables."""
    try:
        # Try to get credentials from Streamlit secrets first
        if hasattr(st, 'secrets') and 'snowflake' in st.secrets:
            conn = snowflake.connector.connect(
                user=st.secrets["snowflake"]["user"],
                password=st.secrets["snowflake"]["password"],
                account=st.secrets["snowflake"]["account"],
                warehouse=st.secrets["snowflake"]["warehouse"],
                database=st.secrets["snowflake"]["database"],
                schema=st.secrets["snowflake"]["schema"]
            )
        else:
            # Fallback to environment variables
            conn = snowflake.connector.connect(
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                database=os.getenv("SNOWFLAKE_DATABASE", "ESG_DEMO_DB"),
                schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW_DATA")
            )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        st.info("Please check your Snowflake credentials in the app settings.")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(query):
    """Load data from Snowflake using external connection."""
    try:
        conn = get_snowflake_connection()
        if conn:
            df = pd.read_sql(query, conn)
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_spatial_sensor_data():
    """Get sensor data with spatial coordinates and compliance status."""
    query = """
    SELECT 
        s.SENSOR_ID,
        s.SENSOR_TYPE,
        s.MEASUREMENT_VALUE,
        s.MEASUREMENT_UNIT,
        s.X_COORDINATE,
        s.Y_COORDINATE,
        s.TIMESTAMP_UTC,
        s.COMPLIANCE_STATUS,
        s.THRESHOLD_TYPE,
        s.COMPLIANCE_STANDARD,
        s.PRIORITY_LEVEL,
        CASE 
            WHEN s.X_COORDINATE <= 70 AND s.Y_COORDINATE <= 80 THEN 'Production Floor'
            WHEN s.X_COORDINATE > 70 AND s.Y_COORDINATE BETWEEN 60 AND 80 THEN 'Office Area'
            WHEN s.X_COORDINATE > 70 AND s.Y_COORDINATE <= 30 THEN 'Utilities'
            WHEN s.X_COORDINATE > 70 AND s.Y_COORDINATE BETWEEN 30 AND 60 THEN 'Storage'
            WHEN s.X_COORDINATE <= 30 AND s.Y_COORDINATE > 80 THEN 'Loading Dock'
            WHEN s.X_COORDINATE BETWEEN 30 AND 70 AND s.Y_COORDINATE > 80 THEN 'Quality Control'
            ELSE 'Other'
        END as ZONE
    FROM ESG_COMPLIANCE_STATUS s
    WHERE s.X_COORDINATE IS NOT NULL AND s.Y_COORDINATE IS NOT NULL
    """
    return load_data(query)

@st.cache_data(ttl=300)
def get_zone_summary():
    """Get ESG compliance summary by factory zone."""
    query = """
    WITH zone_data AS (
        SELECT 
            s.*,
            CASE 
                WHEN s.X_COORDINATE <= 70 AND s.Y_COORDINATE <= 80 THEN 'Production Floor'
                WHEN s.X_COORDINATE > 70 AND s.Y_COORDINATE BETWEEN 60 AND 80 THEN 'Office Area'
                WHEN s.X_COORDINATE > 70 AND s.Y_COORDINATE <= 30 THEN 'Utilities'
                WHEN s.X_COORDINATE > 70 AND s.Y_COORDINATE BETWEEN 30 AND 60 THEN 'Storage'
                WHEN s.X_COORDINATE <= 30 AND s.Y_COORDINATE > 80 THEN 'Loading Dock'
                WHEN s.X_COORDINATE BETWEEN 30 AND 70 AND s.Y_COORDINATE > 80 THEN 'Quality Control'
                ELSE 'Other'
            END as ZONE
        FROM ESG_COMPLIANCE_STATUS s
        WHERE s.X_COORDINATE IS NOT NULL AND s.Y_COORDINATE IS NOT NULL
    )
    SELECT 
        ZONE,
        COUNT(*) as TOTAL_SENSORS,
        COUNT(CASE WHEN COMPLIANCE_STATUS = 'CRITICAL' THEN 1 END) as CRITICAL_COUNT,
        COUNT(CASE WHEN COMPLIANCE_STATUS = 'WARNING' THEN 1 END) as WARNING_COUNT,
        COUNT(CASE WHEN COMPLIANCE_STATUS = 'OFF_TARGET' THEN 1 END) as OFF_TARGET_COUNT,
        COUNT(CASE WHEN COMPLIANCE_STATUS = 'COMPLIANT' THEN 1 END) as COMPLIANT_COUNT,
        ROUND(
            (COUNT(CASE WHEN COMPLIANCE_STATUS = 'COMPLIANT' THEN 1 END) * 100.0) / COUNT(*), 
            1
        ) as COMPLIANCE_PERCENTAGE
    FROM zone_data
    GROUP BY ZONE
    ORDER BY COMPLIANCE_PERCENTAGE ASC
    """
    return load_data(query)

@st.cache_data(ttl=300)
def get_esg_dashboard_metrics():
    """Get ESG dashboard metrics."""
    query = "SELECT * FROM ESG_DASHBOARD_METRICS ORDER BY ESG_CATEGORY, METRIC_NAME"
    return load_data(query)

@st.cache_data(ttl=300)
def get_time_series_data(start_date, end_date):
    """Get time series data for charts."""
    query = f"""
    SELECT 
        TIMESTAMP_UTC,
        SENSOR_TYPE,
        AVG(MEASUREMENT_VALUE) as avg_value,
        MAX(MEASUREMENT_UNIT) as unit
    FROM FACTORY_TELEMETRY
    WHERE DATE(TIMESTAMP_UTC) BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY TIMESTAMP_UTC, SENSOR_TYPE
    ORDER BY TIMESTAMP_UTC
    """
    return load_data(query)

@st.cache_data(ttl=300)
def get_latest_metrics():
    """Get latest KPI metrics for the dashboard."""
    query = """
    WITH latest_readings AS (
        SELECT 
            SENSOR_TYPE,
            MEASUREMENT_VALUE,
            MEASUREMENT_UNIT,
            ROW_NUMBER() OVER (PARTITION BY SENSOR_TYPE ORDER BY TIMESTAMP_UTC DESC) as rn
        FROM FACTORY_TELEMETRY
        WHERE TIMESTAMP_UTC >= CURRENT_TIMESTAMP() - INTERVAL '1 HOUR'
    )
    SELECT 
        SENSOR_TYPE,
        AVG(MEASUREMENT_VALUE) as avg_value,
        MAX(MEASUREMENT_UNIT) as unit
    FROM latest_readings 
    WHERE rn <= 5  -- Average of 5 most recent readings per sensor type
    GROUP BY SENSOR_TYPE
    """
    return load_data(query)

def create_factory_floor_visualization(sensor_data):
    """Create factory floor visualization with sensors overlaid on floor plan."""
    
    if sensor_data.empty:
        st.warning("No sensor data available for visualization")
        return
    
    # Create zone summary
    zone_counts = sensor_data['ZONE'].value_counts()
    compliance_by_zone = sensor_data.groupby(['ZONE', 'COMPLIANCE_STATUS']).size().unstack(fill_value=0)
    
    # Factory floor with overlaid sensors
    st.markdown("### 🏭 Factory Floor Layout with Live Sensor Status (100m × 100m)")
    
    # Create factory floor visualization using Streamlit columns and containers
    st.markdown("#### 🏭 Factory Zone Layout")
    
    # Create a visual factory layout using Streamlit containers
    with st.container():
        # Top row zones
        col1, col2, col3 = st.columns([3, 4, 3])
        
        with col1:
            st.markdown("""
            <div style="background-color: rgba(255, 193, 7, 0.2); border: 2px dashed #ffc107; padding: 10px; text-align: center; height: 80px; display: flex; align-items: center; justify-content: center;">
                <strong>🚛 Loading Dock</strong><br/>
                <small>(0-30m, 80-100m)</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div style="background-color: rgba(13, 202, 240, 0.2); border: 2px dashed #0dcaf0; padding: 10px; text-align: center; height: 80px; display: flex; align-items: center; justify-content: center;">
                <strong>🔬 Quality Control</strong><br/>
                <small>(30-70m, 80-100m)</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div style="background-color: rgba(25, 135, 84, 0.2); border: 2px dashed #198754; padding: 10px; text-align: center; height: 160px; display: flex; align-items: center; justify-content: center;">
                <strong>🏢 Office Area</strong><br/>
                <small>(70-100m, 60-100m)</small>
            </div>
            """, unsafe_allow_html=True)
        
        # Middle and bottom rows
        col1, col2 = st.columns([7, 3])
        
        with col1:
            st.markdown("""
            <div style="background-color: rgba(220, 53, 69, 0.2); border: 2px dashed #dc3545; padding: 20px; text-align: center; height: 240px; display: flex; align-items: center; justify-content: center;">
                <strong>🏭 Production Floor</strong><br/>
                <small>(0-70m, 0-80m)</small><br/>
                <em>Main manufacturing area</em>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            # Storage and Utilities stacked
            st.markdown("""
            <div style="background-color: rgba(108, 117, 125, 0.2); border: 2px dashed #6c757d; padding: 10px; text-align: center; height: 80px; display: flex; align-items: center; justify-content: center; margin-bottom: 10px;">
                <strong>📦 Storage</strong><br/>
                <small>(70-100m, 30-60m)</small>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("""
            <div style="background-color: rgba(102, 16, 242, 0.2); border: 2px dashed #6610f2; padding: 10px; text-align: center; height: 150px; display: flex; align-items: center; justify-content: center;">
                <strong>⚡ Utilities</strong><br/>
                <small>(70-100m, 0-30m)</small>
            </div>
            """, unsafe_allow_html=True)
    
    # Legend
    st.markdown("#### 📍 Sensor Status Legend")
    leg_col1, leg_col2, leg_col3, leg_col4 = st.columns(4)
    
    with leg_col1:
        st.markdown("🟢 **Compliant** - Within ESG thresholds")
    with leg_col2:
        st.markdown("🟡 **Off Target** - Slightly out of range") 
    with leg_col3:
        st.markdown("🟠 **Warning** - Attention required")
    with leg_col4:
        st.markdown("🔴 **Critical** - Immediate action needed")
    
    # Now create the sensor overlay using Altair scatter plot positioned over the floor plan
    st.markdown("#### 📍 Interactive Sensor Map")
    
    if not sensor_data.empty:
        # Create scatter plot for sensor overlay
        sensor_chart = alt.Chart(sensor_data).mark_circle(size=80, opacity=0.8).encode(
            x=alt.X('X_COORDINATE:Q', 
                   title='X Coordinate (meters)', 
                   scale=alt.Scale(domain=[0, 100])),
            y=alt.Y('Y_COORDINATE:Q', 
                   title='Y Coordinate (meters)', 
                   scale=alt.Scale(domain=[0, 100])),
            color=alt.Color('COMPLIANCE_STATUS:N',
                           scale=alt.Scale(
                               domain=['COMPLIANT', 'OFF_TARGET', 'WARNING', 'CRITICAL'],
                               range=['#28a745', '#ffc107', '#fd7e14', '#dc3545']
                           ),
                           title='Compliance Status'),
            stroke=alt.value('white'),
            strokeWidth=alt.value(1),
            tooltip=[
                alt.Tooltip('SENSOR_ID:N', title='Sensor ID'),
                alt.Tooltip('SENSOR_TYPE:N', title='Type'),
                alt.Tooltip('ZONE:N', title='Zone'),
                alt.Tooltip('MEASUREMENT_VALUE:Q', title='Value', format='.2f'),
                alt.Tooltip('MEASUREMENT_UNIT:N', title='Unit'),
                alt.Tooltip('COMPLIANCE_STATUS:N', title='Status'),
                alt.Tooltip('X_COORDINATE:Q', title='X Position'),
                alt.Tooltip('Y_COORDINATE:Q', title='Y Position')
            ]
        ).properties(
            title='Live Sensor Status - Click any sensor for details',
            width=700,
            height=500
        ).interactive()
        
        st.altair_chart(sensor_chart, use_container_width=True)
    
    # Zone statistics below the map
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📊 Sensors by Zone")
        for zone, count in zone_counts.items():
            if 'COMPLIANT' in compliance_by_zone.columns and zone in compliance_by_zone.index:
                compliance_pct = (compliance_by_zone.loc[zone, 'COMPLIANT'] / count * 100)
            else:
                compliance_pct = 0
            status_color = "🟢" if compliance_pct >= 80 else "🟡" if compliance_pct >= 60 else "🔴"
            st.write(f"{status_color} **{zone}**: {count} sensors ({compliance_pct:.1f}% compliant)")
    
    with col2:
        st.markdown("#### ⚠️ Compliance Status")
        status_counts = sensor_data['COMPLIANCE_STATUS'].value_counts()
        for status, count in status_counts.items():
            status_emoji = {"COMPLIANT": "✅", "WARNING": "⚠️", "CRITICAL": "🔴", "OFF_TARGET": "🟡"}.get(status, "❓")
            st.write(f"{status_emoji} **{status}**: {count} sensors")

def create_zone_compliance_chart(zone_summary):
    """Create a zone-based compliance chart using Altair."""
    
    if zone_summary.empty:
        st.warning("No zone summary data available")
        return
    
    # Prepare data for visualization
    chart_data = []
    for _, row in zone_summary.iterrows():
        chart_data.extend([
            {"Zone": row['ZONE'], "Status": "Compliant", "Count": row['COMPLIANT_COUNT'], "Color": "#059669"},
            {"Zone": row['ZONE'], "Status": "Off Target", "Count": row['OFF_TARGET_COUNT'], "Color": "#eab308"},
            {"Zone": row['ZONE'], "Status": "Warning", "Count": row['WARNING_COUNT'], "Color": "#d97706"},
            {"Zone": row['ZONE'], "Status": "Critical", "Count": row['CRITICAL_COUNT'], "Color": "#dc2626"}
        ])
    
    chart_df = pd.DataFrame(chart_data)
    
    # Create stacked bar chart
    chart = alt.Chart(chart_df).mark_bar().encode(
        x=alt.X('Zone:N', title='Factory Zone'),
        y=alt.Y('Count:Q', title='Number of Sensors'),
        color=alt.Color('Status:N', 
                       scale=alt.Scale(
                           domain=['Compliant', 'Off Target', 'Warning', 'Critical'],
                           range=['#059669', '#eab308', '#d97706', '#dc2626']
                       ),
                       title='Compliance Status'),
        order=alt.Order('Status:N', sort='descending')
    ).properties(
        title='ESG Compliance Status by Factory Zone',
        width=600,
        height=400
    )
    
    st.altair_chart(chart, use_container_width=True)

def display_zone_details(zone_summary, selected_zone):
    """Display detailed information for a selected zone."""
    zone_data = zone_summary[zone_summary['ZONE'] == selected_zone]
    
    if not zone_data.empty:
        zone_info = zone_data.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Sensors", int(zone_info['TOTAL_SENSORS']))
        
        with col2:
            compliance_pct = zone_info['COMPLIANCE_PERCENTAGE']
            st.metric("Compliance %", f"{compliance_pct}%")
        
        with col3:
            warning_count = int(zone_info['WARNING_COUNT'] + zone_info['CRITICAL_COUNT'])
            st.metric("Alerts", warning_count)
        
        with col4:
            compliant_count = int(zone_info['COMPLIANT_COUNT'])
            st.metric("Compliant", compliant_count)

def main():
    """Main dashboard function."""
    # Main header
    st.markdown('<h1 class="main-header">🏭 ESG Factory Monitoring with Spatial Intelligence</h1>', 
                unsafe_allow_html=True)
    
    # Connection status check
    conn = get_snowflake_connection()
    if not conn:
        st.error("❌ Unable to connect to Snowflake database")
        st.info("""
        **Setup Required**: Please configure your Snowflake credentials:
        
        **For Streamlit Cloud deployment:**
        1. Add secrets in your Streamlit Cloud app settings
        2. Include: user, password, account, warehouse, database, schema
        
        **For local development:**
        1. Set environment variables: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.
        2. Or create a .streamlit/secrets.toml file
        """)
        return
    
    st.success("✅ Connected to Snowflake ESG Demo Database")
    
    # Sidebar for controls
    st.sidebar.title("🎛️ Dashboard Controls")
    
    # Date range selector
    st.sidebar.subheader("📅 Date Range")
    today = date.today()
    default_start = today - timedelta(days=7)
    
    start_date = st.sidebar.date_input("Start Date", value=default_start, max_value=today)
    end_date = st.sidebar.date_input("End Date", value=today, max_value=today)
    
    # Refresh data button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.rerun()
    
    # Load spatial sensor data
    with st.spinner("Loading factory sensor data..."):
        sensor_data = get_spatial_sensor_data()
        zone_summary = get_zone_summary()
        esg_metrics = get_esg_dashboard_metrics()
        time_series_data = get_time_series_data(start_date, end_date)
        latest_metrics = get_latest_metrics()
    
    if sensor_data.empty:
        st.error("No sensor data available. Please check your database connection.")
        st.info("Expected data source: ESG_COMPLIANCE_STATUS view in ESG_DEMO_DB.RAW_DATA schema")
        return
    
    # Zone filter
    st.sidebar.subheader("🏗️ Factory Zones")
    available_zones = sensor_data['ZONE'].unique().tolist()
    selected_zones = st.sidebar.multiselect(
        "Select Zones to Display",
        available_zones,
        default=available_zones
    )
    
    # Sensor type filter
    st.sidebar.subheader("📊 Sensor Types")
    available_sensor_types = sensor_data['SENSOR_TYPE'].unique().tolist()
    selected_sensor_types = st.sidebar.multiselect(
        "Select Sensor Types",
        available_sensor_types,
        default=available_sensor_types
    )
    
    # Compliance status filter
    st.sidebar.subheader("⚠️ Compliance Status")
    available_statuses = sensor_data['COMPLIANCE_STATUS'].unique().tolist()
    selected_statuses = st.sidebar.multiselect(
        "Select Compliance Status",
        available_statuses,
        default=available_statuses
    )
    
    # Apply filters
    filtered_data = sensor_data[
        (sensor_data['ZONE'].isin(selected_zones)) &
        (sensor_data['SENSOR_TYPE'].isin(selected_sensor_types)) &
        (sensor_data['COMPLIANCE_STATUS'].isin(selected_statuses))
    ]
    
    # KPI Metrics Section
    if not latest_metrics.empty:
        st.markdown('<h2 class="section-header">📊 Key Performance Indicators</h2>', 
                    unsafe_allow_html=True)
        
        # Create columns for metrics
        col1, col2, col3, col4 = st.columns(4)
        
        # Power Consumption
        power_consumption = latest_metrics[latest_metrics['SENSOR_TYPE'] == 'PowerConsumption']
        if not power_consumption.empty:
            with col1:
                st.metric(
                    "Avg Power Consumption",
                    f"{power_consumption.iloc[0]['AVG_VALUE']:.1f} {power_consumption.iloc[0]['UNIT']}"
                )
        
        # Temperature
        temperature = latest_metrics[latest_metrics['SENSOR_TYPE'] == 'Temperature']
        if not temperature.empty:
            with col2:
                st.metric(
                    "Average Temperature",
                    f"{temperature.iloc[0]['AVG_VALUE']:.1f} {temperature.iloc[0]['UNIT']}"
                )
        
        # CO2 Levels
        co2 = latest_metrics[latest_metrics['SENSOR_TYPE'] == 'AirQuality_CO2']
        if not co2.empty:
            with col3:
                st.metric(
                    "Average CO2 Level",
                    f"{co2.iloc[0]['AVG_VALUE']:.0f} {co2.iloc[0]['UNIT']}"
                )
        
        # Water Consumption
        water = latest_metrics[latest_metrics['SENSOR_TYPE'] == 'WaterConsumption']
        if not water.empty:
            with col4:
                st.metric(
                    "Water Usage",
                    f"{water.iloc[0]['AVG_VALUE']:.1f} {water.iloc[0]['UNIT']}"
                )
    
    # ESG Summary Metrics
    if not esg_metrics.empty:
        st.markdown('<h2 class="section-header">🌱 ESG Performance Overview</h2>', 
                    unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("### 🌱 Environmental")
            env_metrics = esg_metrics[esg_metrics['ESG_CATEGORY'] == 'Environmental']
            for _, metric in env_metrics.iterrows():
                rating_class = "alert-good" if metric['RATING'] == 'GOOD' else "alert-warning" if 'ACCEPTABLE' in metric['RATING'] or 'MODERATE' in metric['RATING'] else "alert-critical"
                st.markdown(f'<div class="{rating_class}">{metric["METRIC_NAME"]}: {metric["METRIC_VALUE"]} {metric["UNIT"]} ({metric["RATING"]})</div>', 
                           unsafe_allow_html=True)
        
        with col2:
            st.markdown("### 👥 Social")
            social_metrics = esg_metrics[esg_metrics['ESG_CATEGORY'] == 'Social']
            for _, metric in social_metrics.iterrows():
                rating_class = "alert-good" if metric['RATING'] == 'EXCELLENT' else "alert-warning" if 'ACCEPTABLE' in metric['RATING'] else "alert-critical"
                st.markdown(f'<div class="{rating_class}">{metric["METRIC_NAME"]}: {metric["METRIC_VALUE"]} {metric["UNIT"]} ({metric["RATING"]})</div>', 
                           unsafe_allow_html=True)
        
        with col3:
            st.markdown("### 🏛️ Governance")
            gov_metrics = esg_metrics[esg_metrics['ESG_CATEGORY'] == 'Governance']
            for _, metric in gov_metrics.iterrows():
                rating_class = "alert-good" if metric['RATING'] == 'EXCELLENT' or metric['RATING'] == 'EFFICIENT' else "alert-warning" if 'MODERATE' in metric['RATING'] or 'GOOD' in metric['RATING'] else "alert-critical"
                st.markdown(f'<div class="{rating_class}">{metric["METRIC_NAME"]}: {metric["METRIC_VALUE"]} {metric["UNIT"]} ({metric["RATING"]})</div>', 
                           unsafe_allow_html=True)
    
    # Factory Floor Visualization
    st.markdown('<h2 class="section-header">🗺️ Factory Floor Layout & Sensor Status</h2>', 
                unsafe_allow_html=True)
    
    if not filtered_data.empty:
        # Factory floor representation with overlaid sensors
        create_factory_floor_visualization(filtered_data)
        
        # Zone-based Analytics
        st.markdown('<h2 class="section-header">🏗️ Zone-Based ESG Analytics</h2>', 
                    unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            if not zone_summary.empty:
                create_zone_compliance_chart(zone_summary)
        
        with col2:
            st.markdown("### Zone Selection for Details")
            selected_zone = st.selectbox(
                "Select a zone for detailed analysis:",
                available_zones
            )
            
            if selected_zone:
                st.markdown(f"### 📍 {selected_zone} Details")
                display_zone_details(zone_summary, selected_zone)
    
        # Time Series Charts (if data available)
        if not time_series_data.empty:
            st.markdown('<h2 class="section-header">📈 Time Series Analytics</h2>', 
                        unsafe_allow_html=True)
            
            # Power consumption over time
            power_data = time_series_data[time_series_data['SENSOR_TYPE'] == 'PowerConsumption']
            if not power_data.empty:
                power_chart = alt.Chart(power_data).mark_line().encode(
                    x=alt.X('TIMESTAMP_UTC:T', title='Time'),
                    y=alt.Y('AVG_VALUE:Q', title='Power (kW)'),
                    tooltip=['TIMESTAMP_UTC:T', 'AVG_VALUE:Q']
                ).properties(
                    title='Power Consumption Over Time',
                    width=600,
                    height=300
                )
                st.altair_chart(power_chart, use_container_width=True)
            
            # Temperature over time
            temp_data = time_series_data[time_series_data['SENSOR_TYPE'] == 'Temperature']
            if not temp_data.empty:
                temp_chart = alt.Chart(temp_data).mark_line(color='orange').encode(
                    x=alt.X('TIMESTAMP_UTC:T', title='Time'),
                    y=alt.Y('AVG_VALUE:Q', title='Temperature (°C)'),
                    tooltip=['TIMESTAMP_UTC:T', 'AVG_VALUE:Q']
                ).properties(
                    title='Average Temperature Over Time',
                    width=600,
                    height=300
                )
                st.altair_chart(temp_chart, use_container_width=True)
    
        # Zone Summary Table
        st.markdown('<h3 class="section-header">📋 Zone Compliance Summary</h3>', 
                    unsafe_allow_html=True)
        
        if not zone_summary.empty:
            # Style the dataframe
            zone_display = zone_summary.copy()
            zone_display['COMPLIANCE_PERCENTAGE'] = zone_display['COMPLIANCE_PERCENTAGE'].astype(str) + '%'
            
            st.dataframe(
                zone_display,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "ZONE": "Factory Zone",
                    "TOTAL_SENSORS": "Total Sensors",
                    "CRITICAL_COUNT": "Critical",
                    "WARNING_COUNT": "Warning", 
                    "OFF_TARGET_COUNT": "Off Target",
                    "COMPLIANT_COUNT": "Compliant",
                    "COMPLIANCE_PERCENTAGE": "Compliance %"
                }
            )
    
        # Sensor Details Table
        st.markdown('<h3 class="section-header">📋 Filtered Sensor Details</h3>', 
                    unsafe_allow_html=True)
        
        if not filtered_data.empty:
            # Display filtered sensor data
            display_data = filtered_data[['SENSOR_ID', 'SENSOR_TYPE', 'ZONE', 'MEASUREMENT_VALUE', 
                                        'MEASUREMENT_UNIT', 'COMPLIANCE_STATUS', 'COMPLIANCE_STANDARD']].copy()
            
            st.dataframe(
                display_data,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "SENSOR_ID": "Sensor ID",
                    "SENSOR_TYPE": "Type",
                    "ZONE": "Zone",
                    "MEASUREMENT_VALUE": "Value",
                    "MEASUREMENT_UNIT": "Unit",
                    "COMPLIANCE_STATUS": "Status",
                    "COMPLIANCE_STANDARD": "Standard"
                }
            )
        
        # Summary Statistics
        st.markdown('<h3 class="section-header">📊 Current View Statistics</h3>', 
                    unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Filtered Sensors", len(filtered_data))
        
        with col2:
            compliant_count = len(filtered_data[filtered_data['COMPLIANCE_STATUS'] == 'COMPLIANT'])
            st.metric("Compliant", compliant_count)
        
        with col3:
            warning_count = len(filtered_data[filtered_data['COMPLIANCE_STATUS'].isin(['WARNING', 'CRITICAL'])])
            st.metric("Alerts", warning_count)
        
        with col4:
            if len(filtered_data) > 0:
                compliance_pct = round((compliant_count / len(filtered_data)) * 100, 1)
                st.metric("Compliance %", f"{compliance_pct}%")
            else:
                st.metric("Compliance %", "N/A")
    
    else:
        st.warning("No data matches the current filter selection.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #6b7280; font-size: 0.9rem;'>
            🏭 ESG Factory Monitoring with Spatial Intelligence | Powered by Snowflake & Streamlit<br>
            <small>Real-time ESG compliance monitoring across 100m × 100m factory floor with 120+ sensors</small>
        </div>
        """, 
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
