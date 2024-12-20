import streamlit as st
import pandas as pd
import plotly.express as px
from dbconnector import DB
import streamlit.components.v1 as components

# Initialize the database connection
try:
    db = DB()  # This will trigger connection and cursor initialization
except Exception as e:
    st.error(f"Error connecting to the database: {e}")
    st.stop()  # Stop execution if database connection fails

# Get the list of cities once
try:
    cities = db.source_city()
    if not cities:
        st.error("No cities found in the database.")
except Exception as e:
    st.error(f"Error fetching cities: {e}")
    st.stop()  # Stop execution if cities cannot be fetched

# Streamlit Sidebar
st.sidebar.title("Flight Dashboard")
user_input = st.sidebar.selectbox("Menu", ["Choose one", "Check flights", "Flights analytics"])

# Check Flights Section
if user_input == "Check flights":
    st.title("Flights")
    st.header("Enter source and destination from dropdown")
    
    if cities:
        col1, col2 = st.columns(2)
        with col1:
            source = st.selectbox("Source", cities)
        with col2:
            destination = st.selectbox("Destination", cities)

        if source and destination:
            st.write(f"Showing available flights from {source} to {destination}.")

        if st.button("Search"):
            with st.spinner('Fetching flight data...'):
                result, column_names = db.all_flights(source, destination)

            if result:
                df = pd.DataFrame(result, columns=column_names)
                st.dataframe(df)     
            else:
                st.write("No flights found for the selected route.")

elif user_input == "Choose one":
    st.title("About The Dashboard")
    try:
        with open("about.html", "r") as f:
            html_content = f.read()
        components.html(html_content, height=600, scrolling=True)
    except FileNotFoundError:
        st.error("The 'about.html' file was not found. Please ensure it exists in the application directory.")

elif user_input == "Flights analytics":
    st.title("Analytics")
    
    # Get cities once
    cities = db.source_city()

    col1, col2 = st.columns(2)
    with col1:
        source = st.selectbox("Source", cities)
    with col2:
        destination = st.selectbox("Destination", cities)
    
    # Price Distribution
    st.header('Price Distribution')
    result, column_names = db.get_avg_price_distribution(source, destination)
    df = pd.DataFrame(result, columns=column_names)
    fig = px.bar(df, x='FlightName', y='Average_price', title=f'Average price when going from {source} to {destination}')
    st.plotly_chart(fig)    
    
    # Flight Frequency per Airline
    st.header('Flight Frequency per Airline')
    result, column_names = db.get_flight_frequency_per_airline(source, destination)
    df = pd.DataFrame(result, columns=column_names)

    fig = px.pie(df, names='FlightName', values='Frequency', title='Flight Frequency per Airline')
    st.plotly_chart(fig)

    # Average Flight Duration per Airline
    st.header('Average Flight Duration per Airline')
    result, column_names = db.get_average_duration_per_airline(source, destination)
    df = pd.DataFrame(result, columns=column_names)

    fig = px.bar(df, x='FlightName', y='AverageDuration', title=f'Average duration time (hrs) between {source} to {destination}')
    st.plotly_chart(fig)

    # Peak Departure Times
    st.header('Peak Departure Times')
    result, column_names = db.get_peak_departure_times(source, destination)
    df = pd.DataFrame(result, columns=column_names)
    fig = px.bar(df, x='DepartingTime', y='Frequency', title=f'Number of flights running between {source} to {destination}')
    st.plotly_chart(fig)

    # Price trend by time of the day
    st.header("Price Trend By Time Of The Day")
    result, column_names = db.get_price_by_departure_time(source, destination)
    df = pd.DataFrame(result, columns=column_names)
    fig = px.line(df, x='DepartingTime', y='AveragePrice', title=f'Price trends for flights from {source} to {destination}')
    st.plotly_chart(fig)
