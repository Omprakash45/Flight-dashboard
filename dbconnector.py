import pymysql
from pymysql import MySQLError

class DB:
    def __init__(self):
        """Initialize the connection to the database and cursor."""
        try:
            self.conn = pymysql.connect(
                host='127.0.0.1',  # Update with your DB host if different
                user='root',  # Update with your DB username
                password='548017',  # Update with your DB password
                database='Flights'  # Update with your DB name
            )
            self.mycursor = self.conn.cursor()  # Initialize cursor for executing queries
            print("Database connected successfully")  # Log message for successful connection
        except MySQLError as e:
            print(f"Error connecting to MySQL: {e}")
            raise  # Re-raise exception to indicate that DB connection failed

    def source_city(self):
        """Fetch distinct source cities from the flight table."""
        try:
            self.mycursor.execute("SELECT DISTINCT DepartingCity FROM flight")
            cities = [row[0] for row in self.mycursor.fetchall()]
            return cities
        except pymysql.MySQLError as err:
            print(f"Error fetching cities: {err}")
            return []

    def all_flights(self, source, destination):
        """Fetch all flights between source and destination."""
        try:
            self.mycursor.execute(
                """SELECT FlightName, DepartingTime, ArrivingTime, Duration, Price 
                   FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s""",
                (source, destination)
            )
            data = self.mycursor.fetchall()
            column_names = [i[0] for i in self.mycursor.description]
            return data, column_names
        except pymysql.MySQLError as err:
            print(f"Error fetching flights: {err}")
            return [], []

    def get_avg_price_distribution(self, source, destination):
        """Get average price distribution for flights between source and destination."""
        try:
            self.mycursor.execute(
                """SELECT FlightName, ROUND(AVG(Price), 2) as Average_price 
                   FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s
                   GROUP BY FlightName""",
                (source, destination)
            )
            data = self.mycursor.fetchall()
            column_names = [i[0] for i in self.mycursor.description]
            return data, column_names
        except pymysql.MySQLError as err:
            print(f"Error fetching price distribution: {err}")
            return [], []

    def get_flight_frequency_per_airline(self, source, destination):
        """Get flight frequency per airline."""
        try:
            self.mycursor.execute(
                """SELECT FlightName, COUNT(*) as Frequency 
                   FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s  
                   GROUP BY FlightName""",
                (source, destination)
            )
            data = self.mycursor.fetchall()
            column_names = [i[0] for i in self.mycursor.description]
            return data, column_names
        except pymysql.MySQLError as err:
            print(f"Error fetching flight frequency: {err}")
            return [], []

    def get_average_duration_per_airline(self, source, destination):
        """Get average flight duration per airline."""
        try:
            self.mycursor.execute(
                """SELECT FlightName, AVG(Duration) as AverageDuration 
                   FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s 
                   GROUP BY FlightName""",
                (source, destination)
            )
            data = self.mycursor.fetchall()
            column_names = [i[0] for i in self.mycursor.description]
            return data, column_names
        except pymysql.MySQLError as err:
            print(f"Error fetching average duration: {err}")
            return [], []

    def get_peak_departure_times(self, source, destination):
        """Get peak departure times for flights between source and destination."""
        try:
            self.mycursor.execute(
                """SELECT DepartingTime, COUNT(*) as Frequency 
                   FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s
                   GROUP BY DepartingTime""",
                (source, destination)
            )
            data = self.mycursor.fetchall()
            column_names = [i[0] for i in self.mycursor.description]
            return data, column_names
        except pymysql.MySQLError as err:
            print(f"Error fetching peak departure times: {err}")
            return [], []

    def get_price_by_departure_time(self, source, destination):
        """Get price trend by departure time."""
        try:
            self.mycursor.execute(
                """SELECT DepartingTime, AVG(Price) as AveragePrice 
                   FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s
                   GROUP BY DepartingTime ORDER BY DepartingTime""",
                (source, destination)
            )
            data = self.mycursor.fetchall()
            column_names = [i[0] for i in self.mycursor.description]
            return data, column_names
        except pymysql.MySQLError as err:
            print(f"Error fetching price by departure time: {err}")
            return [], []
