import pymysql

class DB:
    def __init__(self):
        try:
            # Use PyMySQL for the connection
            self.conn = pymysql.connect(
                host="127.0.0.1",
                user="root",
                password="548017",  # Ensure this is your correct password
                database="flights"
                port="3306"
            )
            self.mycursor = self.conn.cursor()
            print("Connection established")
        except pymysql.MySQLError as err:
            print(f"Connection error: {err}")
            raise  # Re-raise the exception to notify Streamlit
    
    def source_city(self):
        """Fetch distinct departing cities from the database."""
        try:
            self.mycursor.execute("SELECT DISTINCT DepartingCity FROM flight")
            cities = [row[0] for row in self.mycursor.fetchall()]  # Extract city names
            return cities
        except pymysql.MySQLError as err:
            print(f"Error fetching cities: {err}")
            return []  # Return an empty list if an error occurs
    def all_flights(self, source, destination):
        self.mycursor.execute(
            """select FlightName,DepartingTime,ArrivingTime,Duration,Price FROM 
            flight WHERE DepartingCity=%s AND ArrivingCity=%s""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names
    def get_avg_price_distribution(self,source,destination):
        self.mycursor.execute(
            """SELECT FlightName, round(avg(Price),2) as Average_price FROM flight
            WHERE DepartingCity=%s AND ArrivingCity=%s
             GROUP BY FlightName""",(source,destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names
    def get_flight_frequency_per_airline(self,source,destination):
        self.mycursor.execute(
            """SELECT FlightName, COUNT(*) as Frequency FROM flight
            WHERE DepartingCity=%s AND ArrivingCity=%s  
            GROUP BY FlightName""",((source,destination))
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names    
    def get_average_duration_per_airline(self,source,destination):
        self.mycursor.execute(
            """SELECT FlightName, AVG(Duration) as AverageDuration FROM flight
            WHERE DepartingCity=%s AND ArrivingCity=%s 
             GROUP BY FlightName""",(source,destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_peak_departure_times(self,source,destination):
        self.mycursor.execute(
            """SELECT DepartingTime, COUNT(*) as Frequency FROM flight
             WHERE DepartingCity=%s AND ArrivingCity=%s
            GROUP BY DepartingTime""",(source,destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_price_by_departure_time(self, source, destination):
        self.mycursor.execute( """
            SELECT DepartingTime, AVG(Price) as AveragePrice 
            FROM flight
            WHERE DepartingCity=%s AND ArrivingCity=%s
            GROUP BY DepartingTime
            ORDER BY DepartingTime
        """,(source,destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names
