import pymysql
from pymysql import MySQLError

class DB:
    def __init__(self):
      

try:
    self.conn = pymysql.connect(
        host='127.0.0.1',
        user='root',
        password='548017',
        database='Flights'
    )
except MySQLError as e:
    print(f"Error connecting to MySQL: {e}")
    # Handle or raise exception further if needed


    def source_city(self):
        try:
            self.mycursor.execute("SELECT DISTINCT DepartingCity FROM flight")
            cities = [row[0] for row in self.mycursor.fetchall()]
            return cities
        except pymysql.MySQLError as err:
            print(f"Error fetching cities: {err}")
            return []

    def all_flights(self, source, destination):
        self.mycursor.execute(
            """SELECT FlightName, DepartingTime, ArrivingTime, Duration, Price 
               FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_avg_price_distribution(self, source, destination):
        self.mycursor.execute(
            """SELECT FlightName, ROUND(AVG(Price), 2) as Average_price 
               FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s
               GROUP BY FlightName""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_flight_frequency_per_airline(self, source, destination):
        self.mycursor.execute(
            """SELECT FlightName, COUNT(*) as Frequency 
               FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s  
               GROUP BY FlightName""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_average_duration_per_airline(self, source, destination):
        self.mycursor.execute(
            """SELECT FlightName, AVG(Duration) as AverageDuration 
               FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s 
               GROUP BY FlightName""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_peak_departure_times(self, source, destination):
        self.mycursor.execute(
            """SELECT DepartingTime, COUNT(*) as Frequency 
               FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s
               GROUP BY DepartingTime""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names

    def get_price_by_departure_time(self, source, destination):
        self.mycursor.execute(
            """SELECT DepartingTime, AVG(Price) as AveragePrice 
               FROM flight WHERE DepartingCity=%s AND ArrivingCity=%s
               GROUP BY DepartingTime ORDER BY DepartingTime""",
            (source, destination)
        )
        data = self.mycursor.fetchall()
        column_names = [i[0] for i in self.mycursor.description]
        return data, column_names
