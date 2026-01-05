#Imports

import json

import requests
import sqlite4
import sqlalchemy as db
from requests import session
from sqlalchemy import MetaData, Table, Column, Integer, String, Numeric, create_engine, select, delete
from sqlalchemy.orm import DeclarativeBase, sessionmaker


#C1
#My Location is Long Beach CA, 33.77, -118.19. The below class contains the default weather values and pulls the location and date from the class arguments.

class Weather:
    def __init__(self, latitude, longitude, day, month, year):
        self.latitude = latitude
        self.longitude = longitude
        self.month = month
        self.day = day
        self.year = year
        self.avgtemp = 0
        self.mintemp = 0
        self.maxtemp = 0
        self.avgspeed = 0
        self.minspeed = 0
        self.maxspeed = 0
        self.sumrain = 0
        self.minrain = 0
        self.maxrain = 0

#C2
# This section creates three methods to pull certain data points from the weather API.
# Each method uses a while loop that uses a counter to subtract one year from the starting year to pull the 5 date's data.
# Then the method calculates the avg and assigned the variable the calculated value.

    def getAvgTemp(self):
        count = 0
        temps = []
        avg = 0
        while count < 5:
            URL = f"https://archive-api.open-meteo.com/v1/archive?latitude={self.latitude}&longitude={self.longitude}&start_date={self.year - count}-{self.month}-{self.day}&end_date={self.year}-{self.month}-{self.day}&daily=temperature_2m_mean&timezone=America%2FLos_Angeles&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch"
            response = requests.get(URL)
            jsonData = response.json()
            temps.append(float(jsonData["daily"]["temperature_2m_mean"][0]))
            count += 1
        avg = round(sum(temps) / len(temps), 2)
        self.avgtemp = avg

    def getMaxWind(self):
        count = 0
        speeds = []
        while count < 5:
            URL = f"https://archive-api.open-meteo.com/v1/archive?latitude={self.latitude}&longitude={self.longitude}&start_date={self.year - count}-{self.month}-{self.day}&end_date={self.year}-{self.month}-{self.day}&daily=wind_speed_10m_max&timezone=America%2FLos_Angeles&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch"
            response = requests.get(URL)
            jsonData = response.json()
            speeds.append(float(jsonData["daily"]["wind_speed_10m_max"][0]))
            count += 1
        maxwind = max(speeds)
        self.maxspeed = maxwind

    def getSumRain(self):
        count = 0
        sumrain = 0
        while count < 5:
            URL = f"https://archive-api.open-meteo.com/v1/archive?latitude={self.latitude}&longitude={self.longitude}&start_date={self.year - count}-{self.month}-{self.day}&end_date={self.year}-{self.month}-{self.day}&daily=precipitation_sum&timezone=America%2FLos_Angeles&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch"
            response = requests.get(URL)
            jsonData = response.json()
            sumrain += float(jsonData["daily"]["precipitation_sum"][0])
            count += 1
        self.sumrain = sumrain


# URL = f"https://archive-api.open-meteo.com/v1/archive?latitude={LBC.latitude}&longitude={LBC.longitude}&start_date={LBC.year}-{LBC.month}-{LBC.day}&end_date={LBC.year}-{LBC.month}-{LBC.day}&daily=temperature_2m_mean,temperature_2m_max,temperature_2m_min,wind_speed_10m_max,wind_speed_10m_mean,wind_speed_10m_min,precipitation_sum&timezone=America%2FLos_Angeles&temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch"


#C3
# This section creates the class instance and runs the 3 methods

LBC = Weather(33.77,-118.19, 25, 12, 2025)
LBC.getAvgTemp()
LBC.getMaxWind()
LBC.getSumRain()

#C4
#This section creates the engine and session for the SQLalchemy ORM, then creates the table.

engine = create_engine('sqlite:///weather.db')

session1 = sessionmaker(engine)

class Base(DeclarativeBase):
    pass

class weatherTable(Base):
    __tablename__ = "weather"
    id = Column(Integer, primary_key=True, autoincrement=True)
    latitude = Column(Numeric)
    longitude = Column(Numeric)
    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    avgtemp = Column(Numeric)
    mintemp = Column(Numeric)
    maxtemp = Column(Numeric)
    avgspeed = Column(Numeric)
    minspeed = Column(Numeric)
    maxspeed = Column(Numeric)
    sumrain = Column(Numeric)
    minrain = Column(Numeric)
    maxrain = Column(Numeric)

#Table creation
Base.metadata.create_all(engine)

#C5
#This section updates the table values using SQL Alchemy ORM

with session1() as s1:
    s1.execute(delete(weatherTable))
    new_row = weatherTable(latitude=LBC.latitude,
                           longitude=LBC.longitude,
                           day=LBC.day,
                           month=LBC.month,
                           year=LBC.year,
                           avgtemp=LBC.avgtemp,
                           mintemp=LBC.mintemp,
                           maxtemp=LBC.maxtemp,
                           avgspeed=LBC.avgspeed,
                           minspeed=LBC.minspeed,
                           maxspeed=LBC.maxspeed,
                           sumrain=LBC.sumrain,
                           minrain=LBC.minrain,
                           maxrain=LBC.maxrain
                           )
    s1.add(new_row)

#C6
#This section queries the data and prints the results in a formatted way.
    stmt = s1.query(weatherTable)
    for row in stmt:
        print(f'Long Beach, CA\nLat: {row.latitude}\nLong: {row.longitude}\nDay: {row.day}\nMonth: {row.month}\nYear: {row.year}\nAvgtemp: {row.avgtemp}\nMintemp: {row.mintemp}\nMaxtemp: {row.maxtemp}\nAvgspeed: {row.avgspeed}\nMinspeed: {row.minspeed}\nMaxspeed: {row.maxspeed}\nSumrain: {row.sumrain}\nMinrain: {row.minrain}')


