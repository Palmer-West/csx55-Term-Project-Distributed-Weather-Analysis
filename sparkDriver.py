import requests
import pyspark
import json
import random
import math
import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Define a spark session to use PySpark
spark = SparkSession.builder.appName("weather").getOrCreate()

def read_weather_json(weather):
    filename = weather + ".json"
    try:
        df = spark.read.option("multiline", True).json(filename)
    except:
        print("Error reading weatherJSON file")
    
    return df


def read_route_json(route):
    filename = route + ".json"
    try:
        df = spark.read.option("multiline", True).json(filename)
    except:
        print("Error reading route JSON file")
    
    return df


def analyze_weather(dataframes):
    # Grab the 7 day forecast for each dataframe
    duration_placeholder = ['str' for i in range(0, 7)]

    resort_weather_status_list = []
    for dataframe in dataframes:
        point_status = dataframe.select(col("properties.periods")).collect()
    
        dailyShortData = [
            point_status[0][0][0].shortForecast,
            point_status[0][0][24].shortForecast,
            point_status[0][0][48].shortForecast,
            point_status[0][0][72].shortForecast,
            point_status[0][0][96].shortForecast,
            point_status[0][0][120].shortForecast,
            point_status[0][0][144].shortForecast,
        ]

        precipPrecentages = [
            point_status[0][0][0][8][1],
            point_status[0][0][24][8][1],
            point_status[0][0][48][8][1],
            point_status[0][0][72][8][1],
            point_status[0][0][96][8][1],
            point_status[0][0][120][8][1],
            point_status[0][0][144][8][1]
        ]

        resort_weather_status_list.append(dailyShortData)
        resort_weather_status_list.append(precipPrecentages)
        
    resort_weather_status_list.append(duration_placeholder)
    return resort_weather_status_list


def analyze_route(dataframe):
    routes = dataframe.select(col("legs.duration.text")).collect()
    return routes # grab the first route in the list as an array of str


def calculate_percentage_slowdown(min, max, percipitation):
    step_value = (max - min) / 100
    return percipitation * step_value + min


def get_duration_to_string(duration):
    hours = math.floor(duration // 60)
    minutes = math.floor(duration % 60)
    return f'{hours} hour(s) {minutes} minutes'


def calculate_optimal_route(resort, route, weather):
    google_duration = route[0].text[0]  # base
    temp = google_duration.split(" ")
    duration = ((int(temp[0]) * 60) + int(temp[2]))
    
    new_travel_times = []
    
    i = 0
  
    percentage_slowdown = 0
    for j in range(len(weather[i])):
        new_duration = 0
        percipitation = float(weather[1][j])
        if "snow" in weather[i][j].lower():
            percentage_slowdown = calculate_percentage_slowdown(5.5, 7.6, percipitation)
            new_duration = duration + (duration * (percentage_slowdown / 100))
            new_travel_times.append(new_duration)
        elif "showers" in weather[i][j].lower():
            percentage_slowdown = calculate_percentage_slowdown(1.5, 3.8, percipitation)
            new_duration = duration + (duration * (percentage_slowdown / 100))
            new_travel_times.append(new_duration)
        elif "showers" in weather[i][j].lower() and "thunderstorms" in weather[i][j].lower():
            # 6 to 8
            percentage_slowdown = calculate_percentage_slowdown(4, 6, percipitation)
            new_duration = duration + (duration * (percentage_slowdown / 100))
            new_travel_times.append(new_duration)
        elif "heavy" in weather[i][j].lower() and "snow" in weather[i][j].lower():
            # 9.4 to 13.4
            percentage_slowdown = calculate_percentage_slowdown(7.4, 9.4, percipitation)
            new_duration = duration + (duration * (percentage_slowdown / 100))
            new_travel_times.append(new_duration)
        else:
            new_duration = duration
            new_travel_times.append(new_duration)

        weather[-1][j] = get_duration_to_string(new_duration)
  
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    now = datetime.datetime.now()
    day_of_the_week = now.weekday()
    columns = days[day_of_the_week:] + days[:day_of_the_week]

    weather_and_route = weather
    df = spark.createDataFrame(weather_and_route, columns)
    print(f"Here is a look at the week ahead for {resort} and how long it would take: ")
    df.show() # df.show(truncate=False)


def analysis(dictionary):
    for resort in dictionary:
        route_df = dictionary[resort][0]
        weather_df = dictionary[resort][1:]  # Because weather could be more than one dataframe
        print(f"Analyzing data for {resort}...")
        calculate_optimal_route(resort, analyze_route(route_df), analyze_weather(weather_df))


def main():
    print("Entering sparkDriver.py")

    # List of route json files
    route_json_files = [
        "Aspen-Route",
        "Steamboat-Route",
        "Winter-Park-Route",
        "Copper-Mountain-Route",
        "Arapahoe-Basin-Route",
        "Eldora-Route"
    ]

    # List of weather json files
    weather_json_files = [
        "Aspen-Weather",
        "Steamboat-Weather",
        "Winter-Park-Weather",
        "Copper-Mountain-Weather",
        "Arapahoe-Basin-Weather",
        "Eldora-Weather",
        
        "Floyd-Hill-Weather", 
        "Idaho-Springs-Weather",
        "Vail-Pass-Weather",
        "Boulder-Weather",
        "Berthoud-Pass-Weather",
        "Larmie-Weather",
        "Walden-Weather"
    ]

    route_dataframes = []
    for route in route_json_files:  # Read in the route data
        route_dataframes.append(read_route_json(route))
    
    weather_dataframes = []
    for resort in weather_json_files:  # Read in the weather data
        weather_dataframes.append(read_weather_json(resort))

    floyd_hill = weather_dataframes[6]
    idaho_springs = weather_dataframes[7]
    vail_pass = weather_dataframes[8]
    boulder = weather_dataframes[9]
    berthoud_pass = weather_dataframes[10]
    larmie = weather_dataframes[11]
    walden = weather_dataframes[12]

    dataframes = []
    for i in range(6):
        dataframes.append(route_dataframes[i]) # .join(weather_dataframes[i]))
    
    dataframes_dict = {
        "Aspen": [dataframes[0], weather_dataframes[0], floyd_hill, vail_pass],
        "Steamboat": [dataframes[1], weather_dataframes[1], larmie, walden],
        "Winter Park": [dataframes[2], weather_dataframes[2], berthoud_pass],
        "Copper Mountain": [dataframes[3], weather_dataframes[3], floyd_hill, idaho_springs],
        "Arapahoe Basin": [dataframes[4], weather_dataframes[4], floyd_hill, idaho_springs],
        "Eldora": [dataframes[5], weather_dataframes[5], boulder]
    }

    # dataframes[0].join(floyd_hill).join(idaho_springs)
    # dataframes[1].join(larmie).join(walden)
    # dataframes[2].join(berthoud_pass)
    # dataframes[3].join(floyd_hill).join(idaho_springs)
    # dataframes[4].join(floyd_hill).join(idaho_springs)
    # dataframes[5].join(boulder)

    print("All datafames have been created. Performing analysis...")
    analysis(dataframes_dict)
    
    print("Exiting sparkDriver.py")

main()
