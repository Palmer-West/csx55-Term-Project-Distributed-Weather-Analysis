#!/bin/bash

# Unload any existing modules. Change to download python 3.9.18 directly?
# module unload courses
# module load courses/cs320

# HAVE TO MANUALLY MODULE LOAD CS320
# Install Python modules
pip3 install googlemaps
# pip3 install pyspark
pip3 install requests

# Optional: Check if installation was successful
if [ $? -eq 0 ]; then
    echo -e "Python modules installed successfully.\n"
else
    echo "Failed to install Python modules."
fi

# Run the python scripts
python3 getRoutes.py  # get the JSON routes
python3 getWeather.py  # get the weather data
python3 sparkDriver.py  # merge them into dataframes

rm *.json