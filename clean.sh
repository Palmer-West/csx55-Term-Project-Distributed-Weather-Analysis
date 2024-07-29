#!/bin/bash

# Uninstall Python modules
pip3 uninstall -y googlemaps 
pip3 uninstall -y pyspark 
pip3 uninstall -y requests

# Optional: Check if uninstallation was successful
if [ $? -eq 0 ]; then
    echo "Python modules uninstalled successfully."
else
    echo "Failed to uninstall Python modules."
fi