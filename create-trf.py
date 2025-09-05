import os, sys
import json

colormap = {
    'minValue': "0.0",
    'maxValue': "1.0",
    'r': '255',
    'g': '255',
    'b': '255',
    'description': 'Description'
}

properties = {
    'eventSetID': '000',
    'sevModelID': '000',
    'userID': '5',
    'eventName': 'Event Name as of $date',
    'eventDesc': 'Event Description',
    'startTS': '$start_date 00:00:00',
    'endTS': '$end_date 00:00:00',
    'scalingFactor': '0.01',
    'threshold': '1',
    'refIntensity': '1',
    'xdef': 'Xdef',
    'units': 'Units',
    'database': 'prod'
}

trf = {
    'tiffName': '$tiff_name',
    'properties': properties,
    'colormap': [colormap for i in range(0, 5)]
}

with open('temp.trf', 'w') as f:
    f.write(json.dumps(trf, indent=4))
