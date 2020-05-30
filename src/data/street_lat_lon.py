import pandas as pd
import numpy as np
import json
import csv
import re
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim

import requests
from pandas.io.json import json_normalize

# Read in the dataframe and assign column headers
ppd_2019 = pd.read_csv('../../data/external/pp-2019.csv')
ppd_2019.columns = ['TUID', 'Price', 'Date_of_Transfer', 'Postcode', 'Property_Type', 'Old_New', 'Duration',
                    'PAON', 'SAON', 'Street', 'Locality', 'Town_City', 'District', 'County', 'PPD_Cat_Type', 'Record_Status']
ppd_2019.sort_values(by=['Date_of_Transfer'], ascending=False, inplace=True)

# Drop features that are irrelevant for this project, filter for London rows and clean up the data
ppd_2019_clean = ppd_2019.drop(columns=['TUID', 'Duration', 'PAON', 'SAON', 'Locality', 'PPD_Cat_Type', 'Record_Status'])
ppd_london = ppd_2019_clean[ppd_2019['Town_City']=='LONDON'].copy()
ppd_london = ppd_london.drop(ppd_london[ppd_london.Price > 2000000].index)
ppd_london.dropna(axis=0, how='any', inplace=True)

# Add a new column for the postcode prefixes
ppd_london['Postcode_Prefix'] = ppd_london['Postcode'].apply(lambda x: x.split(' ')[0])

# Group bt the street name and postcode pre fixes
ppd_grouped = ppd_london.groupby(['Street', 'District', 'Postcode_Prefix'])['Price'].mean().round(0).reset_index()
ppd_grouped.columns = ['street', 'district', 'postcode_prefix', 'avg_price']
#print(ppd_grouped)

with open ('../../data/processed/ppd_london_2019_new1.csv', 'w', encoding='utf-8', newline='') as f:
    column_headers = ['street', 'district', 'postcode_prefix', 'avg_price', 'latitude', 'longitude']
    writer = csv.DictWriter(f, fieldnames=column_headers)
    writer.writeheader()
    i = 0
    geolocator = Nominatim(user_agent='london_explorer')

    for street_name in ppd_grouped['street']:
        avg_price = ppd_grouped['avg_price'][i]
        district = ppd_grouped['district'][i]
        postcode_prefix = ppd_grouped['postcode_prefix'][i]
        i += 1
        try:
            # LONDON is appended to the street name as in some cases the incorrect lat and lon were pulled
            latitude = geolocator.geocode(street_name + ', ' + postcode_prefix + ', LONDON, UK').latitude
            longitude = geolocator.geocode(street_name + ', ' + postcode_prefix + ', LONDON, UK').longitude
            print(i)

            writer.writerow({'street': street_name, 'postcode_prefix': postcode_prefix, 'district': district,
                             'avg_price': avg_price, 'latitude': latitude, 'longitude': longitude})
        except Exception as e:
            writer.writerow({'street': street_name, 'postcode_prefix': postcode_prefix, 'district': district,
                             'avg_price': avg_price, 'latitude': 'NaN', 'longitude': 'NaN'})