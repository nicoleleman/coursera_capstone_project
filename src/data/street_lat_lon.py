import pandas as pd
import numpy as np
import json
import csv
import re
from bs4 import BeautifulSoup
from geopy.geocoders import Nominatim

import requests
from pandas.io.json import json_normalize

ppd_2019 = pd.read_csv('../../data/external/pp-2019.csv')
ppd_2019.columns = ['TUID', 'Price', 'Date_of_Transfer', 'Postcode', 'Property_Type', 'Old_New', 'Duration',
                    'PAON', 'SAON', 'Street', 'Locality', 'Town_City', 'District', 'County', 'PPD_Cat_Type', 'Record_Status']
ppd_2019.sort_values(by=['Date_of_Transfer'], ascending=False, inplace=True)

ppd_2019_clean = ppd_2019.drop(columns=['TUID', 'Duration', 'PAON', 'SAON', 'Locality', 'PPD_Cat_Type', 'Record_Status'])

ppd_london = ppd_2019_clean[ppd_2019['Town_City']=='LONDON']

ppd_grouped = ppd_london.groupby(['Street'])['Price'].mean().round(0).reset_index()
ppd_grouped.columns = ['street', 'avg_price']

with open ('../../data/processed/grouped_prices.csv', 'w', encoding='utf-8', newline='') as f:
    column_headers = ['street', 'avg_price', 'latitude', 'longitude']
    writer = csv.DictWriter(f, fieldnames=column_headers)
    writer.writeheader()
    i = 0
    geolocator = Nominatim(user_agent='london_explorer')

    for street_name in ppd_grouped['street'][:1000]:
        avg_price = ppd_grouped['avg_price'][i]
        i += 1
        latitude = geolocator.geocode(street_name).latitude
        longitude = geolocator.geocode(street_name).longitude
        print(i)
        try:
            writer.writerow({'street': street_name, 'avg_price': avg_price,
                             'latitude': latitude, 'longitude': longitude})
        except Exception as e:
            writer.writerow({'street': street_name, 'avg_price': avg_price,
                             'latitude': 'Nan', 'longitude': 'Nan'})