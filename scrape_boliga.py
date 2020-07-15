# -*- coding: utf-8 -*-
"""
Created on Mon Oct 21 13:55:39 2019

@author: ATYR

Scrape Boliga for sold properties

Improvement ideas:
    1. Configure for parallel processing - e.g. Kaggle can run 4 cores
    2. Import lat-long too and see if noise maps etc have the same info,
       then this property can be used instead for street / street_num. See
       Geodata Styrelsen and see if I can get these maps.
       I can get these maps in ArcGIS - so I just need to convert.

"""

# Get libraries
#from bs4 import BeautifulSoup
#   from urllib.request import urlopen
import pandas as pd
import re
import numpy as np
import requests
import yaml
import time
from bs4 import BeautifulSoup
from urllib.request import urlopen
pd.set_option('expand_frame_repr', False)

# Time scraping process
start_time = time.time()

# Get the number of pages to scrape to get full dataset
payload_init = {
            'salesDateMin': 2018,
            'zipcodeFrom': 2960,
            'zipcodeTo': 2970,
            'street': '',
            'saleType': '',
            'page': 1,
            'sort': 'date-d',
            'propertyType': ''}
headers_init = {'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://www.boliga.dk/salg/resultater?salesDateMin='+str(payload_init['salesDateMin'])+'&zipcodeFrom='+str(payload_init['zipcodeFrom'])+'&zipcodeTo='+str(payload_init['zipcodeTo'])+'&street=&saleType=&page=1&sort=date-d&propertyType='}
prop_req_init = requests.get('https://api.boliga.dk/api/v2/sold/search/results?salesDateMin='+str(payload_init['salesDateMin'])+'&zipcodeFrom='+str(payload_init['zipcodeFrom'])+'&zipcodeTo='+str(payload_init['zipcodeTo'])+'&saleType=&page=1&sort=date-d&street=',
                             data=payload_init,
                             headers=headers_init,
                             timeout=5)
if prop_req_init.status_code == 200:
    
    # Get total number of pages to scrape
    properties_meta = yaml.load(prop_req_init.text)
    no_pages = properties_meta['meta']['totalPages']

    #no_pages = 1
    # Create initial lists to hold variable information
    street = []
    property_type = []
    postnr = []
    settlement = []
    price = []
    sales_date = []
    sqm_price = []
    room_nr = []
    sqm = []
    year_built = []
    price_change = []
    sales_type = []
    lot_size = []
    energy_class = []
    ownership_expenses = []
    basement_size = []
    days_on_market = []
    latitude = []
    longitude = []
    
    i = 1
    # Load all pages of the site
    for page in range(1, int(no_pages)+1):
        # Package set up for requestion data
        payload = {
                'salesDateMin': 2018,
                'zipcodeFrom': 2960,
                'zipcodeTo': 2970,
                'street': '',
                'saleType': '',#1,
                'page': page,
                'sort': 'date-d',
                'propertyType': ''}#1}
        # Header seems to be needed to get data
        headers = {'User-Agent': 'Mozilla/5.0',
                   'Referer': 'https://www.boliga.dk/salg/resultater?salesDateMin='+str(payload['salesDateMin'])+'&zipcodeFrom='+str(payload['zipcodeFrom'])+'&zipcodeTo='+str(payload['zipcodeTo'])+'&street=&saleType=&page='+str(page)+'&sort=date-d&propertyType='}
        # Send request, timeout is implemented to prevent re-requesting if denied
        prop_req = requests.get('https://api.boliga.dk/api/v2/sold/search/results?salesDateMin='+str(payload['salesDateMin'])+'&zipcodeFrom='+str(payload['zipcodeFrom'])+'&zipcodeTo='+str(payload['zipcodeTo'])+'&saleType=&page='+str(page)+'&sort=date-d&street=',
                                data=payload,
                                headers=headers,
                                timeout=5)
        # Check if access is allowed (200 = success)
        if prop_req.status_code == 200:
            # Get text string from request as a dictionary
            properties_dict = yaml.load(prop_req.text)
            
            # Extract element of above dictionary to relevant lists
            for prop in range(0, len(properties_dict['results'])):
                # Extract and append to lists
                street.append(properties_dict['results'][prop]['address'])
                postnr.append(properties_dict['results'][prop]['zipCode'])
                price.append(properties_dict['results'][prop]['price'])
                sales_date.append(properties_dict['results'][prop]['soldDate'])
                property_type.append(properties_dict['results'][prop]['propertyType'])
                sales_type.append(properties_dict['results'][prop]['saleType'])
                sqm_price.append(properties_dict['results'][prop]['sqmPrice'])
                room_nr.append(properties_dict['results'][prop]['rooms'])
                sqm.append(properties_dict['results'][prop]['size'])
                year_built.append(properties_dict['results'][prop]['buildYear'])
                price_change.append(properties_dict['results'][prop]['change'])
                settlement.append(properties_dict['results'][prop]['city'])
                latitude.append(properties_dict['results'][prop]['latitude'])
                longitude.append(properties_dict['results'][prop]['longitude'])
                
                # Get historical and other data on properties through a normal html scrape
                if properties_dict['results'][prop]['estateId'] != 0:
                    # The url should be https://www.boliga.dk/bolig/'estateId'
                    try: # If a website does not work
                        url = 'https://www.boliga.dk/bolig/'+str(properties_dict['results'][prop]['estateId'])
                        html = urlopen(url)
                        soup = BeautifulSoup(html, 'lxml')
        
                        # Get boxed data
                        span_box_data= soup.find_all('span', class_='d-md-none my-auto')
                        # The sequence of variables is: outer_sqm, lot_size, no_rooms, floor, year_built,
                        #                               energy_class, ownership_expenses, basement_size,
                        #                               sqm_tinglyst
                        lot_size.append(float(re.findall('([^\s]+)', span_box_data[1].get_text())[0].replace('.', '')))
                        energy_class.append(span_box_data[5].get_text().strip())
                        ownership_expenses.append(float(re.findall('\d+', span_box_data[6].get_text().replace('.', ''))[0]))
                        basement_size.append(float(re.findall('\d+', span_box_data[7].get_text())[0]))
    
                        # Get time on market
                        span_time_on_market = soup.find_all('span', class_='text-primary h5 h-md-4 m-0')
                        time_on_market = re.findall('\s(.*)\spå markedet', span_time_on_market[0].get_text())
                        # Average month length: 30.42 (I need this to convert to days)
                        if 'dage' in time_on_market[0]:
                            days_on_market.append(float(re.findall('\d+', time_on_market[0])[0]))
                        elif 'måneder' in time_on_market[0]:
                            days_on_market.append(float(re.findall('\d+', time_on_market[0])[0])*30.42)
                        elif 'år' in time_on_market[0]:
                            days_on_market.append(float(re.findall('\d+', time_on_market[0])[0])*365.25)
                        else:
                            days_on_market.append(np.nan)
                    except:
                        lot_size.append(np.nan)
                        energy_class.append(np.nan)
                        ownership_expenses.append(np.nan)
                        basement_size.append(np.nan)
                        days_on_market.append(np.nan)
                        
                else:
                    lot_size.append(np.nan)
                    energy_class.append(np.nan)
                    ownership_expenses.append(np.nan)
                    basement_size.append(np.nan)
                    days_on_market.append(np.nan)
        else:
            break
    
        print("Page scraped: (",page,"/",no_pages,")")
        # print("Page scraped: (",i,"/",no_pages,")")
        i += 1
    
    # Create dataframe from extractions
    df = pd.DataFrame({'street': street,
                       'property_type': property_type,
                       'postnr': postnr,
                       'settlement': settlement,
                       'price': price,
                       'sales_date': sales_date,
                       'sqm': sqm,
                       'sqm_price': sqm_price,
                       'sales_type': sales_type,
                       'year_built': year_built,
                       'price_change': price_change,
                       'lot_size': lot_size,
                       'energy_class': energy_class,
                       'ownership_expenses': ownership_expenses,
                       'basement_size': basement_size,
                       'days_on_market': days_on_market,
                       'latitude': latitude,
                       'longitude': longitude})
    
    # Time scraping process
    time_async_scrape = time.time() - start_time
    print(f"It took {time_async_scrape} seconds to scrape.")

    # Update df with additional columns
    # street number
    def get_street_num(x):
        if len(re.findall(',', x)) == 1:
            return re.findall(".*(\\W\\w+),", x)[0].strip()
        elif len(re.findall(',', x)) == 0: 
            return re.findall(".*(\\W\\w+)", x)[0].strip()
        else:
            return np.nan
    df['street_num'] = df.apply(lambda x: get_street_num(x['street']), axis=1)
    
    # if an appartment then a floor value
    def get_floor(x):
        if x['property_type'] == 3:
            if len(re.findall(',', x['street'])) == 1:
                return re.findall("[,]\s(.*)", x['street'])[0]
            elif len(re.findall(',', x['street'])) == 0:
                return np.nan
        else:
            return np.nan
    df['floor'] = df.apply(lambda x: get_floor(x), axis=1)
    
    # clean street name
    df['street'] = df.apply(lambda x: re.findall("^(.*?)\s[0-9]", x['street'])[0], axis=1)
    
    # Encode property type to human language
    def assign_property_type(x):
        if x['property_type'] == 1:
            return 'Villa'
        elif x['property_type'] == 2:
            return 'Raekkehus'
        elif x['property_type'] == 3:
            return 'Ejerlejlighed'
        elif x['property_type'] == 4:
            return 'Fritidshus'
        else:
            return 'Landejendom'
    df['property_type'] = df.apply(lambda x: assign_property_type(x), axis=1)
    
    # Set up output dataframe with variable in wanted order
    output_df = df[['street', 'street_num', 'floor', 'postnr', 'settlement',
                    'property_type', 'sales_type', 'sqm', 'sqm_price', 
                    'year_built', 'sales_date', 'lot_size', 'energy_class', 
                    'ownership_expenses', 'basement_size', 'days_on_market',
                    'latitude', 'longitude', 'price_change', 'price']]
    
    # Output to csv file - no commas left in variable values so CSV is an ok format
    # output_df.to_csv('C:/Users/atyr/OneDrive - Novo Nordisk/Python/private/scrape_boliga/boliga_scrape_rungsted_horsholm_20200616.csv')



"""
# I think that perhaps BeautifulSoup may be the best tool for this task
# Send request, timeout is implemented to prevent re-requesting if denied
prop_req = requests.get('https://www.boliga.dk/bolig/1581004/lyneborggade_8_st_tv')
# Check if access is allowed (200 mean success)
if prop_req.status_code == 200:
    # Get text string from request as a dictionary
    properties_dict = yaml.load(prop_req.text)
"""

"""
# Get historical and other data on properties through a normal html scrape
# The url should be https://www.boliga.dk/bolig/'estateId'
from bs4 import BeautifulSoup
from urllib.request import urlopen

for i in range(0, 50):
    print(properties_dict['results'][i]['estateId'])

# Scrape vta
url_test = 'https://www.boliga.dk/bolig/1594371'#/lyneborggade_8_st_tv'
html_test = urlopen(url_test)
soup_test = BeautifulSoup(html_test, 'lxml')

# Get day and menu as bs elements
span_box_data= soup_test.find_all('span', class_='d-md-none my-auto')
# The sequence of variables is: outer_sqm, lot_size, no_rooms, floor, year_built,
#                               energy_class, ownership_expenses, basement_size,
#                               sqm_tinglyst
span_time_on_market = soup_test.find_all('span', class_='text-primary h5 h-md-4 m-0')

# Testing regex for extraction of data needed - example
test = re.findall('\s(.*)\spå markedet', span_time_on_market[0].get_text())
# Average month length: 30.42 (I need this to convert to days)
if 'dage' in test[0]:
    days_on_market = 'dage'
elif 'måneder' in test[0]:
    days_on_market = 'måneder'
elif 'år' in test[0]:
    days_on_market = 'år'
else:
    days_on_market = 'other'
print(days_on_market)
"""