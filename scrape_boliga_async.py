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

import pandas as pd
import re
import numpy as np
import requests
import yaml
from bs4 import BeautifulSoup
from urllib.request import urlopen
import time
from unsync import unsync
pd.set_option('expand_frame_repr', False)

# Time scraping process
start_time = time.time()

# Get the number of pages to scrape to get full dataset
payload = {'salesDateMin': 2018,
           'zipcodeFrom': 2960,
           'zipcodeTo': 2970,
           'street': '',
           'saleType': '',
           'page': 1,
           'sort': 'date-d',
           'propertyType': ''}
headers = {'User-Agent': 'Mozilla/5.0',
                'Referer': 'https://www.boliga.dk/salg/resultater?salesDateMin='+str(payload['salesDateMin'])+'&zipcodeFrom='+str(payload['zipcodeFrom'])+'&zipcodeTo='+str(payload['zipcodeTo'])+'&street=&saleType=&page=1&sort=date-d&propertyType='}
prop_req_init = requests.get('https://api.boliga.dk/api/v2/sold/search/results?salesDateMin='+str(payload['salesDateMin'])+'&zipcodeFrom='+str(payload['zipcodeFrom'])+'&zipcodeTo='+str(payload['zipcodeTo'])+'&saleType=&page=1&sort=date-d&street=',
                             data=payload,
                             headers=headers,
                             timeout=5)
if prop_req_init.status_code == 200:
    
    # Get total number of pages to scrape
    properties_meta = yaml.load(prop_req_init.text)
    no_pages = properties_meta['meta']['totalPages']
    print(f"Total number of pages to scrape: {no_pages}")

    # Generate all urls to scrape
    all_urls = list()
    def generate_urls():
        for page in range(1, int(no_pages)+1):
            all_urls.append('https://api.boliga.dk/api/v2/sold/search/results?salesDateMin='+str(payload['salesDateMin'])+'&zipcodeFrom='+str(payload['zipcodeFrom'])+'&zipcodeTo='+str(payload['zipcodeTo'])+'&saleType=&page=' + str(page) + '&sort=date-d&street=')
    
    generate_urls()

    @unsync
    def scrape_boliga(url):
        # Send request, timeout is implemented to prevent re-requesting if denied
        scrape_dict = {'street': [], 'postnr': [], 'price': [], 'sales_date': [],
                       'property_type': [], 'sales_type': [], 'sqm_price': [], 
                       'room_nr': [], 'sqm': [], 'year_built': [], 
                       'price_change': [], 'settlement': [], 'lat': [], 
                       'long': [], 'lot_size': [], 'energy_class': [],
                       'ownership_expenses': [], 'basement_size': [],
                       'days_on_market': [], 'estateId': []}
        
        prop_req = requests.get(url)

        # Check if access is allowed (200 = success)
        if prop_req.status_code == 200:
            
            # Get text string from request as a dictionary
            properties_dict = yaml.load(prop_req.text)
            
            # Extract element of above dictionary to relevant lists
            for prop in range(0, len(properties_dict['results'])):

                # Extract and append to lists
                scrape_dict['street'].append(properties_dict['results'][prop]['address'])
                scrape_dict['postnr'].append(properties_dict['results'][prop]['zipCode'])
                scrape_dict['price'].append(properties_dict['results'][prop]['price'])
                scrape_dict['sales_date'].append(properties_dict['results'][prop]['soldDate'])
                scrape_dict['property_type'].append(properties_dict['results'][prop]['propertyType'])
                scrape_dict['sales_type'].append(properties_dict['results'][prop]['saleType'])
                scrape_dict['sqm_price'].append(properties_dict['results'][prop]['sqmPrice'])
                scrape_dict['room_nr'].append(properties_dict['results'][prop]['rooms'])
                scrape_dict['sqm'].append(properties_dict['results'][prop]['size'])
                scrape_dict['year_built'].append(properties_dict['results'][prop]['buildYear'])
                scrape_dict['price_change'].append(properties_dict['results'][prop]['change'])
                scrape_dict['settlement'].append(properties_dict['results'][prop]['city'])
                scrape_dict['lat'].append(properties_dict['results'][prop]['latitude'])
                scrape_dict['long'].append(properties_dict['results'][prop]['longitude'])
                scrape_dict['estateId'].append(properties_dict['results'][prop]['estateId'])

                # Get historical and other data on properties through a normal html scrape
                if properties_dict['results'][prop]['estateId'] != 0:
                    
                    # The url should be https://www.boliga.dk/bolig/'estateId'
                    try: # If a website does not work
                        url_old = 'https://www.boliga.dk/bolig/'+str(properties_dict['results'][prop]['estateId'])
                        html = urlopen(url_old)
                        soup = BeautifulSoup(html, 'lxml')
        
                        # Get boxed data
                        span_box_data= soup.find_all('span', class_='d-md-none my-auto')
                        # The sequence of variables is: outer_sqm, lot_size, no_rooms, floor, year_built,
                        #                               energy_class, ownership_expenses, basement_size,
                        #                               sqm_tinglyst
                        scrape_dict['lot_size'].append(float(re.findall('([^\s]+)', span_box_data[1].get_text())[0].replace('.', '')))
                        scrape_dict['energy_class'].append(span_box_data[5].get_text().strip())
                        scrape_dict['ownership_expenses'].append(float(re.findall('\d+', span_box_data[6].get_text().replace('.', ''))[0]))
                        scrape_dict['basement_size'].append(float(re.findall('\d+', span_box_data[7].get_text())[0]))
                        # Get time on market
                        span_time_on_market = soup.find_all('span', class_='text-primary h5 h-md-4 m-0')
                        time_on_market = re.findall('\s(.*)\spå markedet', span_time_on_market[0].get_text())
                        # Average month length: 30.42 (I need this to convert to days)
                        if 'dage' in time_on_market[0]:
                            scrape_dict['days_on_market'].append(float(re.findall('\d+', time_on_market[0])[0]))
                        elif 'måneder' in time_on_market[0]:
                            scrape_dict['days_on_market'].append(float(re.findall('\d+', time_on_market[0])[0])*30.42)
                        elif 'år' in time_on_market[0]:
                            scrape_dict['days_on_market'].append(float(re.findall('\d+', time_on_market[0])[0])*365.25)
                        else:
                            scrape_dict['days_on_market'].append(np.nan)
                    except:
                        scrape_dict['lot_size'].append(np.nan)
                        scrape_dict['energy_class'].append(np.nan)
                        scrape_dict['ownership_expenses'].append(np.nan)
                        scrape_dict['basement_size'].append(np.nan)
                        scrape_dict['days_on_market'].append(np.nan)
                        
                else:
                    scrape_dict['lot_size'].append(np.nan)
                    scrape_dict['energy_class'].append(np.nan)
                    scrape_dict['ownership_expenses'].append(np.nan)
                    scrape_dict['basement_size'].append(np.nan)
                    scrape_dict['days_on_market'].append(np.nan)
        else:
            return([np.nan])

        return ([scrape_dict])

    realestate_list = []

    for i, url in enumerate(all_urls):
        realestate_list.append(scrape_boliga(url).result())
        print(f"Page scraped: {i+1} / {no_pages}")

    # Merge list of dict together into one dict
    realestate = {}
    for i in range(len(realestate_list)):
        if (i == 0):
            realestate = realestate_list[0][0] # initial list element
        else:
            realestate = {key: value + realestate_list[i][0][key] for key, value in realestate.items()}

    # Time scraping process
    time_async_scrape = time.time() - start_time
    print(f"It took {time_async_scrape} seconds to scrape.")

    # Create dataframe from extractions
    df = pd.DataFrame(realestate)
    
    # Update df with additional columns
    def get_street_num(x):
        if len(re.findall(',', x)) == 1:
            return re.findall(".*(\\W\\w+),", x)[0].strip()
        elif len(re.findall(',', x)) == 0: 
            return re.findall(".*(\\W\\w+)", x)[0].strip()
        else:
            return np.nan
    df['street_num'] = df.apply(lambda x: get_street_num(x['street']), axis=1)
    
    def get_floor(x):
        if x['property_type'] == 3: # appartment
            if len(re.findall(',', x['street'])) == 1:
                return re.findall("[,]\s(.*)", x['street'])[0]
            elif len(re.findall(',', x['street'])) == 0:
                return np.nan
        else:
            return np.nan
    df['floor'] = df.apply(lambda x: get_floor(x), axis=1)
    
    # clean street name
    df['street'] = df.apply(lambda x: re.findall("^(.*?)\s[0-9]", x['street'])[0], axis=1)
    
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
    output_df = df[['estateId', 'street', 'street_num', 'floor', 'postnr', 
                    'settlement', 'lat', 'long', 'property_type', 
                    'sales_type', 'sqm', 'sqm_price', 'year_built', 
                    'sales_date', 'lot_size', 'energy_class', 
                    'ownership_expenses', 'basement_size', 'days_on_market',
                    'price_change', 'price']]
    
    # Output to csv file - no commas left in variable values so CSV is an ok format
    output_df.to_csv('C:/Users/atyr/OneDrive - Novo Nordisk/Private/scrape_boliga/scraped_data/hoersholm_rungsted_20200811.csv')
    # output_df.to_csv('~/Documents/DataScience/Python/scrape_boliga/hoersholm_rungsted_2018-20200721.csv')

plt_df = output_df[(output_df.property_type == "Villa") &
                   (output_df.days_on_market.notnull()) &
                   (output_df.sales_type == "Alm. Salg")]

sns.distplot(plt_df.days_on_market, kde=False, bins=30)
plt.title("Days on Market - Villas in Hoersholm and Rungsted")                   
plt.xlabel("Days on Market")
plt.ylabel("Frequency")