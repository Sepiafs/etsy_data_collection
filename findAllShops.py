#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  4 20:10:35 2019

@author: sepideh
"""

#findAllShops
#HTTP Method: 	GET

import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import time

client_key = '46puysko4of5zqc8ub6o2yfn'
client_secret = 'qeb6c12is9'
oauth_key_perm = '61099704c68546399831c037b4003a'  #access key
oauth_secret_perm = 'c88eba327c'                   #acess secret

def etsy_scrap(url):
    response = requests.get(url)
    print(response)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    #Extract sales_count
    one_tag = soup.find('a', href=True, attrs={'class':'text-gray-lighter'}, rel="nofollow")
    if one_tag != None:
        sales_count = int(str(one_tag.string).split()[0])
    else:
        one_tag = soup.find('span', attrs={'class':'mr-lg-2 pr-lg-2 br-lg-1'})
    if one_tag != None:
        sales_count = int(str(one_tag.string).split()[0])
    else:
        one_tag = soup.find('span', attrs={'class':'shop-sales hide-border no-wrap'})
    if one_tag != None:
        sales_count = int(str(one_tag.string).split()[0]) 
    else:
        sales_count = -99

    
    #Extract review_count
    two_tag = soup.find('script', attrs={'type': 'application/ld+json'})
    #convert the find result in string to JSON
    if two_tag != None:
        json_two_tag = json.loads(two_tag.string)
        #this is a nested JSON, extract the reviewCount
        if 'aggregateRating' in json_two_tag.keys():
            review_count = int(json_two_tag.get('aggregateRating').get('reviewCount'))
        else:
            review_count = 0
    else:
        review_count = -99
    
    #Extract shop_location    
    three_tag = soup.find('span', attrs={'class': 'shop-location mr-lg-2 pr-lg-2 br-lg-1'})
    if three_tag != None:
        shop_location = str(three_tag.string)
    else:
        shop_location = 'None'    
    
    
    return {'sales_count':sales_count, 'review_count':review_count, 'shop_location':shop_location}

offset = 30000
limit = 100

#List of features to be collected
shop_name = []
creation_tsz = []  # The date and time the shop was created, in epoch seconds.
listing_active_count = [] # The number of active listings in the shop.
num_favorers = [] # The number of members who've marked this Shop as a favorite
currency_code = [] # The ISO code (alphabetic) for the seller's native currency.
is_shop_us_based = [] 
sale_message = [] #A message that is sent to users who buy from this shop.
scrap_data = []  # sales_count and review count

for _ in range(1,201):
    uri = "https://openapi.etsy.com/v2/shops"
    parameters = {"api_key":client_key, "limit": limit, "offset": offset}
    response = requests.get(uri, params=parameters)
    # Print the status code of the response.
    if response.status_code == 200:
        print(response.status_code)
        shop_data = response.json()
        
        # append the shop data into corresponding lists
        shop_name.append([shop['shop_name'] for shop in shop_data['results']])
        creation_tsz.append([shop['creation_tsz'] for shop in shop_data['results']])
        listing_active_count.append([shop['listing_active_count'] for shop in shop_data['results']])
        num_favorers.append([shop['num_favorers'] for shop in shop_data['results']])
        currency_code.append([shop['currency_code'] for shop in shop_data['results']])
        is_shop_us_based.append([shop['is_shop_us_based'] for shop in shop_data['results']])
        sale_message.append([shop['sale_message'] for shop in shop_data['results']])
    
    
        #extract shop url
        shop_url = [shop['url'] for shop in shop_data['results']]
        for url in shop_url:
            scrap_data.append(etsy_scrap(url))
            
        offset = offset + limit
    else:
        print('reponse status error')
    
# some post processing of the collected data
#concatenate the list of lists
shop_name = sum(shop_name, [])    
creation_tsz = sum(creation_tsz, [])   
listing_active_count = sum(listing_active_count, [])   
num_favorers = sum(num_favorers, [])   
currency_code = sum(currency_code, [])   
is_shop_us_based = sum(is_shop_us_based, [])   
sale_message = sum(sale_message, []) 
#Extract the following features from scrap_data
sales_count = [item['sales_count'] for item in scrap_data] 
review_count = [item['review_count'] for item in scrap_data] 
shop_location = [item['shop_location'] for item in scrap_data] 

#convert the time from epoch format to month and year
creation_date = [time.strftime('%Y-%m', time.localtime(i)) for i in creation_tsz]
    
dict = {'shop_name':shop_name, 'creation_date':creation_date, 'listing_active_count':listing_active_count, 
                           'num_favorers':num_favorers, 'currency_code':currency_code, 'is_shop_us_based':is_shop_us_based,
                           'is_shop_us_based':is_shop_us_based, 'sale_message':sale_message, 'sales_count':sales_count,
                           'review_count':review_count, 'shop_location':shop_location}



df = pd.DataFrame(dict)

df.to_csv('etsy_dataset.csv', encoding='utf-8')
