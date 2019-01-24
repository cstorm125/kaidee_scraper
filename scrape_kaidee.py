#imports
from bs4 import BeautifulSoup
import urllib
import pandas as pd
import numpy as np
from datetime import datetime
import os
from tqdm import trange

#path
data_path = 'data/'

#utility
def get_soup(url):
    #read html
    f = urllib.request.urlopen(url)
    page_bytes = f.read()
    page = page_bytes
    f.close()

    #parse
    soup = BeautifulSoup(page,'html.parser')
    return(soup)

#parameters
max_pages = 800
idx = [10] #[0,1,10,20,21] 
cat_df = pd.read_csv(f'{data_path}cat_df.csv')
big_cat_df = pd.read_csv(f'{data_path}big_cat_df.csv')
df = big_cat_df.merge(cat_df.iloc[idx,:1],how='inner')

#scrape
for _,row in df.iterrows():
    sub_cat_url = row.sub_cat_url
    sub_cat_name = row.sub_cat_name
    print(f'scraping {sub_cat_name} for {datetime.today()}')
    
    listing_dicts = []
    for i in trange(max_pages):
        for condition in [1,2]: #new and old
            page = get_soup(f'{sub_cat_url}p-{i+1}?condition={condition}')
            listings = page.find_all('a',class_='crow')
            #iterate through all listings
            for listing in listings:
                #skip if doesn't exist
                if (listing.find('meta') is None) or (listing.find('img') is None): continue
                #dict
                listing_name = listing.find('img')['alt']
                listing_url = f"https://www.kaidee.com{listing['href']}"
                listing_price = listing.find('meta', itemprop='price')['content']
                listing_region = listing.find('meta', itemprop='addressRegion')['content']
                listing_locality = listing.find('meta', itemprop='addressLocality')['content']
                listing_lat = listing.find('meta', itemprop='latitude')['content']
                listing_lon = listing.find('meta', itemprop='longitude')['content']
                listing_dict = {
                    'sub_cat_name': sub_cat_name.replace(' ','_'),
                    'page': i+1,
                    'condition': condition,
                    'listing_name': listing_name.lower(),
                    'listing_url': listing_url,
                    'listing_price':listing_price,
                    'listing_region':listing_region,
                    'listing_locality':listing_locality,
                    'listing_lat':listing_lat,
                    'listing_lon':listing_lon
                } 
                listing_dicts.append(listing_dict)

        #save after x page done
        if i % 100==0:
            print(f'saving {sub_cat_name} at page {i}')
            listing_df = pd.DataFrame(listing_dicts)[['sub_cat_name','page','condition',
                                                  'listing_name',
                                                  'listing_price',
                                                  'listing_url',
                                                  'listing_region',
                                                  'listing_locality',
                                                  'listing_lat','listing_lon']]
            today = str(datetime.today())[:10].replace('-','')
            fname = f"{data_path}{today}_{sub_cat_name.replace(' ','_')}.csv"
            listing_df.to_csv(fname,index=False)
