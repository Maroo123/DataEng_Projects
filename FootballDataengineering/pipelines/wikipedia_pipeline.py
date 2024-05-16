import json

import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy import Nominatim
from geopy.geocoders import Nominatim
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderQuotaExceeded
def get_wikipedia_page(url):

    print("Getting wikipedia page...",url)


    try:
        response = requests.get(url , timeout=10)
        #check if the request is succesful
        response.raise_for_status()


        return response.text
    except requests.RequestException as e:
        print(f"An error occured : {e}")



def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')

    table = soup.find_all("table", {"class": "wikitable sortable"})[0]

    table_rows = table.find_all('tr')

    return table_rows


def clean_data(text):
    if text.find('[') != -1:
        text=text.split('[')[0]

    return text.replace('\n', '')
def extract_wikipedia_data(**kwargs):

    url = kwargs['url']
    html = get_wikipedia_page(url)
    rows = get_wikipedia_data(html)

    #print(rows)
    data = []

    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'Stadium': clean_data(tds[1].text),
            'Capacity': clean_data(tds[2].text).replace(',', '').replace('.', ''),
            'City': clean_data(tds[3].text),
            'Country': clean_data(tds[4].text),
            'Home Team(s)': clean_data(tds[5].text)
        }
        data.append(values)
    #print(data)
    #data_df = pd.DataFrame(data)
    #data_df.to_csv("data/output.csv", index=False)

    #the JSON string is being pushed so it can be used by other tasks within the workflow.
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)

    print('hhh')
    return "OK"

def get_lat_long(country, stadium):
    geolocator = Nominatim(user_agent="MyApp (contact@example.com)")
    try:
        location = geolocator.geocode(f'{stadium}, {country}')
        return (location.latitude, location.longitude) if location else None
    except GeocoderTimedOut:
        return get_lat_long(country, stadium)  # retry geocoding on timeout
    except GeocoderQuotaExceeded:
        return None  # handle quota exceeded error
    except Exception as e:
        print(f"Error geocoding {stadium}, {country}: {e}")
        return None
def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')

    data = json.loads(data)

    print(data)
    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['Country'], x['Stadium']), axis=1)
    stadiums_df['Capacity'] = stadiums_df['Capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['Country'], x['City']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())

    return "OK"

def write_wikipedia_data(**kwargs):
    from datetime import datetime
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                 + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

    data.to_csv('data/' + file_name, index=False)


