from datetime import date
from datetime import timedelta
import pandas as pd
import requests
import time
import os.path

munis = { 'KBOS':'Boston', 
          'KORH':'Worcester', 
          'KCEF':'Chicopee',
          'KLWM':'Lawrence', 
          'KEWB':'New Bedford',
          'KOWD':'Norwood',
          'KPYM':'Carver',
          'KBAF':'Westfield',
          'KBED':'Bedford',
          'KPSF':'Pittsfield' }

def dateformat(d):
    return "{}-{}-{}".format(d.year, d.month, d.day)

def fname(region, year):
    return 'data/{}-{}.csv'.format(region, year)

def fetch_data(region, startdate, enddate):
    frames = []
    delta = timedelta(days=1)
    for year in range(startdate.year, enddate.year + 1):
        if not os.path.isfile(fname(region, year)):
            with open(fname(region, year), 'w') as wfile:
                url = "https://www.wunderground.com/history/airport/" + \
                "{}/{}/{}/{}/CustomHistory.html?".format(region, year, 1, 1) + \
                "dayend={}&monthend={}&yearend={}&format=1".format(31, 12, year)
                csv_data = requests.get(url).text.replace("<br />", "").strip(' \t\n\r')
                wfile.write(csv_data)
                time.sleep(2)
                
        frame = pd.read_csv(fname(region, year))
        if startdate.year == year:
            d = startdate
            while dateformat(d) not in frame.EST.values and d <= enddate:
                d += delta
            if d <= enddate:
                startidx = frame[frame.EST==dateformat(d)].index[0]
                frame = frame[frame.index >= startidx]
            else:
                continue
        if enddate.year == year:
            d = enddate
            while dateformat(d) not in frame.EST.values and d >= startdate:
                d -= delta
            if d >= startdate:
                endidx = frame[frame.EST==dateformat(d)].index[0]
                frame = frame[frame.index <= endidx]
            else:
                continue
        frames.append(frame)
    return pd.concat(frames).set_index("EST")

def dictdf(munis, startdate, enddate):
    data_dict = {}
    missing_dates = set()
    delta = timedelta(days=1)

    if type(munis) == str:
        munis = [munis]

    for muni in munis:
        data_dict[muni] = fetch_data(muni, startdate, enddate)
        d = startdate
        while d <= enddate:
            if dateformat(d) not in data_dict[muni].index:
                missing_dates.add(d)
            d += delta
            
    # delete missing dates from every df
    for muni, df in data_dict.items():
        for d in missing_dates:
            if dateformat(d) in df.index:
                df = df[df.index != dateformat(d)]
        data_dict[muni] = df
    return data_dict