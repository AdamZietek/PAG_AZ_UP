import pandas as pd
import numpy as np
import time
from tabulate import tabulate
from pymongo import MongoClient
from datetime import datetime, timedelta

connection = MongoClient("localhost", 27017) 
db = connection.local
collection = db

def get_collection_names(collection):
    coll_names = collection.list_collection_names()
    return coll_names

def ticks_to_datetime(ticks):
    return datetime(1, 1, 1) + timedelta(microseconds=ticks / 10)

def get_IMGW(coll_names):
    data = {}
    collection_names = [c for c in coll_names if c.startswith("IMGW")]
    for i in collection_names:
        value_init = collection[i].find({}, {"_id":0})
        value = pd.DataFrame(list(value_init))
        value["Date"] = pd.to_datetime(value["Date"])
        data[i[5:]] = value
    return data

def get_Sun_info(coll_names):
    data = {}
    collection_names = [c for c in coll_names if c.startswith("Sun_info")]
    for i in collection_names:
        value_init = collection[i].find({}, {"_id":0})
        value = pd.DataFrame(list(value_init))
        value["Date"] = pd.to_datetime(value["Date"])
        #value["Date"] = value["Date"].dt.date.dt.tz_localize("Europe/Warsaw")
        value["Dawn"] = pd.to_datetime(value["Dawn"])
        value["Dusk"] = pd.to_datetime(value["Dusk"])
        data[i[9:]] = value
    return data

def f_day_night(data, sun_info):
    day_night = {}
    for key in data:
        date_time = data[key]["Date"]
        #save old datetime
        data[key]["Date"] = data[key]["Date"].dt.date
        #trim Date of time, which is necessary to merge(unwanted conv from datetime64 to object)
        data[key]["Date"] = pd.to_datetime(data[key]["Date"]).dt.tz_localize("Europe/Warsaw")
        #conversion from object to datetime64
        day_night[key] = pd.merge(data[key], sun_info[key], on=["KodSH", "Date"], how="inner")
        #merging data with info about dusk and dawn
        data[key].drop(["Date"], axis=1, inplace=True)
        data[key].insert(2, "Date", date_time)
        day_night[key].drop(["Date"], axis=1, inplace=True)
        day_night[key].insert(2, "Date", date_time)
        #bringing back proper "Date" VALUE

        day_night[key]["day/night"] = np.where((day_night[key]["Date"] >= day_night[key]["Dawn"]) & (day_night[key]["Date"] < day_night[key]["Dusk"]), "day", "night")
        #add column which determins if its day or night
    return day_night

def f_analysis_basic(sun_info, day_night):
    analysis_basic = {}
    
    mean = {}
    mean_day = {}
    mean_night = {}
    
    median = {}
    median_day = {}
    median_night = {}
    
    for key in day_night:
        mean[key] = day_night[key].groupby(["KodSH", day_night[key]["Date"].dt.date, day_night[key]["day/night"]], dropna=False)["Wartosc"].mean()
        mean[key].to_frame
        mean[key] = mean[key].reset_index()
        #mean group by
        
        median[key] = day_night[key].groupby(["KodSH", day_night[key]["Date"].dt.date, day_night[key]["day/night"]], dropna=False)["Wartosc"].median()
        median[key].to_frame
        median[key] = median[key].reset_index()
        #median geoup by
        
        mean_day[key] = mean[key][mean[key]["day/night"] != "night"]
        mean_night[key] = mean[key][mean[key]["day/night"] != "day"]
        median_day[key] = median[key][median[key]["day/night"] != "night"]
        median_night[key] = median[key][median[key]["day/night"] != "day"]
        #selecting values for different time of day(loss of nan data)
        
        mean_day[key] = sun_info[key].merge(mean_day[key], how="left", right_on=["KodSH", "Date"], left_on=["KodSH", sun_info[key]["Date"].dt.date])
        mean_night[key] = sun_info[key].merge(mean_night[key], how="left", right_on=["KodSH", "Date"], left_on=["KodSH", sun_info[key]["Date"].dt.date])
        median_day[key] = sun_info[key].merge(median_day[key], how="left", right_on=["KodSH", "Date"], left_on=["KodSH", sun_info[key]["Date"].dt.date])
        median_night[key] = sun_info[key].merge(median_night[key], how="left", right_on=["KodSH", "Date"], left_on=["KodSH", sun_info[key]["Date"].dt.date])
        #bring nan data back
        
        mean_day[key].drop(["Date_x", "Dawn", "Dusk", "Date_y", "day/night"], axis=1, inplace=True)
        mean_night[key].drop(["Date_x", "Dawn", "Dusk", "Date_y", "day/night"], axis=1, inplace=True)
        median_day[key].drop(["Date_x", "Dawn", "Dusk", "Date_y", "day/night"], axis=1, inplace=True)
        median_night[key].drop(["Date_x", "Dawn", "Dusk", "Date_y", "day/night"], axis=1, inplace=True)
        mean_day[key].rename(columns = {"Wartosc" : "Mean_value_day"}, inplace=True)
        mean_night[key].rename(columns = {"Wartosc" : "Mean_value_night"}, inplace=True)
        median_day[key].rename(columns = {"Wartosc" : "Median_value_day"}, inplace=True)
        median_night[key].rename(columns = {"Wartosc" : "Median_value_night"}, inplace=True)
        #basic dataframe maintenance
        
        mean_day[key] = pd.concat([mean_day[key], mean_night[key]["Mean_value_night"], median_day[key]["Median_value_day"], median_night[key]["Median_value_night"]], axis=1)
        analysis_basic[key] = mean_day[key]
        
    return analysis_basic

def f_analysis_trim(sun_info, day_night):
    analysis_trim = {}
    
    return analysis_trim
    
def f_display_analysis(analysis_basic):
    hdrs = ["KodSH", "Date", "City", "Lon", "Lat", "Mean value day", "Mean value night", "Median value day", "Median value night"]
    #result = open("analysis_basic.txt", "w")
    for key in analysis_basic:
        table = tabulate(analysis_basic[key], headers = hdrs, tablefmt = 'psql')
        result = open("analysis_basic_" + key[:15] + ".txt", "w")
        result.write(table)
        result.close()

start_time = time.time()

collection_names = get_collection_names(collection)

IMGW_data = get_IMGW(collection_names)
Sun_info_data = get_Sun_info(collection_names)

day_night = f_day_night(IMGW_data, Sun_info_data)

analysis_basic = f_analysis_basic(Sun_info_data, day_night)
analysis_trim = f_analysis_trim(Sun_info_data, day_night)

f_display_analysis(analysis_basic) 

print("--- %s seconds ---" % (time.time() - start_time))
