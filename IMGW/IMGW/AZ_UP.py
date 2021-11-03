# -*- coding: utf-8 -*-
"""
Created on Mon Oct 25 21:21:33 2021

@author: 48604
"""

import requests, zipfile, io, os, re
import pandas as pd
import numpy as np
import geopandas, astral
import time
from astral.sun import sun
from scipy import stats 

def get_data(url):
    file = requests.get(url) 
    zip = zipfile.ZipFile(io.BytesIO(file.content))
    #download zip from IMGW archive

    url_end = url[-4:]
    #later checking if file ends with .zip or .ZIP
    
    pattern = "Meteo_(.*?)" + url_end
    substring = re.search(pattern, url).group(1)
    #pattern matching in order to name new dir properly
    
    path = r"C:/Users/48604/Documents/semestr5/PAG/pag2/Meteo/Meteo_" + substring + "/"
    #path to dir with data from specified period
    
    if os.path.isdir(path) == 0:
        os.mkdir(path)
    zip.extractall(path)
    #creating dir if it doesnt exist and unpacking data
    
    return path

def read_parametry(path_parametr):
    parametr = pd.read_csv(path_parametr, sep=';', index_col=False, encoding='cp1250')
    return parametr
    
def read_data(path_data):
    fields = ["KodSH", "ParametrSH", "Date", "Wartosc"]
    data = {}
    #column names
    for filename in os.listdir(path_data):
        dataset_name = filename[0:7]
        #date_cols = ["Date"]
        dataset_name = pd.read_csv(path_data + filename, sep=';', header=None, names=fields, index_col=False, low_memory=False)
        
        if dataset_name["Wartosc"].dtypes != np.int64:
            dataset_name["Wartosc"] = dataset_name["Wartosc"].astype('str')
            dataset_name["Wartosc"] = dataset_name["Wartosc"].str.replace(',','.').astype('float64')
        dataset_name["Date"] = pd.to_datetime(dataset_name["Date"]).dt.tz_localize("Europe/Warsaw")
        #setting proper separator and variable type for column "Wartosc" and "Data"
        
        data[filename] = dataset_name
    return data

def read_effacility(path_effacility):
    path = open(path_effacility)
    effacility = geopandas.read_file(path)
    effacility["geometry"] = effacility["geometry"].to_crs(epsg=4258)
    x = effacility["geometry"].x
    y = effacility["geometry"].y
    data = {"KodSH" : effacility["name"], "City" : effacility["name1"], "Lon" : x, "Lat" : y}
    effacility = pd.DataFrame(data)
    effacility["KodSH"] = effacility["KodSH"].astype('float64')
    return effacility
    
def init_mean(data):
    init_mean = {}
    for key in data:
        init_mean[key] = data[key].groupby(["KodSH", data[key]["Date"].dt.date])["Wartosc"].mean()
        init_mean[key] = init_mean[key].to_frame()
        init_mean[key].rename(columns = {"Wartosc" : "24h"}, inplace=True)        
    return init_mean

def sun_info(init_mean, effacility):
    sun_info = {}
    for key in init_mean:
        init_mean[key] = init_mean[key].reset_index("Date")
        #Date as a non index value
        init_mean[key] = init_mean[key].drop(["24h"], axis=1)
        sun_info[key] = pd.merge(init_mean[key], effacility, on = "KodSH", how = "left")
    
    astral_info = {}
    for key in sun_info:
        Dawn = []
        Dusk = []
        for k in sun_info[key].index:
            City = astral.LocationInfo(sun_info[key]["City"][k],"Poland", "Europe/Warsaw", sun_info[key]["Lat"][k], sun_info[key]["Lon"][k])
            Dawn.append((sun(City.observer, date=sun_info[key]["Date"][k], tzinfo=City.timezone))["dawn"])
            Dusk.append((sun(City.observer, date=sun_info[key]["Date"][k], tzinfo=City.timezone))["dusk"])
        data = {"KodSH" : sun_info[key]["KodSH"], "Dawn" : Dawn ,"Dusk" : Dusk}
        astral_info[key] = pd.DataFrame(data)
        sun_info[key] = pd.merge(sun_info[key], astral_info[key], left_index=True, right_index=True)
        sun_info[key].drop(["KodSH_y"], axis=1, inplace=True)
        sun_info[key].rename(columns = {"KodSH_x" : "KodSH", "Date" : "Date"}, inplace=True)
        sun_info[key]["Date"] = pd.to_datetime(sun_info[key]["Date"]).dt.tz_localize("Europe/Warsaw")
        
    return sun_info
            
def day_night(data, sun_info):
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
        #bringing back proper "Date" value

        day_night[key]["day/night"] = np.where((day_night[key]["Date"] >= day_night[key]["Dawn"]) & (day_night[key]["Date"] < day_night[key]["Dusk"]), "day", "night")
        #add column which determins if its day or night
    return day_night

def analysis(sun_info, day_night):
    analysis = sun_info
    
    mean = {}
    mean_day = {}
    mean_night = {}
    
    median = {}
    median_day = {}
    median_night = {}
    
    for key in day_night:
        mean[key] = day_night[key].groupby(["KodSH", day_night[key]["Date"].dt.date, day_night[key]["day/night"].fillna('tmp')])["Wartosc"].mean()
        mean[key].to_frame
        mean[key] = mean[key].reset_index()
        pd.merge(mean[key], day_night[key], on = ["KodSH", "Date"], how="inner")
        
        mean_day[key] = mean[key][mean[key]["day/night"] != "night"]
        mean_night[key] = mean[key][mean[key]["day/night"] != "day"]
        
        median[key] = day_night[key].groupby(["KodSH", day_night[key]["Date"].dt.date, day_night[key]["day/night"].fillna('tmp')])["Wartosc"].median()
        median[key].to_frame
        median[key] = median[key].reset_index()
        
        median_day[key] = median[key][median[key]["day/night"] != "night"]
        median_night[key] = median[key][median[key]["day/night"] != "day"]
        
    return mean_day, mean_night, median_day, median_night
#def main():
start_time = time.time()

url = "https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/2018/Meteo_2018-09.zip"

path_parametry = r"C:\Users\48604\Documents\semestr5\PAG\pag2\Meteo\kody_parametr.csv"
path_data = r"C:/Users/48604/Documents/semestr5/PAG/pag2/Meteo/Meteo_2017-09/"
#path_data = get_data(url)
path_effacility = r"C:/Users/48604/Documents/semestr5/PAG/pag2/Dane/effacility.geojson"

parametry = read_parametry(path_parametry)
data = read_data(path_data)
effacility = read_effacility(path_effacility)

init_mean = init_mean(data)
sun_info = sun_info(init_mean, effacility)
day_night = day_night(data, sun_info)
mean_day, mean_night, median_day, median_night = analysis(sun_info, day_night)

print("--- %s seconds ---" % (time.time() - start_time))
#if __name__ == "__main__":
#    main()
