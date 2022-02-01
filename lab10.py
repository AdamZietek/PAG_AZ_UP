import pandas as pd
import numpy as np
import redis
import json
import geopandas, astral
import time
from astral.sun import sun
import requests, zipfile, io, os, re
from tabulate import tabulate

METEO_FOLDER = r"C:/Users/48604/Documents/semestr5/PAG/pag2/Meteo/" 
ZAPIS_ZIP = METEO_FOLDER + r"Meteo_"
url = "https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/2015/Meteo_2015-07.zip"

r = redis.Redis(host='localhost', port=6379, db=0)

def get_data(url, pth):
    file = requests.get(url) 
    zip = zipfile.ZipFile(io.BytesIO(file.content))
    #download zip from IMGW archive

    url_end = url[-4:]
    #later checking if file ends with .zip or .ZIP
    
    pattern = "Meteo_(.*?)" + url_end
    substring = re.search(pattern, url).group(1)
    #pattern matching in order to name new dir properly
    
    path = pth + substring + "/"
    #path to dir with data from specified period
    
    if os.path.isdir(path) == 0:
        os.mkdir(path)
    zip.extractall(path)
    #creating dir if it doesnt exist and unpacking data
    
    return path

path_data = get_data(url, ZAPIS_ZIP)
path_parametry = METEO_FOLDER + "kody_parametr.csv"
path_effacility = METEO_FOLDER + "effacility.geojson"
path_powiaty = METEO_FOLDER + "powiaty/powiaty.shp"
path_wojewodztwa = METEO_FOLDER + "woj/woj.shp"

def read_parametry(path_parametr):
    parametr = pd.read_csv(path_parametr, sep=';', index_col=False, encoding='cp1250')
        #separator=';' - by default ','
        #index_col=False - store all data as columns not indexes
    return parametr
    #function to read parameters from the path_parametr file
    
def read_data(path_data):
    fields = ["KodSH", "ParametrSH", "Date", "Wartosc"]
    data = {}
    #column names; empty dictionary for data from separate csv files in folder
    for filename in os.listdir(path_data):
        #for every file in folder
        dataset_name = pd.read_csv(path_data + filename, sep=';', header=None, names=fields, index_col=False, low_memory=False, dtype={'KodSH': int, 'Wartosc': str}, parse_dates=['Date'])
        #applying value
            #separator=';' - by default ','
            #no header by default
            #names=fields - column names
            #index_col=False - store all data as columns not indexes
            #low_memory=false - way to get rid of different datatypes in columns warning
        
        dataset_name["Wartosc"] = dataset_name["Wartosc"].str.replace(',','.').astype('float64')
        #replace ',' with '.' and convert string to float
        dataset_name["Date"] = dataset_name["Date"].dt.tz_localize("Europe/Warsaw")
        #setting "Data" column to datetime64[ns, Europe/Warsaw] from datetime64[ns]
        
        data[filename] = dataset_name
    return data
    #function to read data from the path_data file

def read_effacility(path_effacility):
    path = open(path_effacility)
    effacility = geopandas.read_file(path)
    #read geojson
    effacility["geometry"] = effacility["geometry"].to_crs(epsg=4258)
    x = effacility["geometry"].x
    y = effacility["geometry"].y
    data = {"KodSH" : effacility["name"], "City" : effacility["name1"], "Lon" : x, "Lat" : y}
    effacility = pd.DataFrame(data)
    effacility["KodSH"] = effacility["KodSH"].astype('float64')
    #store KodSH as number not string
    return effacility

def f_init_mean(data):
    init_mean = {}
    for key in data:
        init_mean[key] = data[key].groupby(["KodSH", data[key]["Date"].dt.date])["Wartosc"].mean()
        init_mean[key] = init_mean[key].to_frame()
        init_mean[key].drop(columns = ["Wartosc"], inplace=True)        
    return init_mean

def f_sun_info(init_mean, effacility):
    sun_info = {}
    for key in init_mean:
        init_mean[key] = init_mean[key].reset_index("Date")
        #Date as a non index value
        #init_mean[key] = init_mean[key].drop(["24h"], axis=1)
        sun_info[key] = pd.merge(init_mean[key], effacility, on = "KodSH", how = "left")
    
    astral_info = {}
    for key in sun_info:
        shp = sun_info[key].shape[0]
        Dawn = list(range(shp))
        Dusk = list(range(shp))
        for k in sun_info[key].index:
            City = astral.LocationInfo(sun_info[key]["City"][k],"Poland", "Europe/Warsaw", sun_info[key]["Lat"][k], sun_info[key]["Lon"][k])
            Dawn[k] = (sun(City.observer, date=sun_info[key]["Date"][k], tzinfo=City.timezone))["dawn"]
            Dusk[k] = (sun(City.observer, date=sun_info[key]["Date"][k], tzinfo=City.timezone))["dusk"]
        data = {"KodSH" : sun_info[key]["KodSH"], "Dawn" : Dawn ,"Dusk" : Dusk}
        astral_info[key] = pd.DataFrame(data)
        sun_info[key] = pd.merge(sun_info[key], astral_info[key], left_index=True, right_index=True)
        sun_info[key].drop(["KodSH_y"], axis=1, inplace=True)
        sun_info[key].rename(columns = {"KodSH_x" : "KodSH", "Date" : "Date"}, inplace=True)
        sun_info[key]["Date"] = pd.to_datetime(sun_info[key]["Date"]).dt.tz_localize("Europe/Warsaw")
        
    return sun_info

start_time = time.time()

parametry = read_parametry(path_parametry)
data = read_data(path_data)
effacility = read_effacility(path_effacility)

init_mean = f_init_mean(data)
sun_info = f_sun_info(init_mean, effacility)

def pass_effacility(eff):
    #przeksztalcam effacility z dataframe do json
    #wysylam do bazy danych
    for item in eff.items():
        r.set("effacility_" + item[0], json.dumps(item[1].tolist()))

def get_effacility():
    #odbieram z bazy danych
    #json->dataframe
    columns = ["KodSH", "City", "Lon", "Lat"]
    _container = {}
    for col in columns:
        _cached = r.get("effacility_" + col)
        _container[col] = json.loads(_cached)
    data = pd.DataFrame.from_dict(_container)
    return data

def pass_IMGW(imgw):
    names = []
    for key in imgw:
        for item in imgw[key].items():
            name = "IMGW_" + key[:7] + "_" + key[10:12] + key[13:15]
            names.append(name)
            #nalezy zapisac date jako string, aby nie tracic strefy czasowej
            imgw[key]["Date"] = imgw[key]["Date"].astype("str")
            r.set(name + item[0], json.dumps(item[1].tolist()))
    return names

def get_IMGW(names):
    columns = ["KodSH", "ParametrSH", "Date", "Wartosc"]
    _big_container = {}
    _container = {}
    for name in names:
        for col in columns:
            _cached = r.get(name + col)
            _container[col] = json.loads(_cached)
        data = pd.DataFrame.from_dict(_container)
        data["Date"] = pd.to_datetime(data["Date"])
        _big_container[name[5:]] = data 
    return _big_container


def pass_sun_info(sun_info):
    names = []
    for key in sun_info:
        for item in sun_info[key].items():
            name = "Sun_info_" + key[:7] + "_" + key[10:12] + key[13:15]
            names.append(name)
            #nalezy zapisac date jako string, aby nie tracic strefy czasowej
            sun_info[key]["Date"] = sun_info[key]["Date"].astype("str")
            sun_info[key]["Dawn"] = sun_info[key]["Dawn"].astype("str")
            sun_info[key]["Dusk"] = sun_info[key]["Dusk"].astype("str")
            r.set(name + item[0], json.dumps(item[1].tolist()))
    return names

def get_sun_info(names):
    columns = ["KodSH", "Date", "City", "Lon", "Lat", "Dawn", "Dusk"]
    _big_container = {}
    _container = {}
    for name in names:
        for col in columns:
            _cached = r.get(name + col)
            _container[col] = json.loads(_cached)
        data = pd.DataFrame.from_dict(_container)
        data["Date"] = pd.to_datetime(data["Date"])
        data["Dawn"] = pd.to_datetime(data["Dawn"])
        data["Dusk"] = pd.to_datetime(data["Dusk"])
        _big_container[name[9:]] = data 
    return _big_container

def day_night(imgw, sun_info):
    day_night = {}
    for key in imgw:
        date_time = imgw[key]["Date"]
        #save old datetime
        imgw[key]["Date"] = imgw[key]["Date"].dt.date
        #trim Date of time, which is necessary to merge(unwanted conv from datetime64 to object)
        imgw[key]["Date"] = pd.to_datetime(imgw[key]["Date"]).dt.tz_localize("Europe/Warsaw")
        #conversion from object to datetime64
        day_night[key] = pd.merge(imgw[key], sun_info[key], on=["KodSH", "Date"], how="inner")
        #merging data with info about dusk and dawn
        imgw[key].drop(["Date"], axis=1, inplace=True)
        imgw[key].insert(2, "Date", date_time)
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
    for key in analysis_basic:
        table = tabulate(analysis_basic[key], headers = hdrs, tablefmt = 'psql')
        result = open("analysis_basic_" + key[:15] + ".txt", "w")
        result.write(table)
        result.close()

    
pass_effacility(effacility)
names_imgw = pass_IMGW(data)
names_sun_info = pass_sun_info(sun_info)

effacility_redis = get_effacility()
imgw_redis=get_IMGW(names_imgw)
sun_info_redis=get_sun_info(names_sun_info)

day_night = day_night(imgw_redis, sun_info_redis)
analysis_basic = f_analysis_basic(sun_info_redis, day_night)
analysis_trim = f_analysis_trim(sun_info_redis, day_night)
#f_display_analysis(analysis_basic) 

print("--- %s seconds ---" % (time.time() - start_time))

print(r.dbsize())
r.flushdb()
