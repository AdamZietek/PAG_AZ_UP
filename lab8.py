from pymongo import MongoClient
import json 
import requests, zipfile, io, os, re
import pandas as pd
import geopandas, astral
import time
from astral.sun import sun

METEO_FOLDER = r"C:/Users/48604/Documents/semestr5/PAG/pag2/Meteo/" 
ZAPIS_ZIP = METEO_FOLDER + r"Meteo_"
url = "https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/2015/Meteo_2015-07.zip"

connection = MongoClient("localhost", 27017) 
db = connection.local
collection = db

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
    effacility_json = eff.to_json(orient='records')
    parsed = json.loads(effacility_json)
    collection["effacility"].insert_many(parsed)

def pass_IMGW(imgw):
    for key in imgw:
        imgw[key]["Date"] = imgw[key]["Date"].astype("str")
        #nalezy zapisac date jako string, aby nie tracic strefy czasowej
        collection["IMGW_" + key[:7] + "_" + key[10:12] + key[13:15]].insert_many(imgw[key].to_dict('records'))

def pass_sun_info(sun_info):
    for key in sun_info:
        sun_info[key]["Date"] = sun_info[key]["Date"].astype("str")
        sun_info[key]["Dawn"] = sun_info[key]["Dawn"].astype("str")
        sun_info[key]["Dusk"] = sun_info[key]["Dusk"].astype("str")
        collection["Sun_info_" + key[:7] + "_" + key[10:12] + key[13:15]].insert_many(sun_info[key].to_dict('records'))
    
pass_effacility(effacility)
pass_IMGW(data)
pass_sun_info(sun_info)

print("--- %s seconds ---" % (time.time() - start_time))
