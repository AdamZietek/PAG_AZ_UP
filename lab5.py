import requests, zipfile, io, os, re
import pandas as pd
import numpy as np
import geopandas, astral
import time
from astral.sun import sun
import tabulate

METEO_FOLDER = r"C:/Users/48604/Documents/semestr5/PAG/pag2/Meteo/" 
ZAPIS_ZIP = METEO_FOLDER + r"Meteo_"
url = "https://dane.imgw.pl/datastore/getfiledown/Arch/Telemetria/Meteo/2015/Meteo_2015-07.zip"
#!
#change: METEO_FOLDER, url

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

        day_night[key]["day/night"] = np.where((day_night[key]["Date"] >= day_night[key]["Dawn"]) & (day_night[key]["Date"] < day_night[key]["Dusk"]), 1, 0)
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
        
        mean_day[key] = mean[key][mean[key]["day/night"] != 0]
        mean_night[key] = mean[key][mean[key]["day/night"] != 1]
        median_day[key] = median[key][median[key]["day/night"] != 0]
        median_night[key] = median[key][median[key]["day/night"] != 1]
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
        mean_day[key].rename(columns = {"Wartosc" : "Mean_day"}, inplace=True)
        mean_night[key].rename(columns = {"Wartosc" : "Mean_night"}, inplace=True)
        median_day[key].rename(columns = {"Wartosc" : "Median_day"}, inplace=True)
        median_night[key].rename(columns = {"Wartosc" : "Median_night"}, inplace=True)
        #basic dataframe maintenance
        
        mean_day[key] = pd.concat([mean_day[key], mean_night[key]["Mean_night"], median_day[key]["Median_day"], median_night[key]["Median_night"]], axis=1)
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
    
def read_powiaty(path_powiaty):
    powiaty = geopandas.read_file(path_powiaty)
    powiaty["geometry"] = powiaty["geometry"].to_crs(epsg=4258)
    data = {"Powiat" : powiaty["name"], "geometry" : powiaty["geometry"]}
    powiaty = geopandas.GeoDataFrame(data)
    return powiaty

def read_wojewodztwa(path_wojewodztwa):
    wojewodztwa = geopandas.read_file(path_wojewodztwa)
    wojewodztwa["geometry"] = wojewodztwa["geometry"].to_crs(epsg=4258)
    data = {"Wojewodztwo" : wojewodztwa["name"], "geometry" : wojewodztwa["geometry"]}
    wojewodztwa = geopandas.GeoDataFrame(data)
    return wojewodztwa
    
def f_merge_stacje_powiaty(effacility, powiaty):
    stacje_powiaty = effacility
    stacje_powiaty = geopandas.GeoDataFrame(stacje_powiaty, crs="EPSG:4258", geometry=geopandas.points_from_xy(stacje_powiaty["Lon"], stacje_powiaty["Lat"]))
    stacje_powiaty = stacje_powiaty.sjoin(powiaty, how="inner", predicate="within")
    stacje_powiaty.drop(["geometry"], axis=1, inplace=True)
    data = {"KodSH" : stacje_powiaty["KodSH"], "Powiat" : stacje_powiaty["Powiat"]}
    stacje_powiaty = pd.DataFrame(data)
    return stacje_powiaty

def f_merge_stacje_wojewodztwa(effacility, wojewodztwa):
    stacje_woj = effacility
    stacje_woj = geopandas.GeoDataFrame(stacje_woj, crs="EPSG:4258", geometry=geopandas.points_from_xy(stacje_woj["Lon"], stacje_woj["Lat"]))
    stacje_woj = stacje_woj.sjoin(wojewodztwa, how="inner", predicate="within")
    stacje_woj.drop(["geometry"], axis=1, inplace=True)
    data = {"KodSH" : stacje_woj["KodSH"], "Wojewodztwo" : stacje_woj["Wojewodztwo"]}
    stacje_woj = pd.DataFrame(data)
    return stacje_woj

def f_which_powiat(analysis_basic, stacje_powiaty):
    which_powiat = analysis_basic
    for key in which_powiat:
        which_powiat[key] = pd.merge(which_powiat[key], stacje_powiaty, on=["KodSH"], how="left", right_index=False)
    return which_powiat
    
def f_analysis_basic_powiat(analysis_basic, which_powiat):
    analysis_basic_powiat = {}
    for key in analysis_basic:
        analysis_basic_powiat[key] = analysis_basic[key].groupby(["Date", "Powiat"])[["Mean_day", "Mean_night", "Median_day", "Median_night"]].mean()
        analysis_basic_powiat[key] = analysis_basic_powiat[key].reset_index()
    return analysis_basic_powiat

def f_which_woj(analysis_basic, stacje_woj):
    which_woj = analysis_basic
    for key in which_woj:
        which_woj[key] = pd.merge(which_woj[key], stacje_woj, on=["KodSH"], how="left", right_index=False)
    return which_woj
    
def f_analysis_basic_woj(analysis_basic, which_woj):
    analysis_basic_woj = {}
    for key in analysis_basic:
        analysis_basic_woj[key] = analysis_basic[key].groupby(["Date", "Wojewodztwo"])[["Mean_day", "Mean_night", "Median_day", "Median_night"]].mean()
        analysis_basic_woj[key] = analysis_basic_woj[key].reset_index()
    return analysis_basic_woj

def f_wykres_powiat(analysis_basic_powiat, powiat):
    wykres_data = {}
    for p in powiat:
        for key in analysis_basic_powiat:
            data = analysis_basic_powiat[key].loc[analysis_basic_powiat[key]["Powiat"] == p].copy(deep=True)
            if data.empty == False:
                data["Date"] = pd.to_datetime(data["Date"])
                data.index = data["Date"].dt.day
                data.drop(["Date"], axis=1, inplace=True)
                data.plot(ylabel="Values", title=p)
                wykres_data[key] = data
    return wykres_data

def f_wykres_woj(analysis_basic_woj, woj):
    wykres_data = {}
    for w in woj:
        for key in analysis_basic_woj:
            data = analysis_basic_woj[key].loc[analysis_basic_woj[key]["Wojewodztwo"] == w].copy(deep=True)
            if data.empty == False:
                data["Date"] = pd.to_datetime(data["Date"])
                data.index = data["Date"].dt.day
                data.drop(["Date"], axis=1, inplace=True)
                data.plot(xlabel="Dzień miesiąca", ylabel="Wartosci", title=w + " " + key)
                wykres_data[key] = data
    return wykres_data

def main():
    start_time = time.time()
    
    parametry = read_parametry(path_parametry)
    data = read_data(path_data)
    effacility = read_effacility(path_effacility)
    
    init_mean = f_init_mean(data)
    sun_info = f_sun_info(init_mean, effacility)
    day_night = f_day_night(data, sun_info)
    analysis_basic = f_analysis_basic(sun_info, day_night)
    analysis_trim = f_analysis_trim(sun_info, day_night)
    
    # f_display_analysis(analysis_basic)
    powiaty = read_powiaty(path_powiaty)
    wojewodztwa = read_wojewodztwa(path_wojewodztwa)
    
    stacje_powiaty = f_merge_stacje_powiaty(effacility, powiaty)
    stacje_woj = f_merge_stacje_wojewodztwa(effacility, wojewodztwa)
    
    which_powiat = f_which_powiat(analysis_basic, stacje_powiaty)
    analysis_basic_powiat = f_analysis_basic_powiat(analysis_basic, which_powiat)
    which_woj = f_which_woj(analysis_basic, stacje_woj)
    analysis_basic_woj = f_analysis_basic_woj(analysis_basic, which_woj)
    
    #wykres_powiat = f_wykres_powiat(analysis_basic_woj, ["brzeziński"])
    wykres_woj = f_wykres_woj(analysis_basic_woj, ["łódzkie"])
    
    print("--- %s seconds ---" % (time.time() - start_time))
    return 0

if __name__ == "__main__":
    main()