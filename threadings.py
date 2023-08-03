import xarray as xr
import pandas as pd
import numpy as np
import check
import time
from concurrent.futures import ProcessPoolExecutor
import concurrent

#assume for right now that we have a file with all the county latitude and longitudes
df = pd.read_csv("national.csv")
df.to_csv("outputs/updated.csv", index=False)
#need to add in a df here!!
keys = np.arange(len(df))

df["Keys"] = keys
#first we need to add a column of keys to the dataframe for mapping





variable_names = ['u10',
    'v10',
    'v10',
    't2m',
    #'lai_hv',
    #'lai_lv',
    #'src',
    'e',
    'rsn',
    'sde',
    'sro',
    'tp']

dicts_list = [{} for _ in range(len(variable_names)*2)]


months = list(np.arange(1, 13))
months_strs = [check.get_month(x) for x in months]
years = list(np.arange(1999, 2022))



def process_single_month(YEAR, month):
    dicts_list = [{} for _ in range(len(variable_names)*2)]
    ds = xr.open_dataset(f"data/{YEAR}-{month}.nc")
    series = df[(df["Month"] == month) & (df["Year"] == YEAR)]

    if series.empty:
        return

    series_array = series.to_xarray()

    for idx, var in enumerate(variable_names):
        print((idx+1) / 12)

        info = ds[var]
        info = info.sel(latitude=series_array["Latitude"], longitude=series_array["Longitude"], method="nearest")

        dicts_list[idx * 2].update(create_dict(series_array, info, "max"))
        dicts_list[idx * 2 + 1].update(create_dict(series_array, info, "min"))

    print(f"Done with {YEAR}-{month}")
    add_to_df(dicts_list, YEAR, month)

def looping():
    with ProcessPoolExecutor(max_workers=6) as executor:
        future_to_month = {executor.submit(process_single_month, year, month): month for year in years for month in months}
    
        for future in concurrent.futures.as_completed(future_to_month):
            month = future_to_month[future]
            try:
                future.result()
                print(f"Done with {month}")
            except Exception as exc:
                print(f"Error processing {month}: {exc}")

        

     
def create_dict(inputs, temp, type):
    if type == "max":
        #grab the maxes, put them to a dictionary, ansd add it to the larger dict
        maxes = temp.max(dim="time").values

        temps_max_dict = pd.DataFrame({"Keys": inputs["Keys"], 'Temps': maxes}).set_index("Keys")["Temps"].to_dict()

        return temps_max_dict

    if type == "min":
        #same with the mins dict
        mins = temp.min(dim="time").values

        temps_min_dict = pd.DataFrame({"Keys": inputs["Keys"], 'Temps': mins}).set_index("Keys")["Temps"].to_dict()

        return temps_min_dict

    else:
        raise ValueError("Not asking for a min or a max")


# List with maximum and minimum values
parameters_list = [
    "Maximum 10m U-Component of Wind",
    "Minimum 10m U-Component of Wind",
    "Maximum 10m V-Component of Wind",
    "Minimum 10m V-Component of Wind",
    "Maximum 2m Temperature",
    "Minimum 2m Temperature",
    #"Maximum Leaf Area Index, High Vegetation",
    #"Minimum Leaf Area Index, High Vegetation",
    #"Maximum Leaf Area Index, Low Vegetation",
    #"Minimum Leaf Area Index, Low Vegetation",
    #"Maximum Skin Reservoir Content",
    #"Minimum Skin Reservoir Content",
    "Maximum Snow Density",
    "Minimum Snow Density",
    "Maximum Snowfall",
    "Minimum Snowfall",
    "Maximum Surface Runoff",
    "Minimum Surface Runoff",
    "Maximum Total Evaporation",
    "Minimum Total Evaporation",
    "Maximum Total Precipitation",
    "Minimum Total Precipitation"
]

def add_to_df(dicts_list, year, month):
    df = pd.read_csv("national.csv")
    df["Keys"] = keys
    for idx, var in enumerate(parameters_list):
        df[var] = df["Keys"].map(dicts_list[idx])
    
    df.to_csv(f"outputs/updated{year}-{month}.csv", index=False)

if __name__ == "__main__":
    start_time = time.time()
    
    
    looping()

    #for idx, var in enumerate(parameters_list):
        #df[var] = df["Keys"].map(dicts_list[idx])

    df.to_csv("outputs/all_national.csv", index=False)
    
    end_time = time.time()
    execution_time = end_time - start_time

    with open("outputs/time.txt", "w") as f:
    	f.write(f"Execution Time: {execution_time:.4f} seconds")
