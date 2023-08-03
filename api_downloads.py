import cdsapi
import check


c = cdsapi.Client()


#variables
months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
years = [2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021]


def download(retrieving):
    
    counter = 0
    for YEAR in years:
        for month in months:
            
            
            month_str = check.get_month(month)
            
            if retrieving:
                retrieve(YEAR, month_str, month)
            else:
                print(f"Downloading. . . {month_str}-{YEAR}")
            
            
            #printout logic
            counter += 1
            percent = round(counter / (len(months) * len(years)), 3) * 100
            print(f"*********************************       {percent}% DONE!          *********************************")

    
    #for testing purposes
    if not retrieving:
        return counter
            
            
    
   
            

def retrieve(YEAR, month_str, month, returning=False):
    if month < 0 or month > 12:
        raise ValueError("Incorrect month")
    if int(month_str) != month:
        raise ValueError("Month string and month do not equal each other")
    
    path = f'data/{YEAR}-{month}.nc'
    
    c.retrieve(
        'reanalysis-era5-land',
        {
            'variable': [
                '10m_u_component_of_wind', '10m_v_component_of_wind', '2m_temperature',
                'leaf_area_index_high_vegetation', 'leaf_area_index_low_vegetation', 'skin_reservoir_content',
                'snow_density', 'snow_depth', 'surface_runoff',
                'total_evaporation', 'total_precipitation',
            ],
            'year': str(YEAR),
            'month': month_str,
            'day': check.days_check(month, YEAR),
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'area': [
                49.4, 66.9, 25.1,
                124.7,
            ],
            'format': 'netcdf',
        },
        path)
    
    
   


    

if __name__ == "__main__":
    download(True)
