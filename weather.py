'''DocString Weather.py
Module for Feb, March 2016 weather analysis.
    
    Hottest date: '2016-03-17' 
    Temperature: '15.8' 
    Region: 'Highland & Eilean Siar'
Usage:
    cli: python3 weather.py   
    script: from weather import weather_data
    repl: from weather import weather_data
Note: 
    productionisation of gb/tb of weather data 
    best achieved via containerised distributable
    approach using spark, docker, k8s etc.
'''
import csv, sqlite3, os, pandas as pd
import fastparquet
import file_path as fp


def weather_data():
    '''Callable function for the weather analysis.
    '''
    file_path = fp.file_path
    weather1 = pd.read_csv('weather.20160201.csv', index_col='ForecastSiteCode')
    weather1.columns
    print('file_1 dim: ', weather1.shape)
    weather2 = pd.read_csv('weather.20160301.csv', index_col='ForecastSiteCode')
    print()
    print('file_2 dim: ', weather2.shape)
    weather_data = pd.DataFrame()
    weather_df = weather_data.append([weather1, weather2])
    print()
    print('combined_file: ',weather_df.shape)
    print()
    weather_df.to_parquet('weather_df_pq.parquet', compression='gzip')
    weather_df_pq = pd.read_parquet('weather_df_pq.parquet')
    print('returned parquet_df dim',weather_df_pq.shape)
    print()
    con = sqlite3.connect('weather.db')
    cur = con.cursor()
    cur.execute('''DROP TABLE IF EXISTS weather_data''')
    cur.execute('''
                CREATE TABLE IF NOT EXISTS weather_data (
                    ForecastSiteCode int not null,
                    ObservationTime int not null, ObservationDate DATE not null, 
                    WindDirection int, WindSpeed int,
                    WindGust int, Visibility int,
                    ScreenTemperature double not null, Pressure int,
                    SignificantWeatherCode int, SiteName text, 
                    Latitude double, Longitude double, 
                    Region text, Country text
                    )
                ''')
    con.commit()
    test = con.execute('SELECT * FROM weather_data')
    # table should be blank (object print)
    print(test)
    print()
    field_names = [description[0] for description in test.description]
    print('checking loaded fields..')
    print(field_names)
    print()
    weather_df_pq.to_sql('weather_data', con, if_exists='append', index=True)
    record_count = con.execute('SELECT count(*) FROM weather_data').fetchall()
    print('record_count: ',record_count)
    print()
    result = con.execute('''
                        SELECT substr(ObservationDate, 1, 10) ObservationDate, 
                                ScreenTemperature, Region
                        FROM weather_data
                        WHERE ScreenTemperature = 
                            ( SELECT max(ScreenTemperature) 
                            FROM weather_data )
                        ''').fetchone()
    con.commit()
    con.close()
    print('Hottest date, Temperature, Region')
    return result
weather_data()