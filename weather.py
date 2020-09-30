''' weather.py
    Module for Feb, March 2016 weather analysis.
            
        Hottest date: '2016-03-17' 
        Temperature: '15.8' 
        Region: 'Highland & Eilean Siar'
    Usage:
        cli: python3 weather.py   
        script: import weather
                       weather.hottest_day()
                       weather.temperature_on_the_day()
                       weather.hottest_day_region()
        repl: import weather
'''

import pandas as pd
import fastparquet
from file_path import file_path


def read_weather_files():
    '''Function that reads in the given csv files - weather data.
    Returns:
        weather_df: A pandas dataframe for the sourced data.
    Helper    
        help(weather.read_weather_files) '''
    try:
        weather_20160201_csv = file_path + 'weather.20160201.csv' 
        weather_20160301_csv = file_path + 'weather.20160301.csv'
        with open(weather_20160201_csv) as csvfile1, \
            open(weather_20160301_csv) as csvfile2:
                file1 = pd.read_csv(csvfile1, sep=',')
                file2 = pd.read_csv(csvfile2, sep=',')
                weather_data = pd.DataFrame()
                weather_df = weather_data.append([file1, file2])
                if weather_df.empty:
                    raise TypeError('Empty dataframe. Check source files')
                weather_df['ObservationDate'] = pd.to_datetime(weather_df['ObservationDate'])
                # weather_df.isna().sum()
                weather_df[['WindGust', 'Visibility', 'Pressure']] = weather_df[['WindGust', 'Visibility', 'Pressure']].fillna(0)
                weather_df['Country'] = weather_df['Country'].fillna('')             
        return weather_df
    except IOError as error:
        print('Operation failed: ', error.strerror)
read_weather_files()


def convert_to_parquet():
    '''Function that converts the returned dataframe into parquet format.
    Returns:
        weather_df_pq dataframe from the parquet file - weather_df_pq.parquet.gzip.
    Helper    
        help(weather.convert_to_parquet) '''
    try:
        weather_df = read_weather_files()
        weather_df.to_parquet('weather_df_pq.parquet.gzip', compression='gzip')
        weather_df_pq = pd.read_parquet('weather_df_pq.parquet.gzip')
        if weather_df_pq.empty:
            raise TypeError('Empty dataframe. Check source path')
        return weather_df_pq
    except Exception as error:
        print(error)
        raise
convert_to_parquet()


def hottest_day():
    '''Function that answers the question: "Which date was the hottest day?"
    Helper    
        help(weather.hottest_day) '''
    try:
        weather_df_pq = convert_to_parquet()
        hottest_day_row = weather_df_pq[weather_df_pq['ScreenTemperature'] == \
                weather_df_pq['ScreenTemperature'].max()]
        hottest_day = hottest_day_row.iloc[:,2].map(lambda x: str(x)[:-9])
        print('Hottest date:', hottest_day.values)
    except Exception as error:
        print(error)


def temperature_on_the_day():
    '''Function that answers the question: "What was the temperature on that day?"
    Helper    
        help(weather.temperature_on_the_day) '''
    try:
        weather_df_pq = convert_to_parquet()
        hottest_day_row = weather_df_pq[weather_df_pq['ScreenTemperature'] == \
                weather_df_pq['ScreenTemperature'].max()]
        temp_on_hottest_day = hottest_day_row.iloc[:,7]
        print('Temperature on hottest date: ', temp_on_hottest_day.values)
    except Exception as error:
        print(error)


def hottest_day_region():
    '''Function that answers the question: "In which region was the hottest day?"
    Helper    
        help(weather.hottest_day_region)
    '''
    try:
        weather_df_pq = convert_to_parquet()
        hottest_day_row = weather_df_pq[weather_df_pq['ScreenTemperature'] == \
                weather_df_pq['ScreenTemperature'].max()]
        hottest_day_region = hottest_day_row.iloc[:,13]
        print('Region with hottest date: ', hottest_day_region.values)
    except Exception as error:
        print(error)


if __name__ == '__main__':
    hottest_day()
    temperature_on_the_day()
    hottest_day_region()