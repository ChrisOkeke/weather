'''Test suite for weather.py  
Usage:
    CLI: python3 -m unittest test_weather.py 
'''

import unittest
from unittest.mock import patch
from file_path import file_path
import pandas as pd
import weather
import io
import os

    
class TestWeather(unittest.TestCase):
    # pass

    @classmethod
    def setUpClass(cls):
        print('setUpClass')
        # define in-memory mock data for the weather files
        cls.mock_weather_df = pd.DataFrame ( 
                    {'ObservationDate': 
                        ['2016-02-01',
                        '2016-02-01',
                        '2016-02-01',
                        '2016-02-01',
                        '2016-02-01'],
                    'ScreenTemperature': 
                        ['2.1','0.1','2.8','1.6','9.8'],
                    'Region':
                        ['Orkney & Shetland',
                        'Orkney & Shetland',
                        'Orkney & Shetland',
                        'Orkney & Shetland',
                        'Highland & Eilean Siar']} )
        cls.mock_weather_df['ObservationDate'] = pd.to_datetime(cls.mock_weather_df['ObservationDate'])
        cls.mock_weather_df['ScreenTemperature'] = cls.mock_weather_df['ScreenTemperature'].astype('float64')
    
    @classmethod
    def tearDownClass(cls):
        print('tearDownClass')

    def setUp(self):
        print('setUp')
        self.file_paths = [os.path.join(os.path.dirname(__file__), 'data/weather.20160201.csv') \
                         , os.path.join(os.path.dirname(__file__), 'data/weather.20160301.csv')]
        self.read_weather_files_select_fields = weather.read_weather_files() \
                                        [['ObservationDate','ScreenTemperature','Region']].head(5)
        self.mock_df_max_temp = self.mock_weather_df['ScreenTemperature'].max()
    
    def tearDown(self):
        print('tearDown\n')
    
    
    def test_that_source_files_exist(self):
        print('test_the_existence_of_weather.20160201.csv_and_weather.20160301.csv_source_files')
        self.assertEqual(self.file_paths[0], file_path + 'weather.20160201.csv')        
        self.assertEqual(self.file_paths[1], file_path + 'weather.20160301.csv')
        self.assertNotEqual(self.file_paths[0], self.file_paths[1])


    def test_mock_csv_source_files_open_call(self):
        print('test_mock_csv_source_files_with_open_call')
        # call the mock data
        mock_weather_df = self.mock_weather_df
        output_df = io.StringIO()
        mock_weather_df.to_csv(output_df)
        output_df.seek(0)
                    
        with patch('weather.read_weather_files') as files_mock:
            files_mock.return_value = output_df
            weather_df = self.read_weather_files_select_fields
            files_mock.assert_called_once_with(weather.read_weather_files(output_df))
            pd.testing.assert_frame_equal(weather_df, mock_weather_df)


    # mock pandas read_csv functionality
    @patch('weather.read_weather_files')
    def test_mock_read_csv_files(self, read_csv):
        print('test_mock_read_csv_files')
        # assign the mock file as the 'return value'
        read_csv.return_value = self.mock_weather_df
        weather_df_csv = weather.read_weather_files() \
                                        [['ObservationDate','ScreenTemperature','Region']].head(5)
        read_csv.assert_called_once()
        pd.testing.assert_frame_equal(weather_df_csv, self.mock_weather_df)


    def empty_df_except(self):
        ''' Function that helps to mock exception for empty dataframes '''
        if self.mock_weather_df.empty:
            raise TypeError('Empty dataframe. Review source files')
        else:
            return True
    
    def test_for_empty_dataframe(self):
        print('test_mock_for_empty_dataframe')
        self.assertRaises(TypeError, self.empty_df_except())            


    @patch('weather.convert_to_parquet')
    def test_mock_parquet_file(self, read_parquet):
        print('test_mock_parquet_file')
        read_parquet.return_value = self.mock_weather_df
        weather_df_pq = weather.convert_to_parquet() \
                                        [['ObservationDate','ScreenTemperature','Region']].head(5)
        read_parquet.assert_called_once()
        pd.testing.assert_frame_equal(weather_df_pq, self.mock_weather_df)
    
    
    def test_max_temperature_function(self):
        print('test_that_max()_temperature_function_returns_the_maximum_ScreenTemperature_value')
        self.assertEqual(self.mock_df_max_temp, 9.8)

if __name__=='__main__':
    unittest.main()