# Sample weather data - early 2016

> weather.py
```
  Script that reads in the weather data (194697 rows x 15 columns) and coverts into parquet data format. Performs minor transformations as detailed below under `Assumptions`. Then answers the following questions (individual python functions):

  a. Which date was the hottest day?
  b. Temperature on that day
  c. Region was the hottest day

```
> test_weather.py
```
  Test suite that independently tests small units of codes within the weather.py file.
```
> file_path_sample.py
```
  Small python module that supplies/ hides your local file path for the .csv files. Note: Rename file_path_sample.py to file_path.py
```

Repro steps via cli:

* Clone the repository using terminal/cli.
  ```
  $ mkdir weather
  $ cd weather
  $ git clone https://github.com/Chrisochok/weather_analysis.git
  ```
* Refactor file_path_sample.py to file_path.py, and
* Set the file_path to your csv files location.
```
  $ cd ~/Desktop/weather/data/
```
* Navigate to the folder containing python files.
```
  $ pwd `to check that you are in the right location`
  $ cd ~/Desktop/weather/
```
* Install `pandas` and `fastparquet` modules and dependencies (if you do not have these already)
  On Windows:
```
  $ pip install pandas
  $ pip install fastparquet
```
  On Mac:
```
  $ pip3 install pandas
  $ pip3 install fastparquet
```
* Run test_weather.py script ( the tests should fail at this stage )
```
  $ python3 -m unittest test_weather.py
```
* Run weather.py script
```
  $ python3 weather.py
```
* Now run test_weather.py once more
```
  $ python3 -m unittest test_weather.py
```
* Alternatively you can run the scripts directly from your favourite IDE.


ASSUMPTIONS:

Upon previewing the source data:
 1. The 'ObservationDate' field should be 'date' data type so was coerced to 'datetime' from 'str' data type for querying accuracy.
 2. 'NaN' values were identified in the following numeric fields and have been imputted with zeros - [WindGust - 151411, Visibility - 26493, Pressure - 14820].
 3. 'NaN' values (27760) were identified in the 'Country' field. These were replaced with empty strings.
 4. "The converted data should be queryable to answer the following question." - it's assumed that this statement refers to converting data file to parquet columnar store before further analysis.