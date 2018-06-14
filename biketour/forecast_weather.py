#!/bin/env python

from darksky import forecast

import numpy as np
import pandas as pd

import datetime

import sqlite3, logging

class Forecast_Weather(object):
    """The class queries weather forecast at a list of coordinates

    """

    def __init__(self, coordinates, darksky_apikey,
                 filename="weather_forecast.db",
                 darkskyapi_calls_limit = 900,
                 darksky_units = "si",
                 forecast_expire_age = 12,
                 forecast_purge_age = 240):
        """Initialise class

        :coordinates: a pandas dataframe with latitude and longitude columns

        :darksky_apikey: api key for darksky queries

        :filename: filename of the sqlite database to use that stores
        the current weather forecasts

        :darkskyapi_calls_limit: maximum number of api calls allowed
        to make per day

        :darkskyapi_units: units to query darksky data (see more in darksky)

        :forecast_expire_age: time in hours after which forecast is
        considered to be old and new forecast is fetched

        :forecast_purge_age: time in hours after which the old
        forecasts are deleted from database
        """
        self.coordinates = np.squeeze(np.array(coordinates[['latitude','longitude']]))
        self.apikey = darksky_apikey

        self._darkskyapi_calls_limit = darkskyapi_calls_limit
        self._darkskyapi_current_usage = None
        self._darkskyapi_timestamp = None

        self._darkskyapi_units = darksky_units
        self.filename = filename

        self._forecast_expire_age = forecast_expire_age
        self._forecast_purge_age = forecast_purge_age

        # initialise database connection
        self._dbconn=sqlite3.connect(self.filename, timeout = 15,
                                     isolation_level="EXCLUSIVE")

        # initialise database
        self._init_database()

    def _get_current_time(self):
        """Get current time in unixtime in utc

        """
        return int(datetime.datetime.utcnow().timestamp())

    def _get_db_columns(self):
        return list(self._get_db_schema().keys())

    def _get_db_schema(self):
        return {'id':                         'INTEGER PRIMARY KEY AUTOINCREMENT',
                'forecast_type':              'VARCHAR(255)',
                'forecast_time':              'INTEGER NOT NULL',
                'latitude':                   'REAL NOT NULL',
                'longitude':                  'REAL NOT NULL',
                'time':                       'INTEGER NOT NULL',
                'summary':                    'VARCHAR(255)',
                'icon':                       'VARCHAR(255)',
                'sunriseTime':                'INTEGER',
                'sunsetTime':                 'INTEGER',
                'moonPhase':                  'REAL',
                'precipIntensity':            'REAL',
                'precipIntensityMax':         'REAL',
                'precipIntensityTime':        'INTEGER',
                'precipProbability':          'REAL',
                'precipType':                 'VARCHAR(255)',
                'temperature':                'REAL',
                'temperatureHigh':            'REAL',
                'temperatureHighTime':        'INTEGER',
                'temperatureLow':             'REAL',
                'temperatureLowTime':         'INTEGER',
                'apparentTemperature':        'REAL',
                'apparentTemperatureHigh':    'REAL',
                'apparentTemperatureHighTime':'INTEGER',
                'apparentTemperatureLow':     'REAL',
                'apparentTemperatureLowTime': 'INTEGER',
                'dewPoint':                   'REAL',
                'humidity':                   'REAL',
                'pressure':                   'REAL',
                'windSpeed':                  'REAL',
                'windGust':                   'REAL',
                'windGustTime':               'INTEGER',
                'windBearing':                'REAL',
                'cloudCover':                 'REAL',
                'uvIndex':                    'REAL',
                'uvIndexTime':                'INTEGER',
                'visibility':                 'REAL',
                'ozone':                      'REAL'
                }

    def _init_database(self):
        """Initialise the database with weather forecasts

        """
        try:
            c = self._dbconn.execute('''BEGIN EXCLUSIVE''')

            c.execute('''
            CREATE TABLE IF NOT EXISTS weather_forecast
            (
            ''' + ", ".join(x[0] + " " + x[1] for x in self._get_db_schema().items()) +  ''',
            CONSTRAINT uc_time_latitude_longitude_ftype_ftime UNIQUE (time, latitude, longitude, forecast_type, forecast_time)
            )''')

            self._dbconn.commit()
        except Exception as e:
            logging.error("Error creating table (weather_forecast)",e)
            self._dbconn.rollback()
            raise e

    def _isallowed_darksky(self):
        """Check if the query is allowed

        This operation uses locally known variables to decide wether
        we can make another query
        """
        # TODO: to do not yet completely implemented

        if self._darkskyapi_current_usage is not None and self._darkskyapi_current_usage >= self._darkskyapi_calls_limit:
            return False

        return True

    def _is_recent_forecast_present(self):
        """Check if recent forecast is present in the database

        """

        # get current time
        current_time = self._get_current_time()

        # get cursor
        c = self._dbconn.cursor()

        # check forecast timestamps age

        # NOTE: forecast can be incomplete. Here one need to verify
        # that forecast for all coordinates is present
        try:
            c.execute('''
            SELECT count(*)
            FROM weather_forecast
            WHERE abs(forecast_time -
            ''' + str(current_time) + ''') <
            ''' + str(self._forecast_expire_age*60*60))

            query_res = c.fetchone()[0]
        except Exception as e:
            logging.error("Error quering data from weather_forecast",e)
            self._dbconn.rollback()
            raise e

        self._dbconn.commit()

        return query_res >= len(self.coordinates)

    def _query_coordinate(self, coord):
        """Make a darksky query at a given place

        The query result is written to the database

        :coord: coordinate of the place
        """
        # try to make darksky query
        try:
            if not self._isallowed_darksky():
                logging.error("Reached limit for darksky API calls")
                raise RuntimeError('API limit')

            logging.info("Quering coordinate = " + str(coord))

            query_res = forecast(key=self.apikey,
                                 latitude=coord[0],longitude=coord[1],
                                 units=self._darkskyapi_units)

            # update the API usage counters
            try:
                self._darkskyapi_current_usage = int(query_res.response_headers['X-Forecast-API-Calls'])
            except Exception as e:
                logging.warning("X-Forecast-API-Calls is missing! Using only local information.")
                if self._darkskyapi_current_usage is None:
                    self._darkskyapi_current_usage = 1
                else:
                    self._darkskyapi_current_usage += 1

            self._darkskyapi_timestamp = datetime.datetime.now()

        except Exception as e:
            logging.error("Error making query to darksky: ",e)
            raise e

        # get current time
        current_time = self._get_current_time()

        # list with new data
        new_data = []
        # list with expected column names
        expected_columns = self._get_db_columns()[5:]

        # add hourly data
        for item in query_res['hourly']['data']:
            # add queried data to list
            x = ['hourly',current_time,coord[0],coord[1]]
            for col in expected_columns:
                # check presence of all columns in the query result
                if col not in item.keys():
                    x += [None]
                else:
                    x += [item[col]]

            new_data += [x]

        # add daily data
        for item in query_res['daily']['data']:
            # add queried data to list
            x = ['daily',current_time,coord[0],coord[1]]
            for col in expected_columns:
                # check presence of all columns in the query result
                if col not in item.keys():
                    x += [None]
                else:
                    x += [item[col]]

            new_data += [x]


        # get cursor
        c = self._dbconn.cursor()

        # insert values to the database
        try:
            # insert new weather data
            c.executemany('''
            INSERT OR REPLACE INTO weather_forecast
            (''' + ", ".join(self._get_db_columns()[1:]) + ''')
            VALUES
            ('''+ ",".join("?"*len(self._get_db_columns()[1:])) + ''')
            ''', new_data)

        except Exception as e:
            logging.error("Error with db insertion into weather_forecast table",e)
            self._dbconn.rollback()
            raise e

        # commit changes
        self._dbconn.commit()

    def query_darksky_weather(self):
        """Query current weather forecast from darksky API

        """
        X = np.array(self.coordinates)
        N = len(X)

        for x in X:
            print("Requests to make: " + str(N))
            N -= 1
            try:
                self._query_coordinate((x[0],x[1]))
            except Exception as e:
                logging.warning("Error during queries",e)
                break

    def query_local_weather(self, columns='*',where=None):
        """Query weather data from local database

        :columns: columns to query (can be list or string in sql format).

        :where: where condition in terms of database column names and
        sql language

        """
        # get column in a proper format
        columns = ",".join(columns)

        # get cursor
        c = self._dbconn.cursor()

        # query data from the database
        try:
            if where is None:
                c.execute('''
                SELECT
                ''' + columns + '''
                FROM weather_forecast
                ''')
            else:
                c.execute('''
                SELECT
                ''' + columns + '''
                FROM weather_forecast
                WHERE ''' + where)

            query_res = c.fetchall()

        except Exception as e:
            logging.error("Error quering data from weather_forecast",e)
            self._dbconn.rollback()
            raise e

        # column names
        if '*' == columns:
            columns = self._get_db_columns()
        else:
            columns = columns.split(",")

        # convert to pandas
        return pd.DataFrame(np.array(query_res),columns=columns)

    def _cleanup_old_forecasts(self):
        """Cleanup old forecast entries in the database

        """

        # get current time
        current_time = self._get_current_time()

        # get cursor
        c = self._dbconn.cursor()

        # delete entries from the database
        try:
            c.execute('''
            DELETE
            FROM weather_forecast
            WHERE abs(forecast_time -
            ''' + str(current_time) + ''') >
            ''' + str(self._forecast_expire_age*60*60))

            self._dbconn.commit()
        except Exception as e:
            logging.error("Error purging entries (weather_forecast)",e)
            self._dbconn.rollback()
            raise e

    def _clean_forecast(self, data):
        """Clean forecast pandas dataframe.

        By cleaning we mean choosing preferrably hourly data, and in
        case hourly data is missing use daily data.

        In case no forecast data is available throw an exception.

        Resulting pandas dataframe contains hourly data, with the same
        (or larger) set of columns as historical weather data frame.

        :data: dataframe returned by query_local_weather

        """

        return data

    def forecast(self, forecast_day = 1):
        """Get forecast at the coordinates for every hour of the day

        :forecast_day: when to compute the forecast. Default 1 means
        tomorrow

        """
        # if data is not fresh, query new forecasts
        if not self._is_recent_forecast_present():
            self.query_darksky_weather()

        current_time = self._get_current_time()

        # evaluate forecast interval
        forecast_begin = (datetime.datetime.fromtimestamp(current_time).date() +
                          datetime.timedelta(days=forecast_day))
        forecast_begin = datetime.datetime.combine(forecast_begin,
                                                   datetime.datetime.min.time())
        forecast_end = forecast_begin + datetime.timedelta(days=1)

        forecast_begin = int(forecast_begin.timestamp())
        forecast_end = int(forecast_end.timestamp())

        # get weather forecast
        res = self.query_local_weather(where='time >= ' + str(forecast_begin) +
                                       ' AND time < ' + str(forecast_end))

        return self._clean_forecast(res)

