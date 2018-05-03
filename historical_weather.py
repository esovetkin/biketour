#!/bin/env python

from darksky import forecast

import sqlite3

import logging

from datetime import datetime, timedelta

import random

import pandas as pd

import numpy as np

class Iter_Historical_Weather(object):
    """Iterator class that allows iterate over a sample of historical weather

    """
    def __init__(self, hist_weather):
        """Initialise iterator for historical weather object

        :hist_weather: Historical_Weather object

        """
        self.hw = hist_weather
        self._init_iter_variables()


    def __iter__(self):
        return self


    def _init_iter_variables(self):
        """Get sampled weather times

        """
        # get cursor
        c = self.hw._dbconn.cursor()

        # query data from the database
        try:
            c.execute('''
            SELECT DISTINCT time
            FROM weather
            ORDER BY time
            ''')

            self._times = pd.DataFrame(c.fetchall(),columns=['time'])
        except Exception as e:
            logging.error("Error quering data from weather table",e)
            self.hw._dbconn.rollback()
            raise e

        self.hw._dbconn.commit()

        self._times['datetime'] = self._times.apply(datetime.fromtimestamp,axis=1)
        self._times['date'] = self._times['datetime'].apply(lambda x: (x.year,x.month,x.day))

        self._dates = list(self._times['date'].drop_duplicates())
        self.i = 0
        self.n = len(self._dates)


    def __next__(self, columns='*'):
        """Return next weather in the sample

        """
        if self.i >= self.n:
            raise StopIteration()

        # get minimal and maximal time for this iteration
        m = self._times[self._times['date'] == self._dates[self.i]]['time'].min()
        M = self._times[self._times['date'] == self._dates[self.i]]['time'].max()

        # increment i
        self.i += 1

        return self.hw.query_local_weather(
            columns=columns,
            where="time >= " + str(m) + " AND time <= " + str(M))


class Historical_Weather(object):
    """The class queries historical weather at a list of coordinates

    """

    def __init__(self, coordinates, darksky_apikey,
                 filename="weather_history.db",
                 sample_size=20,
                 sample_years=10,
                 sample_around_interval=None,
                 sample_current_date=None,
                 darkskyapi_calls_limit=900,
                 darksky_units = "si"):
        """Initialise class

        :coordinates: list of coordinates

        :darksky_apikey: api key for darksky queries

        :filename: filename of the sqlite database to use that stores
        the historical weather

        :sample_size: number of days to query in the last sample_years

        :sample_years: number of years in the past to query from

        :sample_around_interval: number of days to query around which
        the weather is queried (for example, when one is only
        interested in summer days). In case None (default) the whole
        year is sampled.

        :sample_current_date: current date around which the
        sample_around_interval is computed

        :darkskyapi_calls_limit: maximum number of api calls allowed
        to make per day

        :darkskyapi_units: units to query darksky data (see more in darksky)

        """
        self.coordinates=coordinates
        self.apikey=darksky_apikey

        self._sample_size=sample_size
        self._sample_years=sample_years
        self._sample_around_interval=sample_around_interval
        self._sample_current_date=sample_current_date

        self._darkskyapi_calls_limit = darkskyapi_calls_limit
        self._darkskyapi_current_usage = None
        self._darkskyapi_timestamp = None

        self._darkskyapi_units = darksky_units

        self.filename=filename

        # initialise database connection
        self._dbconn=sqlite3.connect(self.filename, timeout = 15,
                                       isolation_level="EXCLUSIVE")

        # initialise database
        self._init_database()

        # check that query dates exists
        if 0 == self._count_query_dates_from_db():
            days = self._sample_days_to_query()
            self._save_query_dates_to_db(days)

    def _get_db_columns(self):
        return ['id','latitude','longitude','time','summary','icon',
                'precipIntensity','precipProbability','precipType',
                'temperature','apparentTemperature','dewPoint','humidity',
                'pressure','windSpeed','windGust','windBearing',
                'cloudCover','uvIndex','visibility','ozone']

    def _init_database(self):
        """This function initialised the database with weather data

        """
        try:
            c = self._dbconn.execute('''BEGIN EXCLUSIVE''')

            c.execute('''
            CREATE TABLE IF NOT EXISTS weather
            (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            latitude            REAL NOT NULL,
            longitude           REAL NOT NULL,
            time                INTEGER NOT NULL,
            summary             VARCHAR(125),
            icon                VARCHAR(125),
            precipIntensity     REAL,
            precipProbability   REAL,
            precipType          VARCHAR(125),
            temperature         REAL,
            apparentTemperature REAL,
            dewPoint            REAL,
            humidity            REAL,
            pressure            REAL,
            windSpeed           REAL,
            windGust            REAL,
            windBearing         REAL,
            cloudCover          REAL,
            uvIndex             REAL,
            visibility          REAL,
            ozone               REAL,
            CONSTRAINT uc_time_latitude_longitude UNIQUE (time, latitude, longitude)
            )''')

            c.execute('''
            CREATE TABLE IF NOT EXISTS query_dates
            (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            time                INTEGER NOT NULL,
            latitude            REAL NOT NULL,
            longitude           REAL NOT NULL,
            if_queried          INTEGER NOT NULL,
            CONSTRAINT uc_time_latitude_longitude UNIQUE(time, latitude, longitude)
            )''')

            self._dbconn.commit()
        except Exception as e:
            logging.error("Error creating table (weather)",e)
            self._dbconn.rollback()
            raise e


    def _sample_days_to_query(self):
        """Generate table of the weather to query

        """

        # evaluate delta of the interval
        if self._sample_around_interval is None:
            delta=365
        else:
            delta=self._sample_around_interval//2
        if delta>365:
            delta=365

        # get days ago
        days_ago=set()
        for k in range(0,self._sample_years):
            days_ago=days_ago.union(range(365*(k+1)-delta,365*(k+1)+delta))
        # remove last 2 weeks from the forecast
        days_ago = days_ago.difference(range(0,14))

        # sample from days_ago
        days_ago = random.sample(days_ago, self._sample_size)

        # evaluate date times
        if self._sample_current_date is None:
            self._sample_current_date=datetime.now()

        # computed required timestamps
        days=[(self._sample_current_date - timedelta(days=x)).strftime("%s") for x in days_ago]

        # get a cross-product with coordinates
        return [(x[0],x[1],y,False) for x in np.array(self.coordinates) for y in days]


    def _save_query_dates_to_db(self, days):
        """Save list of days to query to the database

        :days: sample of days
        """

        # get cursor
        c = self._dbconn.cursor()

        # insert values to the database
        try:
            c.executemany('''
            INSERT OR IGNORE INTO query_dates
            (latitude, longitude, time, if_queried)
            VALUES (?,?,?,?)
            ''', days)

        except Exception as e:
            logging.error("Error with db insertion into query_dates table",e)
            self._dbconn.rollback()
            raise e

        # commit changes
        self._dbconn.commit()


    def _read_query_dates_from_db(self, if_queried=None):
        """Read sample days from query_dates table

        :if_queried: specifies which date select. If None select all
        dates. If True select only those that are queried
        (i.e. if_queried field is True). If False select only those
        that are not yet queried.

        """

        # get cursor
        c = self._dbconn.cursor()

        # query data from the database
        try:
            if if_queried is None:
                c.execute('''
                SELECT time, latitude, longitude, if_queried
                FROM query_dates
                ''')
            else:
                c.execute('''
                SELECT time, latitude, longitude, if_queried
                FROM query_dates
                WHERE if_queried = ?
                ''', (bool(if_queried),))

            query_res = c.fetchall()
        except Exception as e:
            logging.error("Error quering data from query_dates",e)
            self._dbconn.rollback()
            raise e

        self._dbconn.commit()

        return query_res


    def _count_query_dates_from_db(self, if_queried=None):
        """Count number of days in query_dates table

        :if_queried: specifies which date select. If None select all
        dates. If True select only those that are queried
        (i.e. if_queried field is True). If False select only those
        that are not yet queried.
        """

        # get cursor
        c = self._dbconn.cursor()

        # query data from the database
        try:
            if if_queried is None:
                c.execute('''
                SELECT count(*)
                FROM query_dates
                ''')
            else:
                c.execute('''
                SELECT count(*)
                FROM query_dates
                WHERE if_queried = ?
                ''', (bool(if_queried),))

            query_res = c.fetchone()[0]
        except Exception as e:
            logging.error("Error quering data from query_dates",e)
            self._dbconn.rollback()
            raise e

        self._dbconn.commit()

        return query_res


    def _isallowed_darksky(self):
        """Check if the query is allowed

        This operation uses locally known variables to decide wether
        we can make another query
        """
        # TODO: to do not yet completely implemented

        if self._darkskyapi_current_usage is not None and self._darkskyapi_current_usage >= self._darkskyapi_calls_limit:
            return False

        return True


    def _query_coordinate_time(self, coord, time):
        """Make darksky query with at a given place and time

        The query result is written to the database

        :coord: coordinate of the place

        :time: time in seconds since epoch

        """
        # try to make darklib query
        try:
            if not self._isallowed_darksky():
                logging.error("Reached limit for darksky API calls")
                raise RuntimeError('API limit')

            logging.info("Quering time = " + str(time) + " coordinate = " + str(coord))

            query_res = forecast(key=self.apikey,
                                 latitude=coord[0],longitude=coord[1],
                                 time=time, units=self._darkskyapi_units)

            # TODO: verify here that result is not error

            # update the API usage counters
            try:
                self._darkskyapi_current_usage = int(query_res.response_headers['X-Forecast-API-Calls'])
            except Exception as e:
                logging.warning("X-Forecast-API-Calls is missing! Using only local information.")
                if self._darkskyapi_current_usage is None:
                    self._darkskyapi_current_usage = 1
                else:
                    self._darkskyapi_current_usage += 1

            self._darkskyapi_timestamp = datetime.now()

        except Exception as e:
            logging.error("Error making query to darksky: ",e)
            raise e

        # lsit with new data
        new_data=[]
        # list with expected columns
        expected_columns = self._get_db_columns()[3:]

        for item in query_res['hourly']['data']:

            # check presence of all columns in the query result
            for col in expected_columns:
                if col not in item.keys():
                    item[col] = None

            # add queried data to list
            new_data += [(coord[0],coord[1],
                          item['time'],item['summary'],item['icon'],
                          item['precipIntensity'],item['precipProbability'],item['precipType'],
                          item['temperature'],item['apparentTemperature'],
                          item['dewPoint'],item['humidity'],item['pressure'],
                          item['windSpeed'],item['windGust'],item['windBearing'],
                          item['cloudCover'],item['uvIndex'],item['visibility'],item['ozone'])]

        # get cursor
        c = self._dbconn.cursor()

        # insert values to the database
        try:
            # insert new weather data
            c.executemany('''
            INSERT OR REPLACE INTO weather
            (''' + ", ".join(self._get_db_columns()[1:]) + ''')
            VALUES
            ('''+ ",".join("?"*len(self._get_db_columns()[1:])) + ''')
            ''', new_data)

            # update the query_dates table
            c.executemany('''
            INSERT OR REPLACE INTO query_dates
            (latitude, longitude, time, if_queried)
            VALUES (?,?,?,?)
            ''', [(coord[0],coord[1],time,True)])

        except Exception as e:
            logging.error("Error with db insertion into weather table",e)
            self._dbconn.rollback()
            raise e

        # commit changes
        self._dbconn.commit()

    def query_darksky_weather(self):
        """Query historical weather from darksky API at required time points

        """
        # read dates and coordinates to query
        query_dates = self._read_query_dates_from_db(False)

        for q in query_dates:
            print("Requests to make: " + str(self._count_query_dates_from_db(False)))
            try:
                self._query_coordinate_time((q[1],q[2]), q[0])
            except Exception as e:
                logging.warning("Error during queries",e)
                break

    def query_local_weather(self,columns='*',where=None):
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
                FROM weather
                ''')
            else:
                c.execute('''
                SELECT
                ''' + columns + '''
                FROM weather
                WHERE ''' + where)

            query_res = c.fetchall()

        except Exception as e:
            logging.error("Error quering data from query_dates",e)
            self._dbconn.rollback()
            raise e

        # column names
        if '*' == columns:
            columns = self._get_db_columns()
        else:
            columns = columns.split(",")

        # convert to pandas
        return pd.DataFrame(np.array(query_res),columns=columns)

