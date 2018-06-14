#!/bin/env python

import pandas as pd
import datetime

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

        self._times['datetime'] = self._times.apply(datetime.datetime.fromtimestamp,axis=1)
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


class Iter_Historical_Plan(object):
    """ Iterator through plans with historical weather

    """

    def __init__(self, plan, columns='*'):
        """Initialise iterator

        :plan: must be one of the Plan_With_* objects, that is
        containing _compute_plan method given a weather at the day

        :columns: columns from weather needed for computing plan

        """
        self.plan = plan
        self.columns = columns
        self.ihw = Iter_Historical_Weather(self.plan.hw)

    def __iter__(self):
        return self

    def __next__(self):
        """Return next plan

        """
        flag=True
        while flag:
            try:
                res=self.plan._compute_plan(self.ihw.__next__(columns = self.columns))
                flag=False
            except TypeError as e:
                continue
        return res
