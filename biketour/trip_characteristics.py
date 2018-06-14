#!/bin/env python

import pandas as pd
import datetime

class Trip_Characteristics:
    """Given a plan the class returns a list of available journey
    characteristics

    """

    def __init__(self):
        """Initialise the class

        """

    def _compute_journey_date(self, plan):
        """Compute journey date

        """
        return str(datetime.datetime.fromtimestamp(plan.time[0]).date())


    def _compute_journey_time(self, plan):
        """Compute journey longevity

        """
        return plan['time'].max()-plan['time'].min()

    def _compute_journey_distance(self, plan):
        """Compute joruney distance

        """

    def _compute_wind_index(self, plan):
        """Compute some wind quantities along the route

        """

    def _compute_climbing_index(self, plan):
        """Compute some climbing quantitites along the route

        """

    def _compute_column_quantities(self, plan, column):
        """Compute weather related quantities along the journey

        """
        return (plan[column].min(),
                plan[column].max(),
                plan[column].mean())


    def _compute_precipitation_quantities(self, plan):
        """Compute some precipitation quantities.

        Such as overall probability and overall amount

        """
        x = plan[['w_time','w_precipProbability','w_precipIntensity']]\
            .drop_duplicates(subset="w_time")

        # silly probability computation, assuming independent events

        return (1-(1-x.w_precipProbability).prod(min_count=1),
                x.w_precipIntensity.sum(min_count=1))

    def _compute_most_likely_summary(self, plan):
        """Get most common summary

        """
        return plan.w_summary.value_counts().idxmax()

    def _get_all(self, plan):
        """Compute all plan characteristics

        :plan: a pandas DataFrame with journey coordinates and weather

        """

        res={}
        res["date"] = self._compute_journey_date(plan)
        res["time"] = self._compute_journey_time(plan)
        # res["distance"] = self._compute_journey_distance(plan)
        # res["wind_index"] = self._compute_wind_index(plan)
        # res["climbing_index"] = self._compute_climbing_index(plan)

        x = self._compute_precipitation_quantities(plan)
        res["w_precipProbability"] = x[0]
        res["w_precipIntensity"] = x[1]

        res["summary"] = self._compute_most_likely_summary(plan)

        column_quantites = ['w_temperature','w_apparentTemperature',
                            'w_dewPoint','w_humidity','w_pressure',
                            'w_cloudCover','w_uvIndex', 'w_visibility','w_ozone']

        for column in column_quantites:
            x = self._compute_column_quantities(plan,column)
            res[column + "_Min"] = x[0]
            res[column + "_Max"] = x[1]
            res[column + "_Average"] = x[2]

        return res

    def compute_historical(self, plan):
        """Compute historical plan characteristics

        :plan: plan object (e.g. Plan_With_Constant_Power)

        """
        i = 0
        res = []

        for x in plan.historical_plans():
            i += 1
            print("Computing historical plans, progress: " + str(i))

            res += [self._get_all(x)]

        return pd.DataFrame(res)
