#!/bin/env python

from forecast_weather import Forecast_Weather
from historical_weather import Historical_Weather
from route import Route
from physical_models import Plan_With_Wind, Plan_With_Constant_Speed, Plan_With_Wind_And_Elevation, Plan_With_Elevation

import os

class Planner(object):
    """Class that joins route, weather and time

    The class produces a tables with time, location and local weather
    at this time (historical or forecast)

    """

    def __init__(self, route_file, darksky_apikey):
        """Initialise class

        :route_path: path to a gpx file containing the route

        :darksky_apikey: key to the darksky api

        """

        self.route = Route(route_file)

        self.historical_weather = Historical_Weather(
            coordinates=self.route.get_short_coordinates(),
            darksky_apikey = darksky_apikey,
            filename=self._get_historical_weather_filename(route_file))

        self.weather_forecast = Forecast_Weather(
            coordinates=self.route.get_short_coordinates(),
            darksky_apikey = darksky_apikey)

    def _get_historical_weather_filename(self, filename):
        """Add and change extension of the route filename

        :filename: filename

        """
        filename, file_extension = os.path.splitext(filename)

        return filename + "_weather.db"

    def get_plan(self, plan_type, **kwargs):
        """Return a plan object that can iterate through historically computed
        plans and get a plan using the current forecast.

        For example, to iterate through historical plans
        ```
        plan = planner.get_plan(...)
        for plan in planner.historical_plans():
            # compute something for the plan

        plan.plan() returns the plan for current forecast
        ```

        :plan_type: either of the strings 'with_constant_speed',
        'with_wind', 'with_wind_and_elevation', 'with_elevation'

        :starting_time: departure hour (all plans)

        :speed: constant speed along the journey (constant speed plan)

        :P_rider: power of the rider (in Watts) (wind/elevation plans)

        :K: constant K describing cross-sectional area and drag
        coefficient (in Ns^2/m^2) (wind/elevation plans)

        """
        if "with_wind" == plan_type:
            return Plan_With_Wind(starting_time = kwargs["starting_time"],
                                  route = self.route,
                                  historical_weather = self.historical_weather,
                                  weather_forecast = self.weather_forecast,
                                  P_rider = kwargs["P_rider"], K = kwargs["K"])

        if "with_constant_speed" == plan_type:
            raise NotImplementedError

            return Plan_With_Constant_Speed(starting_time = kwargs["starting_time"],
                                            speed = kwargs["speed"],
                                            route = self.route,
                                            historical_weather = self.historical_weather,
                                            weather_forecast = self.weather_forecast)

        if "with_wind_and_elevation" == plan_type:
            raise NotImplementedError

            return Plan_With_Wind_And_Elevation(
                starting_time = kwargs["starting_time"],
                route = self.route,
                historical_weather = self.historical_weather,
                weather_forecast = self.weather_forecast,
                P_rider = kwargs["P_rider"], K = kwargs["K"])

        if "with_elevation" == plan_type:
            raise NotImplementedError

            return Plan_With_Elevation(starting_time = kwargs["starting_time"],
                                  route = self.route,
                                  historical_weather = self.historical_weather,
                                  weather_forecast = self.weather_forecast,
                                  P_rider = kwargs["P_rider"], K = kwargs["K"])
