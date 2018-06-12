#!/bin/env python

import numpy.polynomial.polynomial as poly

from helping_functions import geodesic_distance, bearing

from iterators import Iter_Historical_Plan

import datetime

import math

import numpy as np

import pandas as pd

from pdb import set_trace as bp

class Plan_With_Constant_Power(object):
    """ Compute plan given a constant power

    """

    def __init__(self, starting_time,
                 route, historical_weather, weather_forecast,
                 **kwargs):
        """Initialise the class

        :starting_time: departure time of the journey

        :route: Route object

        :historical_weather: corresponding to route historical weather object

        :weather_forecast: corresponding to route weather forecast object

        :kwargs: an array of possible physical paramters
        """
        self.departure_hour = starting_time
        self.route = route
        self.hw = historical_weather
        self.wf = weather_forecast
        self.ihp = Iter_Historical_Plan(self)

        # parse kwargs for set parameters
        self._parse_kwargs(kwargs)

    def _parse_kwargs(self, kwargs):
        """Parse kwargs for presence of the specified arguments

        """
        if "total_mass" in kwargs.keys():
            self._total_mass = kwargs["total_mass"]
        else:
            self._total_mass = 100

        if "P_rider" in kwargs.keys():
            self._P_rider = kwargs["P_rider"]
        else:
            self._P_rider = 150

        if "drivetrain_efficiency" in kwargs.keys():
            self._drivetrain_efficiency = kwargs["drivetrain_efficiency"]
        else:
            self._drivetrain_efficiency = 0.95

        if "C_D" in kwargs.keys():
            self._C_D = kwargs["C_D"]
        else:
            self._C_D = 0.7

        if "C_rr" in kwargs.keys():
            self._C_rr = kwargs["C_rr"]
        else:
            self._C_rr = 0.005

    def _get_weather_at_location_and_time(self, weather, location, time):
        """Get the closest weather at location and time

        :weather: pandas dataframe with time, latitude and longitude

        :location: location of interest

        :time: time of interest
        """
        # select items with smallest
        x = (weather['time'] - time).apply(abs)
        res = weather[(x-x.min()).abs() < 1].copy()

        # select with closest coordinates
        res['distance'] = [geodesic_distance((location.latitude,location.longitude),x)
                           for x in zip(res.latitude, res.longitude)]
        res = res.loc[res['distance'].idxmin()]
        res = res.drop(columns=['distance'])

        return res

    def _convert_departure_hour(self, departure_hour, weather):
        """Convert departure hour to the given weather time frame

        """
        x = (weather['time'].apply(lambda x: datetime.datetime.fromtimestamp(x).hour) - departure_hour).apply(abs)

        return int(weather['time'][x.idxmin()])


    def _compute_absolute_of_air_speed(self, coord_prev, coord_cur, windSpeed, windBearing):
        """

        :data: pandas DataFrame with coordinates, windSpeed and windBearing
        """
        routeBearing = bearing(start = coord_prev, finish = coord_cur)

        return windSpeed*math.cos(routeBearing - windBearing)


    def _power_model(self,
                     P_rider, total_mass, v_wind, slope,
                     air_pressure, air_temperature, air_relative_humidity,
                     drivetrain_efficiency, C_D, C_rr,
                     specific_gas_constant_dry_air = 287.058,
                     specific_gas_constant_water_vapor = 461.495,
                     g_constant=9.8):
        """ Compute rider speed given power and other parameters

        :P_rider: power of the rider (Watts)

        :total_mass: total mass of the rider (kg)

        :v_wind: absolute speed of head wind (m/s)

        :slope: ckimbing slope (ratio of height and length)

        :air_pressure: air pressure (Pa)

        :air_temperature: air temperature (K)

        :air_relative_humidity: relative humidity (in interval [0,1])

        :drivetrain_efficiency: efficiency of the drivetrain

        :C_D: drag coefficient (default is questionable)

        :C_rr: rolling resistance coefficient

        :specific_gas_constant_dry_air: specific gas constant for dry air (J/(kg*K))

        :specific_gas_constant_water_vapor: specific gas constant for water vapor (J/(kg*K))

        :g_constant: free-body acceleration

        """
        # p_v = air_relative_humidity*610.78*10**(7.5*(air_temperature-273.15)/(air_temperature-35.85))
        # p_d = air_pressure - p_v
        # K_1 = (p_d/specific_gas_constant_dry_air + p_v/specific_gas_constant_water_vapor)*C_D/(2*air_temperature)

        K_1 = (air_pressure/specific_gas_constant_dry_air)*C_D/(2*air_temperature)

        K_2 = total_mass*g_constant * (C_rr + math.sin(math.atan(slope)))

        v_bike = poly.polyroots([-P_rider*drivetrain_efficiency/K_1,
                                 v_wind*v_wind + K_2/K_1,
                                 -2*v_wind,
                                 1])

        # get a real root
        return float(np.real(v_bike[np.angle(v_bike) == 0])[0])

    def _compute_time(self, prev_point, cur_loc):
        """Compute adjusted speed

        :plan_point: a tuple with location, weather and time

        :cur_loc: current locations

        """
        # compute distance
        distance = geodesic_distance(prev_point[0][['latitude','longitude']],
                                     cur_loc[['latitude','longitude']])

        # compute absolute wind
        v_wind = self._compute_absolute_of_air_speed(prev_point[0][['latitude','longitude']],
                                                     cur_loc[['latitude','longitude']],
                                                     prev_point[1].windSpeed,
                                                     prev_point[1].windBearing)

        # compute slope
        try:
            slope = (np.array(cur_loc.elevation) - np.array(prev_point[0].elevation))/distance
        except Exception as e:
            slope = 0

        # compute adjusted bike speed
        v_bike = self._power_model(P_rider = self._P_rider,
                                   total_mass = self._total_mass,
                                   v_wind = v_wind,
                                   slope = slope,
                                   air_pressure = prev_point[1].pressure*100,
                                   air_temperature = prev_point[1].temperature + 273.15,
                                   air_relative_humidity = prev_point[1].humidity,
                                   drivetrain_efficiency = self._drivetrain_efficiency,
                                   C_D = self._C_D,
                                   C_rr = self._C_rr)

        return distance/v_bike

    def _compute_plan(self, weather):
        """Compute the journey plan given a weather during the whole day

        The journey ends either at the final point or when there is no
        more weather data available

        :weather_at_that_day: weather at the day (at least starting
        from the departure time)

        """
        # get route together with corresponding weather coordinates
        route = self.route.get_join_coordinates()

        # convert departure time to the time of the weather timestamp
        time = self._convert_departure_hour(self.departure_hour,weather)

        plan = []

        # iterate through each location
        for k in route['point_no']:
            # get current location
            l = route.loc[route['point_no'] == k].squeeze()

            # if starting point. Here we assume that at the start
            # point_no has value 0
            if 0 == k:
                # get current weather at starting point
                w = self._get_weather_at_location_and_time(weather, l, time)

                plan += [(l,w,time)]
                continue

            # compute required time
            time += self._compute_time(plan[-1], l)

            # get weather at the reached point
            w = self._get_weather_at_location_and_time(weather, l, time)

            plan += [(l,w,time)]

        # return plan
        return self._convert_list_plan_to_pandas(plan)

    def _convert_list_plan_to_pandas(self,plan):
        """Convert plan list to pandas

        The list consists of tuples: (location, weather, time)

        """
        res = []
        for x in plan:
            res += [pd.concat([x[0],
                               x[1].rename(lambda x: "w_" + x),
                               pd.Series(x[2]).rename(lambda x: "time")])]

        return pd.concat(res,axis=1).transpose()

    def historical_plans(self):
        """Iterator through historical plans

        """
        return self.ihp

    def plan(self, forecast_days=1):
        """Compute plan with the current forecast

        :forecast_days: when to compute the forecast. Default 1 means
        tomorrow

        """
        return self._compute_plan(self.wf.forecast(forecast_days))
