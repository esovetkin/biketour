#!/bin/env python

import numpy as np
import math

def earth_radius(units="m"):
    """Get earth radius in different units

    :units: units

    """
    if "m" == units:
        return 6371000
    elif "km" == units:
        return 6371
    elif "mi" == units:
        return 3959


def bearing(start,finish):
    """Compute forward azimuth between two coordinates

    :start: starting coordinate

    :finish: finish coordinate

    """

    s = math.pi * np.squeeze(np.array(start)) / 180
    f = math.pi * np.squeeze(np.array(finish)) / 180

    y = math.sin(f[1] - s[1]) * math.cos(f[0])
    x = math.cos(s[0])*math.sin(f[0]) - math.sin(s[0])*math.cos(f[0])*math.cos(f[1] - s[1])

    return math.atan2(y,x)/math.pi * 180 % 360


def rhumb_bearing(start,finish):
    """Compute rhumb bearing

    The rhumb bearing is a constant bearing that takes from start to
    finish

    :start: starting coordinate

    :finish: finish coordinate

    """
    s = math.pi * np.squeeze(np.array(start)) / 180
    f = math.pi * np.squeeze(np.array(finish)) / 180

    delta_lat = math.log(math.tan(math.pi/4 + f[0]/2)/
                         math.tan(math.pi/4 + s[0]/2))
    delta_lon = f[1]-s[1]

    if abs(delta_lon) > math.pi:
        if delta_lon > 0:
            delta_lon = -2*math.pi + delta_lon
        else:
            delta_lon = 2*math.pi + delta_lon

    res = 180*math.atan2(delta_lon,delta_lat)/math.pi

    return (res + 360) % 360


def geodesic_distance(coord1, coord2):
    """Compute geodesic distance between two coordinates

    :coord1: first coordinate

    :coord2: second coordinate

    """
    # convert coordinates to radians
    s = math.pi * np.squeeze(np.array(coord1)) / 180
    f = math.pi * np.squeeze(np.array(coord2)) / 180

    delta = (f - s)/2
    t = math.cos(f[0]) * math.cos(s[0]) * math.sin(delta[1])**2 + math.sin(delta[0])**2

    return earth_radius() * 2 * math.atan2(t**(1/2),(1-t)**(1/2))


def rhumb_distance(coord1,coord2):
    """Compute rhumb distance

    The rhumb line is a line with a constant bearing connecting start
    and finish

    :coord1: first coordinate

    :coord2: second coordinate

    """
    s = math.pi * np.squeeze(np.array(coord1)) / 180
    f = math.pi * np.squeeze(np.array(coord2)) / 180

    delta_psi = math.log(math.tan(math.pi/4 + f[0]/2)/math.tan(math.pi/4 + s[0]/2))
    delta_phi = f[0] - s[0]
    if math.isclose(delta_psi,0):
        q = math.cos(coord1[0])
    else:
        q = delta_phi/delta_psi

    delta_lambda = f[1] - s[1]
    if abs(delta_lambda) > math.pi:
        if delta_lambda > 0:
            delta_lambda = -2*math.pi + delta_lambda
        else:
            delta_lambda = 2*math.pi + delta_lambda

    return earth_radius() * (delta_phi**2 + (q*delta_lambda)**2)**(1/2)
