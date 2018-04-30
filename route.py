#!/bin/env python

import xml.etree.ElementTree

from scipy.cluster.hierarchy import linkage, fcluster

import numpy as np

import math

import pandas as pd

class Route(object):
    """A class that parses the xml route file and stores it in memory.

    Further, this class clusters the points based on distance between
    each other (this functionality helps to reduce number of calls for
    the weather service)

    The class allows to query list of coordinates (with or without
    elevation data).
    """

    def __init__(self, gpx_path):
        """Initialise class

        """
        self.filename = gpx_path
        self._coordinates = None
        self._short_coordinates = None


    def _geodesic_distance(self, coord1, coord2):
        """Compute geodesic distance between two coordinates

        NOTE: this function can be outside the class

        :coord1: first coordinate
        :coord2: second coordinate

        """
        # Earth radius
        R = 6371000

        # convert coordinates to radians
        a = math.pi * np.array(coord1) / 180
        b = math.pi * np.array(coord2) / 180

        delta = (b - a)/2
        t = math.cos(b[0]) * math.cos(a[0]) * math.sin(delta[1])**2 + math.sin(delta[0])**2

        return R * 2 * math.atan2(t**(1/2),(1-t)**(1/2))


    def _parse_gpx(self):
        """Function tries to parse gpx file

        """
        self._coordinates = []

        for tag in xml.etree.ElementTree.parse(self.filename).iter():
            ele=None
            name=None

            if "rtept" in tag.tag:
                lat=float(list(filter(lambda x: 'lat' in x[0], tag.items()))[0][1])
                lon=float(list(filter(lambda x: 'lon' in x[0], tag.items()))[0][1])

                for x in tag:
                    if "ele" in x.tag:
                        ele=float(x.text)

                    if "name" in x.tag:
                        name=x.text

                self._coordinates += [(lat,lon,ele,name)]

            if "trkpt" in tag.tag:
                lat=float(tag.attrib['lat'])
                lon=float(tag.attrib['lon'])

                self._coordinates += [(lat,lon,ele,name)]

        self._coordinates = pd.DataFrame(self._coordinates, columns=["latitude","longitude","elevation","name"])

        return self._coordinates


    def _cluster_coordinates(self,method,max_distance):
        """Cluster coordinates for get_short_coordinates

        :method: method to perform clustering (see
        ?scipy.cluster.hierarchy.linkage)

        :max_distance: maximal distance (in meters) between the
        clusters

        """
        Z = linkage(self._coordinates[['latitude','longitude']],
                    method=method,metric=self._geodesic_distance)

        return fcluster(Z, max_distance, criterion='distance')


    def _compute_average_from_cluster(self):
        """Compute average coordinates

        """
        return self._coordinates[['latitude','longitude','elevation','cluster']].groupby('cluster').mean()


    def get_coordinates(self):
        """Return coordinates

        """
        if self._coordinates is None:
            self._coordinates = self._parse_gpx()

        return self._coordinates


    def get_short_coordinates(self, method="average",max_distance=4000):
        """Get a shorter list of coordinates by clustering them together. This
        allow to reduce number of queries to the weather API

        :method: method to perform clustering (see
        ?scipy.cluster.hierarchy.linkage)

        :max_distance: maximal distance (in meters) between the
        clusters

        """
        if self._short_coordinates is None:
            self._coordinates = self.get_coordinates()
            self._coordinates['cluster'] = self._cluster_coordinates(method, max_distance)
            self._short_coordinates = self._compute_average_from_cluster()

        return self._short_coordinates
