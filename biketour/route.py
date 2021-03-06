#!/bin/env python

import xml.etree.ElementTree

from scipy.cluster.hierarchy import linkage, fcluster

import numpy as np

import math

import pandas as pd

from helping_functions import geodesic_distance

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


    def _parse_gpx(self):
        """Function tries to parse gpx file

        """
        # list to store coordinates
        coordinates = []

        # the point number
        i = 0

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

                coordinates += [(i,lat,lon,ele,name)]
                i+=1

            if "trkpt" in tag.tag:
                lat=float(tag.attrib['lat'])
                lon=float(tag.attrib['lon'])

                coordinates += [(i,lat,lon,ele,name)]
                i+=1

        self._coordinates = pd.DataFrame(coordinates,
                                         columns=["point_no",
                                                  "latitude","longitude",
                                                  "elevation","name"])

        return self._coordinates


    def _cluster_coordinates(self,method,max_distance):
        """Cluster coordinates for get_short_coordinates

        :method: method to perform clustering (see
        ?scipy.cluster.hierarchy.linkage)

        :max_distance: maximal distance (in meters) between the
        clusters

        """
        Z = linkage(self._coordinates[['latitude','longitude']],
                    method=method,metric=geodesic_distance)

        return fcluster(Z, max_distance, criterion='distance')


    def _compute_average_from_cluster(self):
        """Compute average coordinates

        """
        return self._coordinates[['latitude','longitude','elevation','cluster']].groupby('cluster',as_index=False).mean()

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

    def get_join_coordinates(self):
        """Join coordinates and short coordinates together

        TODO: all arguments are passed to get_short_coordinates

        """
        return pd.merge(self.get_coordinates(),
                        self.get_short_coordinates(),
                        on='cluster',suffixes=('','_short'))
