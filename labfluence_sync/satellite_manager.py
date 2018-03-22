#!/usr/bin/env python3
# -*- coding: utf-8 -*-
##    Copyright 2013 Rasmus Scholer Sorensen, rasmusscholer@gmail.com
##
##    This program is free software: you can redistribute it and/or modify
##    it under the terms of the GNU General Public License as published by
##    the Free Software Foundation, either version 3 of the License, or
##    (at your option) any later version.
##
##    This program is distributed in the hope that it will be useful,
##    but WITHOUT ANY WARRANTY; without even the implied warranty of
##    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
##    GNU General Public License for more details.
##
##    You should have received a copy of the GNU General Public License
##
# pylint: disable-msg=C0103,C0301,C0302,R0902,R0201,W0142,R0913,R0904,W0221,E1101,W0402,E0202,W0201

"""
Code for managing satellite locations.
Consider using virtualfs python module to normalize external locations,
rather than implementing ftp, etc...

Object graph:
                        Experiment
                       /          \
            Filemanager             WikiPage
           /
SatelliteMgr
           \
            SatelliteLocation

"""
from __future__ import print_function, absolute_import
from collections import OrderedDict
import logging
logger = logging.getLogger(__name__)


from .satellite_location import location_factory



class SatelliteManager(LabfluenceBase):
    """
    Manager class for satellite locations.
    """
    def __init__(self, confighandler):
        # LabfluenceBase.__init__(self, confighandler)
        if 'satellitemanager' not in confighandler.Singletons:
            confighandler.Singletons['satellitemanager'] = self
        # Initialize dict:
        self._satellitelocations = {} # dict with name : satellite-location-object
        self.loadLocations()




    @property
    def SatelliteLocations(self):
        """
        Satellite locations. Stored as a dict( name -> location-object )
        """
        return self._satellitelocations

    def getLocationsSorted(self):
        """
        Returns a sorted copy of the satellite locations:
        """
        return OrderedDict(sorted(self.SatelliteLocations.items()))

    def getLocationNames(self):
        """
        Returns a sorted list of satellite location names.
        """
        return sorted(self.SatelliteLocations.keys())

    def loadLocations(self):
        """
        Loads satellite locations from confighandler.
        Note: Will overwrite dict with current locations.
        If you have added new location objects that you want to save,
        invoke saveLocations().
        """
        locationscfg = self.Confighandler.get('satellite_locations')
        if not locationscfg:
            return {}
        for name, locationparams in locationscfg.items():
            self._satellitelocations[name] = loc = location_factory(locationparams)
            logger.debug("Location added: %s : %s", name, loc)

    def get(self, name):
        """ Return location object for specified location. """
        return self.SatelliteLocations[name]
