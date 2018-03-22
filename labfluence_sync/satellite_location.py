#!/usr/bin/env python3
# -*- coding: utf-8 -*-
##    Copyright 2013-2014 Rasmus Scholer Sorensen, rasmusscholer@gmail.com
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
# pylint: disable-msg=C0103,C0301,C0302,R0902,R0201,W0142,R0913,R0904,
# W0221,E1101,W0402,E0202,W0201,E0102
# messages:
#   C0301: Line too long (max 80), R0902: Too many instance attributes (includes dict())
#   C0302: too many lines in module; R0201: Method could be a function; W0142: Used * or ** magic
#   R0904: Too many public methods (20 max); R0913: Too many arguments;
#   W0221: Arguments differ from overridden method,
#   W0402: Use of deprechated module (e.g. string)
#   E1101: Instance of <object> has no <dynamically obtained attribute> member.
#   R0921: Abstract class not referenced. Pylint thinks any class that raises a NotImplementedError somewhere is abstract.
#   E0102: method already defined in line <...> (pylint doesn't understand properties well...)
#   E0202: An attribute affected in <...> hide this method (pylint doesn't understand properties well...)
#   C0303: Trailing whitespace (happens if you have windows-style \r\n newlines)
#   C0111: Missing method docstring (pylint insists on docstrings, even for one-liner inline functions and properties)
#   W0201: Attribute "_underscore_first_marks_insternal" defined outside __init__ -- yes, I use it in my properties.
# Regarding pylint failure of python properties: should be fixed in newer versions of pylint.
"""
Code for dealing with satellite locations.
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


In general, I imagine this being used in two ways:

1)  Location-centric: You ask a satellite location if something has changed (possibly as part
    of a loop over all locations). If something has changed, it figures out which experiment(s)
    the change(s) is (are) related to and syncs to the local directory tree.

2)  Experiment-centric: You (or an experiment object) ask the satellite location to identify folders related to a specific
    experiment and then syncs just those folders to the experiment's folders.
    (Note: Probably cache the local directory tree in memory for maybe 1 min so you can
    rapidly sync multiple experiments...)

What about the use case where I simply want to do a one-way sync, pulling new files from a
satellite location to the local experiment data directory?
  - This should not be handled here; A satellite location knows nothing about the local
    experiment data file structure.
    Two questions: WHERE should this be handled/implemented and HOW?

HOW to implement "one-way sync to pull new files from sat loc to local exp data file structure":
 a) ExpManager parses local experiment directory tree, getting a list of expids.
    SatLoc parses remote tree, getting a list of expids.
    For each local experiment, sync all remote subentry folders into the experiment's local directory.
    This could be handled by the (already large) experimentmanager module, or it could be
    delegated to a separate module, syncmanager.py

    All other ways I've found is either where the local experiment folder is specified
    (effectively the same as the "experiment centric" implementation/usage above)
    or where an experiment ID is provided, effectively just applying the above for
    a single expid instead of all expids.

As default, sync should be one-way only: from satellite location to local data tree.
Only exception might be to update foldernames on the satellite location to match the
foldernames in the data tree.


TODO:
    # TODO: Consolidate all file structure parsing to a single module.
            We parse the file system for folders according to a folderscheme like
            './year/experiment/subentry' or 'subentry'.
            This is done both here in satellite_location and experiment_manager,
            and should be consolidated to a single parser module.


== Other design considerations: ==

    It is a design target that the subentry folder on the satellite location DOES NOT have
    to match the name of the subentry folder in the main data tree exactly, but can be
    inferred from the name by regex parsing, so that the subentry folder "RS190c PAGE analysis of b"
    will be interpreted correctly as Experiment=RS190, subentry=c.
    The experiment/subentry folder can then optionally be renamed to match the name in the main
    data tree, if the program has read permissions. (Possibly extending this to files...)


== Discussion: Monitoring filesystem changes: ==

    Consideration: Which approach is better if I want to be able to check if something has changed?
        a)  Keep a full directory list structure in memory and check against this?
        b)  Create a simple checksum of the directory structure. Re-calculate this when checking for changes.
            But then, say you discover that something has changed. Then you need to figure out
            *what* has changed. This argument would prefer strategy (a).

    There must be a library for checking for updates to file trees...
    Indeed: Watchdog is one (cross-platform). pyinotify is another (linux only). Watcher is a third (windows only).

    Attention: A lot of these 'smart' solutions use system signals to watch for changes. However, since
    the satellite location is most likely on a remote system, I'm not sure any signal will be emitted
    which the code can catch.
    Watchdog specifically states not to use the dirsnapshot module for virtual filesystems mapped to a network share.

    The general approach seems to be to store all filepaths in a dict.
    When checking for changes, generate a new dict and compare with old.
    This can be optimized by e.g. using folder's mtime (on UNIX) to assume it has not been changed.

    Watchdog library:
    - https://pythonhosted.org/watchdog/
    - http://blog.philippklaus.de/2011/08/use-the-python-module-watchdog-to-monitor-directories-for-changes/
    For Windows:
    - http://timgolden.me.uk/python/win32_how_do_i/watch_directory_for_changes.html
    On UNIX:
    - http://code.activestate.com/recipes/215418-watching-a-directory-tree-on-unix/
    - http://code.activestate.com/recipes/217829-watching-a-directory-tree-under-linux/
    - http://pyinotify.sourceforge.net/  (relies on inotify in linux kernel)
    With PyQt:
    - http://stackoverflow.com/questions/182197/how-do-i-watch-a-file-for-changes-using-python (also links other solutions)


"""
from __future__ import print_function
from six import string_types

import os
import re
import shutil
import time
# FTP not yet implemented...
#from ftplib import FTP
import logging
logger = logging.getLogger(__name__)

# from labfluencebase import LabfluenceBase
from dirtreeparsing import genPathmatchTupsByPathscheme, getFoldersWithSameProperty

try:
    from .decorators.cache_decorator import cached_property
except SystemError:
    # If the module is run from the model directory, we cannot do relative imports.
    from decorators.cache_decorator import cached_property



class SatelliteLocation(object):
    r"""
    Base class for satellite locations, treats the location as if it is a locally available file system.

    Initialize with a locationparams dict and an optional satellite manager.
    Locationparams is generally specified in a yaml/json configuration file.
    Location params dict can include the following:
        name:       The display name of the satellite location.
        description: Description of the satellite location.
        protocol:   The protocol to use. Currently, only file protocol is supported.
        uri:        The location of the satellite directory, e.g. Z:\.
        rootdir:    The folder on the satellite location, e.g. Microscopy\Rasmus.
        folderscheme: String specifying how the folders are organized, e.g. {year}/{experiment}/{subentry}. Defaults to {subentry}.
        regexs:     A dict with regular expressions specifying how to parse each element in the folderscheme.
                    The key must correspond to the name in the folderscheme, e.g. 'experiment': r'(?P<expid>RS[0-9]{3})[_ ]+(?P<exp_titledesc>.+)'
        ignoredirs: A list of directories to ignore when parsing the satellite location for experiments/subentries.
        mountcommand: Specifies how to mount the satellite location as a local, virtual filesystem.

    The rationale for keeping uri and rootdir separate is that the uri can be mounted, e.g. if it is an FTP server,
    followed by a 'cd' to the root dir.

    Notes:
    Protocol is used to determine how to access the file. This is generally handled by subclassing with the location_factory module function,
        which e.g. returns a SatelliteFileLocation subclass for 'file' protocol locations.
    URI, rootdir, and folderscheme is used to access the files.
    - URI is the "mount point", FTP server, or NFS share or similar,
        e.g. /mnt/typhoon_ftp/
        (Currently, only local file protocol is supported...)
    - Rootdir is the directory on the URI where your files are located, e.g.
        /scholer/   or  /typhoon_data/jk/Rasmus/
    - Folderscheme is used to figure out how to handle/sync files from the satellite location
        into the main experiment data tree. Examples of Folderschemes:
        ./subentry/     -> Means that data is stored in folders named after the subentry related to that data.
                        e.g. /mnt/typhoon_ftp/scholer/RS190c PAGE analysis of b/RS190c_PAGE_sybr_600V.gel
        ./              -> data is just stored in a big bunch within the root folder.
                        Use filenames to interpret what experiments/subentries they belong to, e.g.:
                            /mnt/typhoon_ftp/scholer/RS190c_PAGE_sybr_600V.gel
        ./year/experiment/subentry/  -> data is stored, e.g.
                        /mnt/typhoon_ftp/scholer/2014/RS190 Test experiment/RS190c PAGE analysis of b/RS190c_PAGE_sybr_600V.gel

    QUESTION: If the subentryId (expid+subentry_idx) does not match the experiment id, which takes preference?
    E.g. for folder:    <rootdir>/RS190 Test experiment/RS191c PAGE analysis of b/
    which takes preference?

    == Methods and Usage: ==
    This class contains a lot of code intended for a persistent/long-lived satellite location object.
    This includes code to parse a full directory tree to produce a central data structure consisting of
        dict[expid][subentry_idx] = filepath
    e.g.
        ds['RS123']['a'] = "2014/RS123 Some experiment/RS123a Relevant subentry"
    The primary file tree parsing methods are:
        genPathmatchTupsByPathscheme
        getSubentryfoldersByExpidSubidx

    Many of the methods are used to either extract info from this datastructure,
    or update this data structure:
        update_expsubfolders : Updates the two catalogs _expidsubidxbyfolder, _subentryfolderset
            and returns a three-tuple specifying what has been updated since it was last invoked.

    A few methods are intended to update the satellite location's file structure (keeping the database updated in the process).
        renameexpfolder
        renamesubentryfolder
        ensuresubentryfoldername

    Subclasses provides med-level methods for one-way syncing:
        syncToLocalDir      Syncs a satellite directory to a local directory.
        syncFileToLocalDir  Syncs a satellite file to a local path.

    Each subclass additionally provides some low-level "file system" methods, e.g. rename, copy, etc.
        rename
        listdir, isdir, join,

    Additionally, there are a few rarely-used methods:
        getFilepathsByExpIdSubIdx: Like getSubentryfoldersByExpidSubidx, but returns a list of files within the folder.


    """
    def __init__(self, locationparams, manager=None):
        self._locationparams = locationparams
        self._manager = manager
        self._fulldirectoryset = set()
        self._regexpats = None
        # self._subentryfoldersbyexpidsubidx = None # Is now a cached property; the cache value should only be located in one place and that is not here.
        self._subentryfolderset = set()
        self._expidsubidxbyfolder = None
        self._cache = dict()
        self.path = os.path # Default

    def __repr__(self):
        return "sl> {}".format(self.Name or self.Description or self.URI)

    #########################
    ### Properties ##########
    #########################

    @property
    def LocationManager(self):
        """ The locationsmanager used to manage the locations (if any). """
        return self._manager
    @property
    def Confighandler(self):
        """ The universal confighandler. """
        if self._manager:
            return self._manager.Confighandler
    @property
    def LocationParams(self):
        """ Location parameters for this satellite location. """
        return self._locationparams
    @property
    def Protocol(self):
        """ Protocol """
        return self.LocationParams.get('protocol', 'file')
    @property
    def URI(self):
        """ URI """
        return self.LocationParams.get('uri')
    @property
    def Rootdir(self):
        """ Rootdir. Can be simply '.' -- most fs methods will interpret that with getAbsPath()... """
        return self.LocationParams.get('rootdir', '.')
    @property
    def IgnoreDirs(self):
        """
        List of directories to ignore. Consider implementing glob-based syntax...
        Excluding directories from e.g. previous years can help speed up lookups and location hashing.
        """
        return self.LocationParams.get('IgnoreDirs', list())
    @property
    def Folderscheme(self):
        """ Folderscheme """
        return self.LocationParams.get('folderscheme', './subentry/')
    @property
    def Mountcommand(self):
        """ Mountcommand """
        return self.LocationParams.get('mountcommand')
    @property
    def DoNotSync(self):
        """ Use this to exclude satellite location from default sync. (You can still force sync manually). """
        return self.LocationParams.get('donotsync', False)
    @property
    def Name(self):
        """ Description """
        return self.LocationParams.get('name')
    @property
    def Description(self):
        """ Description """
        return self.LocationParams.get('description')
    @property
    def FileExcludePatterns(self):
        """ Description """
        return self.LocationParams.get('file_exclude_patterns')
    @property
    def Regexs(self):
        """
        Returns regex to use to parse foldernames.
        If regex is defined in locationparams, this is returned.
        Otherwise, try to find a default regex in the confighandler.
        If that doesn't work, ... ?
        """
        if not self._regexpats:
            ch = self.Confighandler
            if 'regexs' in self.LocationParams:
                regexs = self.LocationParams['regexs']
            elif ch:
                regexs = ch.get('satellite_regexs') or ch.get('exp_folder_regexs')
            else:
                logger.warning("Could not obtain any regex!")
                return
            logger.debug('Loading regex from config: %s', list(regexs.items()))
            self._regexpats = {schemekey : re.compile(pattern) for schemekey, pattern in regexs.items()}
        return self._regexpats
    @Regexs.setter
    def Regexs(self, regexs):
        """
        Set regexs. Format must be a dict where key corresponds to a pathscheme element (e.g. 'experiment')
        and the values must be either regex patterns or strings (which will then be compiled...)
        """
        self._regexpats = {schemekey : re.compile(regex) if isinstance(regex, string_types) else regex for schemekey, regex in regexs.items()}
        logger.debug("self._regexpats set to {}".format(self._regexpats))


    def getConfigEntry(self, cfgkey, default=None):
        """
        Returns a config key from the confighandler, if possible.
        """
        ch = self.Confighandler
        if ch:
            return ch.get(cfgkey, default)


    ##################################
    ## Not-used/diabled properties ###
    ##################################

    # Some of these should probably be implemented differently, they are just here for the concept.

    #@property
    #def FoldersByExpSub(self):
    #    """
    #    Return a dict-dict datastructure:
    #    [<expid>][<subentry_idx>] = folderpath.
    #    With this, it is easy to find a satellite folderpath for a particular experiment subentry.
    #    """
    #    return self.getSubentryfoldersByExpidSubidx()
    #
    #@property
    #def FolderStructureStat(self):
    #    """
    #    Returns a dict datastructure:
    #    [folderpath] = stat
    #    I'm not sure how much it costs to stat() a file/folder on a network share vs just listing the contents.
    #    """
    #    pass
    #
    #@property
    #def Fulldirectoryset(self):
    #    """
    #    Returns a set of all files in the datastructure.
    #    This would be very easy to compare for new files and folders:
    #    """
    #    return self._fulldirectoryset


    #####################################
    #- Properties for subentryfolders  -#
    #####################################

    @cached_property(ttl=60)
    def SubentryfoldersByExpidSubidx(self):
        """
        Returns a dict-dict with subentry folders as:
            ds[expid][subidx] = <filepath>

        Implementation discussion:
        Should this call update_expsubfolders?
        update_expsubfolders is also calling this!
        I guess this should really be the other way around.
        But currently, update_expsubfolders() takes care of resetting the cache items.
        self.update_expsubfolders() will calculate:
        self._expidsubidxbyfolder = expidsubidxbyfolder
        self._subentryfolderset = subentryfoldersset
        """
        logger.debug("Getting foldersbyexpidsubidx with self.getSubentryfoldersByExpidSubidx(), [%s]", time.time())
        foldersbyexpidsubidx = self.getSubentryfoldersByExpidSubidx()
        logger.debug("-- foldersbyexpidsubidx obtained with %s items, [%s]", foldersbyexpidsubidx if foldersbyexpidsubidx is None else len(foldersbyexpidsubidx), time.time())
        logger.debug("Invoking self.update_expsubfolders(foldersbyexpidsubidx=foldersbyexpidsubidx) [%s]", time.time())
        self.update_expsubfolders(foldersbyexpidsubidx=foldersbyexpidsubidx)
        return foldersbyexpidsubidx
    @property
    def ExpidSubidxByFolder(self):
        """
        ExpidSubidxByFolder[<folderpath] --> (expid, subidx)
        """
        if not self._subentryfolderset:
            logger.debug("Invoking self.update_expsubfolders(), %s", time.time())
            self.update_expsubfolders()
        return self._expidsubidxbyfolder
    @property
    def Subentryfoldersset(self):
        """
        set(<list of subentry folders>)
        """
        if not self._subentryfolderset:
            logger.debug("Invoking self.update_expsubfolders(), %s", time.time())
            self.update_expsubfolders()
        return self._subentryfolderset



    def update_expsubfolders(self, clearcache=False, foldersbyexpidsubidx=None):
        """
        Updates the catalog of experiment subentry folders and the complementing
        _subentryfolderset and _expidsubidxbyfolder.

        Returns a tuple of
            (newexpsubidx, newsubentryfolders, removedsubentryfolders)
        listing folder changes since last update, where:
        - newexpsubidx = set with tuples of (expid, subidx) of newly changed folder.
          (Same as _expidsubidxbyfolder[folder] for a newly changed folders)
        - newsubentryfolders = set of added subentry foldernames since last update.
        - removedsubentryfolders = set of removed subentry foldernames since last update.

        NOTICE: Can NOT be used to check for updates to files within a folder; only
                for changes to subentry foldernames / paths.
        """
        logger.debug("update_expsubfolders(clearcache=%s, foldersbyexpidsubidx='%s')",
                     clearcache, foldersbyexpidsubidx)
        # Avoid premature optimizations:
        # self.getSubentryfoldersByExpidSubidx() is the only calculation expected to be slow,
        # so only make a cached_property for this.
        if clearcache:
            logger.debug("Clearing cache for self.SubentryfoldersByExpidSubidx")
            # Note: This will delete most references to the SubentryfoldersByExpidSubidx. Maybe clear it rather than delete/reassign?
            # How is the property's del defined?
            del self.SubentryfoldersByExpidSubidx
        if foldersbyexpidsubidx is None:
            logger.debug("Obtaining foldersbyexpidsubidx = self.SubentryfoldersByExpidSubidx")
            foldersbyexpidsubidx = self.getSubentryfoldersByExpidSubidx() # self.SubentryfoldersByExpidSubidx
            # Calling self.SubentryfoldersByExpidSubidx will get foldersbyexpidsubidx and call this
            # update_expsubfolders with foldersbyexpidsubidx argument.
            # Still, if SubentryfoldersByExpidSubidx is None, this will give an infinite/cyclic recursion.
            #logger.debug("foldersbyexpidsubidx obtained")

        # Perform calculations
        expidsubidxbyfolder = {subentryfolder : (expid, subidx)
                               for expid, expdict in foldersbyexpidsubidx.items()
                               for subidx, subentryfolder in expdict.items()}
        subentryfoldersset = set(expidsubidxbyfolder.keys())
        newsubentryfolders = subentryfoldersset - self._subentryfolderset
        removedsubentryfolders = self._subentryfolderset - subentryfoldersset
        newexpsubidx = {expidsubidxbyfolder[folder] for folder in newsubentryfolders}

        self._expidsubidxbyfolder = expidsubidxbyfolder
        self._subentryfolderset = subentryfoldersset

        return (newexpsubidx, newsubentryfolders, removedsubentryfolders)



    ### DIR TREE PARSING ###

    def genPathmatchTupsByPathscheme(self, filterfun=None, matchcombiner=None, matchinit=None, rightmost=None):
        """
        Specifying regexs, basedir and folderscheme have been deprechated.
        These are taken from self.Regexs, self.Rootdir, and self.Folderscheme.
        See dirtreeparsing.genPathmatchTupsByPathscheme for more info.

        Args:
            :filterfun:     A function that determines whether the path is included in the result.
                            Default is something like:
                                lambda path: self.isdir(path) and self.path.basename(path) not in self.IgnoreDirs
            :matchcombiner: Can be used control what is returned as the second item in the two-tuples:
                            (path, matchcombiner(basematch, schemekey, match))
                            The default is to return a dict with schemekeys: match-object, i.e.:
                                matchcombiner = lambda basematch, schemekey, match: dict(basematch, **{schemekey: match})

        If you want to exclude folders based simply on their names (not path), add the foldername to self.IgnoreDirs.

        """
        basepath = self.getRealPath(os.path.normpath(self.Rootdir))
        folderscheme = self.Folderscheme
        regexs = self.Regexs
        default_filter = lambda path: self.isdir(path) and self.path.basename(path) not in self.IgnoreDirs
        filterfun = filterfun or default_filter

        foldermatchtups = genPathmatchTupsByPathscheme(basepath=basepath, folderscheme=folderscheme, regexs=regexs,
                                                       filterfun=filterfun, matchcombiner=matchcombiner,
                                                       matchinit=matchinit, rightmost=rightmost, fs=self)
        return foldermatchtups

    def genPathMatchlistTupByPathscheme(self, filterfun=None, rightmost=None):
        """
        Example to demonstrate how to use the matchcombiner argument in self.genPathmatchTupsByPathscheme.
        Instead of returning
            ('/mnt/data/nanodrop/2014/RS123 My experiment/RS123a subentryA':
             {'year': <year match>, 'experiment': <exp-match>, 'subentry': <subentry-match>})
        This method returns a two-tuple generator where the second element is a *list of match objects*,
        in the same order as folderscheme, i.e. (for folderscheme='year/experiment/subentry')
            ('/mnt/data/nanodrop/2014/RS123 My experiment/RS123a subentryA':
             [<year match>, <exp-match>, <subentry-match>])
        """
        def matchcombiner(basematch, schemekey, match):  # pylint: disable=W0613
            """ Creates a list with all match objects. """
            if basematch is None:
                basematch = []
            newlist = basematch.copy()
            newlist.append(match)
            return newlist
        return self.genPathmatchTupsByPathscheme(filterfun=filterfun, matchcombiner=matchcombiner, rightmost=rightmost)


    def genPathGroupdictTupByPathscheme(self, filterfun=None, rightmost=None):
        """
        Example to demonstrate how to use the matchcombiner argument in self.genPathmatchTupsByPathscheme.
        This also sets a starting basematch using matchinit argument (rather than handling the case in matchcombiner).
        Instead of returning
            ('/mnt/data/nanodrop/2014/RS123 My experiment/RS123a subentryA':
             {'year': <year match>, 'experiment': <exp-match>, 'subentry': <subentry-match>)

        This method returns a two-tuple generator where the second element is a single, combined match dict for the path, i.e. tuples similar to:
            ('/mnt/data/nanodrop/2014/RS123 My experiment/RS123a subentryA':
             {'year': '2014', 'exp_titledesc': 'My experiment', 'expid': 'RS123', 'subentry_idx': 'a', 'subentry_titledesc': 'subentryA'})

        Here, matches for later/deeper elements will overwrite previous match dict entries.
        In the example above, if path had been '/mnt/data/nanodrop/2014/RS123 My experiment/RS124a subentryA'
        then the resulting dict would include 'expid': 'RS124' for the /subentry/ level match.
        """
        def matchcombiner(basematch, schemekey, match):  # pylint: disable=W0613
            """ Creates a copy of basematch and updates it with the match's groupdict. """
            return dict(basematch, **match.groupdict())
        return self.genPathmatchTupsByPathscheme(filterfun=filterfun, matchcombiner=matchcombiner,
                                                 matchinit={}, rightmost=rightmost)

    def make_dirparse_kwargs(self, basepath=None, folderscheme=None, regexs=None, fs=None, filterfun=None):
        """ Generate ubiqutous keyword arguments for dirtree parsing. """
        default_filter = lambda path: self.isdir(path) and self.path.basename(path) not in self.IgnoreDirs
        return dict(basepath=basepath or self.getRealPath(),
                    folderscheme=folderscheme or self.Folderscheme,
                    regexs=regexs or self.Regexs,
                    fs=fs or self,
                    filterfun=filterfun or default_filter)

    def getExpfoldersByExpid(self):
        """
        Return datastructure:
            [expid][subentry_idx] = <filepath relative to basedir/rootdir>
        Usage:
            ds = getSubentryfoldersByExpidSubidx(...)
            subentry_fpath = ds['RS123']['a'] # returns e.g. "2014/RS123 Some experiment/RS123a Relevant subentry"
        Requirements for this method to work:
         a) Folderscheme and corresponding regexs must be configured (optionally also the rootdir).
         b) Folderscheme must specify 'experiment', e.g. './year/experiment' or just 'experiment'
         c) The regexs must specify the named group 'expid'.

        Almost identical to experimentmanager.ExperimentManager.findLocalExpsPathGdTupByExpid method.
        """
        foldermatchtuples = self.genPathGroupdictTupByPathscheme(rightmost='experiment')
        foldersbyexpid = {gd.get('expid'): path for path, gd in foldermatchtuples}
        return foldersbyexpid

    def getFoldersWithSameProperty(self, group, rightmost=None, countlim=1):
        """
        Returns folders with the same set of dirtree parsed group properties.
        Args:
            :group:     The property or group of property to filter for, e.g. 'expid', or ('expid', 'year')
            :rightmost: The rightmost part of the pathscheme to parse. E.g. for pathscheme
                        'year/experiment/subentry', setting rightmost='experiment' will only parse experiments and not subentries.
            :countlim:  Can be used to only return for groups with more than a certain number of hits,
                        e.g. setting countlim=2 will only return groups with duplicate folders.
        """
        return getFoldersWithSameProperty(group=group, rightmost=rightmost, countlim=countlim,
                                          **self.make_dirparse_kwargs())

    def getDuplicates(self, subentries=False):
        if subentries:
            return self.getDuplicateSubentries()
        else:
            return self.getDuplicateExps()

    def getDuplicateExps(self):
        """
        Returns a dict with lists of paths for experiment folders with duplicate IDs.
        """
        #foldermatchtuples = self.genPathGroupdictTupByPathscheme(rightmost='experiment')
        #listfoldersbyexp = {}
        #for folderpath, matchdict in foldermatchtuples:
        #    listfoldersbyexp.setdefault(matchdict['expid'], []).append(folderpath)
        #listfoldersbyexp = {expid: folderlist for expid, folderlist in listfoldersbyexp.items() if len(folderlist) > 1}
        #return listfoldersbyexp
        return self.getFoldersWithSameProperty(group='expid', rightmost='experiment', countlim=2)

    def getDuplicateSubentries(self):
        " Return a ... "
        return self.getFoldersWithSameProperty(group=('expid', 'subentry_idx'), rightmost='subentry', countlim=2)

    def getSubentryfoldersByExpidSubidx(self, regexs=None, basedir=None, folderscheme=None):
        """
        Return datastructure:
            [expid][subentry_idx] = <filepath relative to basedir/rootdir>
        Usage:
            ds = getSubentryfoldersByExpidSubidx(...)
            subentry_fpath = ds['RS123']['a'] # returns e.g. "2014/RS123 Some experiment/RS123a Relevant subentry"

        Requirements for this method to work:
         a) Folderscheme and corresponding regexs must be configured (optionally also the rootdir).
         b) Folderscheme must specify 'subentry', e.g. './year/experiment/subentry' or just 'subentry'
         c) The regexs must specify the named groups 'expid' and 'subentry_idx'
        Changelog:
            Deprechated the use of self.Matchpriorities and just using genPathmatchdictTupByPathscheme to
            get a combined match group dict for each path.
        """
        logger.debug("getSubentryfoldersByExpidSubidx(regexs=%s, basedir='%s', folderscheme='%s')",
                     regexs, basedir, folderscheme)
        foldermatchtuples = self.genPathGroupdictTupByPathscheme(rightmost='subentry')
        foldersbyexpidsubidx = {}
        # This runs the generator. You may want to grab as much as possible now that you have it.
        for folderpath, matchdict in foldermatchtuples:
            try:
                expid, subentry_idx = (matchdict[k] for k in ('expid', 'subentry_idx'))
            except KeyError:
                logger.warning("Matchdict %s for folderpath %s does not contain keys 'expid' and 'subentry_idx' !!",
                               matchdict, folderpath)
                continue
            foldersbyexpidsubidx.setdefault(expid, {})[subentry_idx] = folderpath
        logger.debug("expsubfolders expids: %s", foldersbyexpidsubidx.keys())
        return foldersbyexpidsubidx



    def getFilepathsByExpIdSubIdx(self, regexs=None, basedir=None, pathscheme=None):
        """
        Equivalent to getSubentryfolderssByExpIdSubIdx, but for files rather than
        subentryfolders. Probably not as useful, but implemented because I got the idea.
        Returns datastructure:
            [expid][subentry_idx] = list of filenames/filepaths for subentry relative to basedir/rootdir.

        Question: How to you handle sub-folders in subentries, e.g.
            ./2014/RS190 Something/RS190c Else/good_images/<files>   ?
        """
        regexs = regexs or self.Regexs
        basedir = basedir or self.Rootdir
        pathscheme = pathscheme or self.Folderscheme
        pathscheme = pathscheme.strip().rstrip('/')
        if not pathscheme.endswith('filename'):
            pathscheme = "/".join((pathscheme, 'filename'))
        if 'filename' not in regexs:
            regexs['filename'] = re.compile('.*')

        pathmatchtuples = self.genPathmatchTupsByPathscheme(filterfun=lambda path: True)
        pathsbyexpidsubidx = {}

        for path, matchdict in pathmatchtuples:
            expid, subentry_idx = (matchdict[k] for k in ('expid', 'subentry_idx'))
            pathsbyexpidsubidx.setdefault(expid, {}).setdefault(subentry_idx, []).append(path)
        logger.debug("pathsbyexpidsubidx: %s", pathsbyexpidsubidx)
        return pathsbyexpidsubidx


    def renameexpfolder(self, folderpath, newbasename):
        """
        If you use this method to rename folders, it will take care of keeping the database intact.
        """
        logger.debug("INVOKED renameexpfolder(%s, %s, %s)", self, folderpath, newbasename)
        logger.warning("Not implemented")

    def renamesubentryfolder(self, folderpath, newbasename):
        """
        If you use this method to rename folders, it will take care of keeping the database intact.
        """
        folderpath = self.path.normpath(folderpath)
        newbasename = self.path.normpath(newbasename)
        if self.path.dirname(newbasename) and self.path.dirname(folderpath) != self.path.dirname(newbasename):
            logger.warning("Called renamesubentryfolder(%s, %s), but the parent dirname does not match, aborting.",
                           folderpath, newbasename)
            raise OSError("Called renamesubentryfolder(%s, %s), but the parent dirname does not match." %
                          (folderpath, newbasename))
        newbasename = self.path.basename(newbasename)
        # Check if a rename is superflouos:
        if self.path.basename(folderpath) == newbasename:
            logger.warning("folderpath and newbasename has same basename: '%s' vs '%s'.", folderpath, newbasename)
        # See if folderpath is in the database:
        SubentryfoldersByExpidSubidx = self.SubentryfoldersByExpidSubidx
        ExpidSubidxByPath = self.ExpidSubidxByFolder
        if folderpath not in ExpidSubidxByPath:
            logger.warning("Called renamesubentryfolder(%s, %s), but folderpath is not in self._expidsubidxbyfolder",
                           folderpath, newbasename)
            return
        # Try to perform filesystem rename:
        try:
            # os.rename returns None if rename operations succeeds. We should do the same.
            self.rename(folderpath, newbasename)
        except OSError as e:
            logger.error("Error while trying to rename '%s' to '%s' --> %s", folderpath, newbasename, e)
            raise ValueError("Error while trying to rename '%s' to '%s' --> %s" % (folderpath, newbasename, e))

        parentfolder = self.path.dirname(folderpath)
        newfolderpath = self.path.normpath(self.path.join(parentfolder, newbasename))

        # Update the database:
        # self.SubentryfoldersByExpidSubidx (cached property)
        # ExpidSubidxByPath = self._expidsubidxbyfolder = self.Expidsubidxbyfolder
        # self._subentryfolderset = None
        # Update ExpidSubidxByFolder:
        expid, subidx = ExpidSubidxByPath.pop(folderpath)   # remove old path
        ExpidSubidxByPath[newfolderpath] = (expid, subidx)  # insert new path
        # Update SubentryfoldersByExpidSubidx (by overwriting old value):
        SubentryfoldersByExpidSubidx[expid][subidx] = newfolderpath
        if self._subentryfolderset:
            self._subentryfolderset.discard(folderpath)
            self._subentryfolderset.add(newfolderpath)

        return newfolderpath


    def ensuresubentryfoldername(self, expid, subidx, subentryfoldername):
        """
        Can be used to ensure that a subentry-folder is correctly named.
        Returns:
            None if expid/subidx is not found in this datastore,
            None if no foldername already matches,
            True if a rename was performed,
            False if renaming failed.
        """
        subentryfoldername = self.path.basename(subentryfoldername)
        subentryfoldersbyexpidsubidx = self.SubentryfoldersByExpidSubidx
        if expid not in subentryfoldersbyexpidsubidx:
            logger.warning("Expid '%s' not present in this satellite store.", expid)
            return
        if subidx not in subentryfoldersbyexpidsubidx[expid]:
            logger.warning("Subentry '%s' for experiment '%s' not present in this satellite store.", subidx, expid)
            return
        currentfolderpath = subentryfoldersbyexpidsubidx[expid][subidx]
        currentfolderbasename = self.path.basename(currentfolderpath)
        if currentfolderbasename == subentryfoldername:
            logger.info("currentfolderbasename == subentryfoldername : '%s' == '%s'", currentfolderbasename, subentryfoldername)
            return
        try:
            # self.rename(currentfolderpath, subentryfoldername)
            # Make sure you use the encapsulated rename to update the database...
            self.renamesubentryfolder(currentfolderpath, subentryfoldername)
        except OSError as e:
            logger.warning("OSError while renaming '%s' to '%s' :: '%s", currentfolderpath, subentryfoldername, e)
            return False
        # subentryfoldersbyexpidsubidx[expid][subidx] = subentryfoldername # Uh... this should be the path... but updating in self.renamesubentryfolder
        return True


    ### Methods for subclasses:

    def rename(self, path, newname):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("rename() not implemented for base class - something is probably wrong.")

    def isdir(self, path):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("%s not implemented for base class - something is probably wrong.")
    def listdir(self, path):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("%s not implemented for base class - something is probably wrong.")
    def join(self, *paths):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("%s not implemented for base class - something is probably wrong.")
    def getRealPath(self, path='.'):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("%s not implemented for base class - something is probably wrong.")
    def mount(self, path):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("%s not implemented for base class - something is probably wrong.")
    def isMounted(self):
        """ Override in filesystem/ressource-dependent subclass. """
        raise NotImplementedError("%s not implemented for base class - something is probably wrong.")




class SatelliteFileLocation(SatelliteLocation):
    """
    This is either a local folder or another resource that has been mounted as a local file system,
    and is available for manipulation using standard filehandling commands.
    In other words, if you can use ls, cp, etc on the location, this is the class to use.
    """

    def __init__(self, locationparams):
        super(SatelliteFileLocation, self).__init__(locationparams=locationparams)
        # python3 is just super().__init__(uri, confighandler)
        # old school must be invoked with BaseClass.__init__(self, ...), like:
        # SatelliteLocation.__init__(self,
        self.ensureMount()
        self.path = os.path # Make this class work like the standard os.path.


    def ensureMount(self):
        """
        Ensures that the file location is available.
        """
        if not self.isMounted():
            logger.warning("SatelliteFileLocation does not seem to be correctly mounted (it might just be empty, but hard to tell) -- %s -- will try to mount with mountcommand...", self.URI)
            ec = self.mount()
            return ec
        logger.debug("SatelliteFileLocation correctly mounted (well, it is not empty): %s", self.URI)

    def mount(self, uri=None):
        """
        Uses mountcommand to mount; is specific to each system.
        Not implemented yet.
        Probably do something like #http://docs.python.org/2/library/subprocess.html
        """
        if uri is None:
            uri = self.URI
        mountcommand = self.Mountcommand
        if not mountcommand:
            logger.warning("Trying to mount satellite location %s, but mount command is: %s", uri, mountcommand)
            return
        import subprocess, sys
        errorcode = subprocess.call(mountcommand, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)
        logger.info("Mount command '%s' returned with errorcode %s", mountcommand, errorcode)
        return errorcode

    def isMounted(self):
        """ Tests if a location is mounted (by checking if it is non-empty)."""
        path = self.getRealRootPath()
        try:
            return len(os.listdir(path))
        except FileNotFoundError as err:
            logger.info("FileNotFoundError while trying to list dir %s: %s", path, err)
            return False

    def getRealRootPath(self):
        """ Returns self.getRealPath('.') """
        return self.getRealPath()

    def getRealPath(self, path='.'):
        """
        Why not just os.path.realpath(path) for consistency, defaulting to self.Rootdir for path?
        Because we need the URI. Rootdir is the directory APPENDED TO the location's URI.
        """
        return os.path.normpath(os.path.join(self.URI, self.Rootdir, path))

    def listdir(self, path):
        """ Implements directory listing with os.listdir(...) """
        if os.path.isabs(path):
            return os.listdir(path)
        return os.listdir(os.path.join(self.getRealRootPath(), path))

    def join(self, *paths):
        """ Joins filesystem path elements with os.path.join(*paths) """
        return os.path.join(*paths)

    def isdir(self, path):
        """ os.path.isdir(...) """
        res = os.path.isdir(os.path.join(self.getRealRootPath(), path))
        #logger.debug("SatelliteFileLocation.isdir(%s) returns %s", path, res)
        return res

    def rename(self, path, newname):
        """ Renames basename of path to newname using os.rename(path, newname) """
        os.rename(path, newname)


    def syncToLocalDir(self, satellitepath, localpath, verbosity=0, dryrun=False):
        """
        Consider making a call to rsync and see if that is available, and only use the rest as a fallback...
        # Note, if satellitepath ends with a '/', the basename will be ''.
        # This will thus cause the contents of satellitepath to be copied into localpath, rather than localpath/foldername
        # I guess this is also the behaviour of e.g. rsync, so should be ok. Just be aware of it.
        """
        if not os.path.isdir(localpath):
            logger.warning("localpath NOT A DIRECTORY, skipping...\n--'%s'", localpath)
            return
        realpath = self.getRealPath(satellitepath)
        # If it is just a file:
        if os.path.isfile(realpath):
            #print("verbosity:", verbosity, "; dryrun:", dryrun)
            return self.syncFileToLocalDir(satellitepath, localpath, verbosity=verbosity, dryrun=dryrun)
        elif not os.path.isdir(realpath):
            logger.warning("satellitepath is not a file or directory, skipping...\n--'%s'", realpath)
            return
        # We have a folder:
        foldername = os.path.basename(satellitepath)
        # If the folder does not exists in localpath destination, just use copytree:
        if not os.path.exists(os.path.join(localpath, foldername)):
            logger.info(u"Remote folder not present in source, invoking shutil.copytree('%s', os.path.join('%s', '%s'))", realpath, localpath, foldername)
            if verbosity > 0:
                print("%s\t%s\t %s \t %s" % ('N', 'copytree', realpath, os.path.join(localpath, foldername)))
            if not dryrun:
                shutil.copytree(realpath, os.path.join(localpath, foldername))    # Does copytree return anything? Or does it just raise errors?
            return
        # foldername already exists in local directory, just recurse for each item...
        for item in os.listdir(realpath):
            self.syncToLocalDir(os.path.join(satellitepath, item), os.path.join(localpath, foldername), verbosity=verbosity, dryrun=dryrun)


    def syncFileToLocalDir(self, satellitepath, localpath, verbosity=0, dryrun=False):
        """
        Syncs A FILE to local dir.
        True = File was copied, False = Sync failed, None = File not copied.
        """
        if not os.path.isdir(localpath):
            logger.warning("Destination localpath '%s' is not a directory, skipping...", localpath)
            ## Consider perhaps creating destination instead...?
            return False
        srcfilepath = self.getRealPath(satellitepath)
        if not os.path.isfile(srcfilepath):
            logger.info("Source file '%s' is not a file, skipping...", srcfilepath)
            if verbosity > 0:
                print("%s\t%s\t %s \t %s" % ('S!', 'skipping', srcfilepath, '<src is not a file>')) # Symbols: N=New, O=Overwrite, S=Skipping
            return False
        filename = os.path.basename(srcfilepath)
        import fnmatch
        if self.FileExcludePatterns:
            if any(fnmatch.fnmatch(filename, pat) for pat in self.FileExcludePatterns):
                logger.info("File excluded by pattern: %s", srcfilepath)
                if verbosity > 1:
                    print("Excluding file '%s'" % srcfilepath)
                return
        destfilepath = os.path.join(localpath, filename)
        if not os.path.exists(destfilepath):
            logger.info("Destfilepath does not exists. Invoking shutil.copy2(\n'%s',\n'%s')", srcfilepath, destfilepath)
            if verbosity > 0:
                # Symbols: N=New, O=Overwrite, S=Skipping
                print("%s\t%s\t %s \t %s" % ('N', 'copy2   ', srcfilepath, destfilepath))
            if not dryrun:
                shutil.copy2(srcfilepath, destfilepath)
            return
        lastmodst = "\n".join("-- {} last modified: {}".format(f, modtime)
                              for f, modtime in (('srcfile ', time.ctime(os.path.getmtime(srcfilepath))),
                                                 ('destfile', time.ctime(os.path.getmtime(destfilepath)))))
        logger.debug("Destfile exists: '%s'\n%s", destfilepath, lastmodst)
        if os.path.isdir(destfilepath):
            logger.warning("Destfilepath '%s' is a directory in localpath (but a file on source). Cannot sync, skipping...", destfilepath)
            if verbosity > 0:
                print("%s\t%s\t %s \t %s" % ('S!', 'skipping', srcfilepath, '<dest is a directory (unexpected)>')) # Symbols: N=New, O=Overwrite, S=Skipping
            return False
        if not os.path.isfile(destfilepath):
            logger.warning("Destfilepath '%s' exists but is not a file (but a file on source). Cannot sync,  skipping...", destfilepath)
            if verbosity > 0:
                print("%s\t%s\t %s \t %s" % ('S!', 'skipping', srcfilepath, '<dest is not a file (unexpected)>')) # Symbols: N=New, O=Overwrite, S=Skipping
            return False
        # destfilepath is a file, determine if it should be overwritten...
        # Add 10 seconds to account for time differences between network and local:
        if round(os.path.getmtime(srcfilepath)) > round(os.path.getmtime(destfilepath))+10:
            logger.info("srcfile NEWER than destfile, OVERWRITING destfile... ('%s')", filename)
            logger.debug("\n--srcfile: '%s'\n--dstfile: '%s'\n--Invoking shutil.copy2(%s, %s)",
                         srcfilepath, destfilepath, srcfilepath, destfilepath)
            if verbosity > 0:
                # Symbols: N=New, O=Overwrite, S=Skipping
                print("%s\t%s\t %s \t %s" % ('O', 'copy2   ', srcfilepath, destfilepath))
            if not dryrun:
                shutil.copy2(srcfilepath, destfilepath)
            return
        else:
            logger.info("srcfile NOT newer than destfile, SKIPPING... verbosity=%s, ('%s')", verbosity, filename)
            logger.debug("\n--srcfile: '%s'\n--dstfile: '%s'", srcfilepath, destfilepath)
            if verbosity > 1:
                # Symbols: N=New, O=Overwrite, S=Skipping
                print("%s\t%s\t %s \t %s" % ('S', 'skipping   ', srcfilepath, destfilepath))






#
#class SatelliteFtpLocation(SatelliteLocation):
#    """
#    This class is intended to deal with ftp locations.
#    This has currently not been implemented.
#    On linux, you can mount ftp resources as a locally-available filesystem using curlftpfs,
#    and use the SatelliteFileLocation class to manipulate this location.
#
#    Other resources that might be interesting to implement:
#    (probably by interfacing with helper libraries)
#    - NFS
#    - http
#    - webdav
#    - ...
#    """
#    def __init__(self, locationparams):
#        SatelliteLocation.__init__(self, locationparams=locationparams)
#
#    def rename(self, path, newpath):
#        raise NotImplementedError("rename() not implemented for FTP class.")




location_types = {'file' : SatelliteFileLocation}


def location_factory(locationparams):
    """
    Create a satellitelocation object, deriving the correct sub-class from the protocol
    in locationparams.
    """
    protocol = locationparams.get('protocol', 'file')
    LocationCls = location_types[protocol]
    return LocationCls(locationparams=locationparams)




if __name__ == '__main__':
    pass
