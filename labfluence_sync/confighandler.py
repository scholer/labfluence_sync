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
# pylint: disable-msg=C0103,C0301,C0302,R0902,R0201,W0142,R0913,R0904,W0221
# pylint: disable-msg=W0142,R0904
# messages:
#   C0301: Line too long (max 80), R0902: Too many instance attributes (includes dict())
#   C0302: too many lines in module; R0201: Method could be a function; W0142: Used * or ** magic
#   R0904: Too many public methods (20 max); R0913: Too many arguments;
#   W0221: Arguments differ from overridden method
"""
Confighandler module includes all logic to read, parse and save config and


## TODOs ##

# TODO: implement check for whether a config (file) has been updated elsewhere.
Implementation suggestion:
1) Load config from file = cfgfromfile
2) Check if cfgfromfile['lastsaved'] is newer than cfginmemory['lastsaved']
    If cfgfromfile is NOT newer, then it can simply be overwritten.
3) If cfgfromfile IS newer, then for each keyfromfile, valuefromfile in cfgfromfile.items():
    if keyfromfile in cfginmemory and valuefromfile != cfginmemory[keyfromfile]:
        cfginmemory[keyfromfile] = valuefromfile # update
        self.invokeEntryChangeCallback(keyfromfile, valuefromfile) # PUBLISH the update
Here I intend for objects which derive their Properties from
confighandler to subscribe to updates and update their properties
if required and call self.invokePropertyCallbacks(...) to update downstream objects,
e.g. widgets subscribing to em.ActiveExperiments.
For experiment configs this becomes a bit more tedious.
I can either do everything in the experiment object,
making sure that changes to keys are propagated,
or I can let the HierarchicalConfigHandler publish the update messages,
and then make sure that the corresponding experiment is
able to pick this up.

The latter would likely require using pypubsub module or similar.

Alternatively, the announcement could go as:
1) HierarchicalConfigHandler (HCH) detects that cfgfromfile has updated keys.
2) during the merge/update of cfginmemory, all changed keys are collected.
3a) HCH announces this via the regular confighandler callback system,
    using the config's path as announcement key,
    and pass the set of changed cfgkeys as the newvalue argument.
3b) Alternatively, the updated keys could just be returned to the caller,
    which would in many cases be the experiment object it self.
    This is simpler, but maybe also less reliable.
    I would have to check who calls HCH.saveConfig(path, ...) and how/why.
    Update: HCH.saveConfig is called by ExpConfigHandler.saveExpConfig - which is again only
    called by ECH.updateAndPersist(path, props).
4) The experiment object picks up the set of changed keys and
    updates properties and invokePropertyCallback(...) as required.


"""

from __future__ import print_function
from six import string_types
import os
import os.path
import yaml
import json
from datetime import datetime
import collections
from collections import OrderedDict
import logging
logger = logging.getLogger(__name__)
try:
    from tkinter import TclError # Used by the callback system
except ImportError:
    from Tkinter import TclError # Python 2

from pathutils import getPathParents

MODELDIR = os.path.dirname(os.path.realpath(__file__))
APPDIR = os.path.dirname(MODELDIR)


def check_cfgs_and_merge(cfginmemory, cfgfromfile):
    """
    Compares two configs (intended as one from memory and the same reloaded from file).
    Uses 'lastsaved' entry to determine if cfgfromfile is newer than cfginmemory.
    If it is, then update cfginmemory with cfgfromfile, keeping track
    of which config entries are truely new.

    Usecase:
        You have a config file in memory. You need to save that back to file.
        However, the file may have been updated since the app loaded it, and
        you do not want to overwrite any new or updated items.
        Solution: Load the config file as the dict cfgfromfile, then use
            check_cfgs_and_merge(cfginmemory, cfgfromfile)
        to update the config you have in memory.

    Returns three sets of keys:

        keysupdatedfromfile, keysupdatedinmemory, changedkeys

    where:

        keysupdatedfromfile is a set of keys that were found to be updated in cfgfromfile,
                i.e. present in cfgfromfile and different from the corresponding key in cfginmemory,
                IF cfgfromfile was never (saved more recently) than cfginmemory.
                If cfgfromfile is not newer, this will be an empty set.

        keysupdatedinmemory is a set of keys that were found to be updated in cfginmemory
                compared to cfgfromfile.
                If cfgfromfile is newer than cfginmemory, this will be an empty set.

        changedkeys is the symmetric difference of the set of keys from the two configs,
        i.e. the keys that are present in one of the configs but not the other.

    """
    #keysupdatedinmemory = set()
    #keysupdatedfromfile = set()
    if not 'lastsaved' in cfgfromfile or cfgfromfile['lastsaved'] <= cfginmemory.get('lastsaved', datetime.fromordinal(1)):
        # cfgfromfile is NOT newer (they might be the same)
        # check what has been updated in cfginmemory since last save:
        # Note: Do not just use cfgfromfile.get(keyinmemory) - values that are boolean False may be significant.
        #for keyinmemory, valueinmemory in cfginmemory.items():
        #    if keyinmemory not in cfgfromfile or valueinmemory != cfgfromfile[keyinmemory]:
        #        #cfginmemory[keyinmemory] = valueinmemory # does not make sense.
        #        keysupdatedinmemory.add(keyinmemory)
        # with set comprehension:
        keysupdatedinmemory = {keyinmemory for keyinmemory, valueinmemory in cfginmemory.items()
                               if keyinmemory not in cfgfromfile or valueinmemory != cfgfromfile[keyinmemory]}
        # Should cfginmemory be updated with items in cfgfromfile that are not present in cfginmemory?
        # Probably not; otherwise you would never be able to delete a key!
    else:
        # cfgfromfile IS NEWER; update cfginmemory:
        #for keyfromfile, valuefromfile in cfgfromfile.items():
        #    if keyfromfile not in cfginmemory or valuefromfile != cfginmemory[keyfromfile]:
        #        cfginmemory[keyfromfile] = valuefromfile # update
        #        keysupdatedfromfile.add(keyfromfile)
        # with set comprehension, plus just use update:
        keysupdatedfromfile = {keyfromfile for keyfromfile, valuefromfile in cfgfromfile.items()
                               if keyfromfile not in cfginmemory or valuefromfile != cfginmemory[keyfromfile]}
        cfginmemory.update(cfgfromfile)

    ## Question: If a key is in cfginmemory but not in cfgfromfile AND cfgfromfile is newer,
    ## does it make sense to keep the key in cfginmemory or should it be removed?
    ## I say... It should stay. The risk is too high that you lose something critical if you drop keys.
    changedkeys = set(cfginmemory.keys()).symmetric_difference(cfgfromfile.keys())
    return keysupdatedfromfile, keysupdatedinmemory, changedkeys



# from http://stackoverflow.com/questions/4579908/cross-platform-splitting-of-path-in-python
def os_path_split_asunder(path, debug=False):
    """
    Can be used to split directory paths into individual parts.
    """
    parts = []
    while True:
        newpath, tail = os.path.split(path)
        if debug:
            logger.debug(repr(path), (newpath, tail))
        if newpath == path:
            assert not tail
            if path:
                parts.append(path)
            break
        parts.append(tail)
        path = newpath
    parts.reverse()
    return parts


def saveConfig(outputfn, config, updatelastsaved=True):
    """
    For internal use; does the actual saving of the config.
    Can be easily mocked or overridden by fake classes to enable safe testing environments.
    """
    logger.debug("Saving config (type: '%s') to path: %s", type(config), outputfn)
    try:
        # default_flow_style=False -> make the output "prettier" (block style)
        # width=-1 -> disables line-wrapping in Ruby, but doesn't work for PyYAML,
        # because yaml.dumper.Emitter checks if width > self.best_indent*2 (and reverts to 80 otherwise)
        # setting width=400 to only wrap very long lines...
        # line_break is the line-termination (EOL) character.
        if updatelastsaved:
            old_lastsaved = config.get('lastsaved')
            config['lastsaved'] = datetime.now()
        yaml.dump(config, open(outputfn, 'wb'), default_flow_style=False, width=400)
        logger.info("Config saved to file: %s", outputfn)
        return True
    except IOError as e:
        # This is to be expected for the system config...
        logger.warning("Could not save config to file '%s', error raised: %s", outputfn, e)
        config['lastsaved'] = old_lastsaved

def loadConfig(inputfn, storage_format='yaml'):
    """
    Load config (dict) from file.
    """
    logger.debug("Loading config from path: %s", inputfn)
    with open(inputfn) as fd:
        if storage_format == 'json':
            cfg = json.load(fd)
        else:
            cfg = yaml.load(fd)
    return cfg

def _printConfig(config, indent=2):
    """
    Returns a pretty string representation of a config.
    """
    return "\n".join(u"{indent}{k}: {v}".format(indent=' '*indent, k=k, v=v) for k, v in config.items())



class ConfigHandler(object):
    """
    For now, the configs are "flat", i.e. no nested entries, ala config["subject"]["key"] = value. Only config["key"] = value.

    A config type can be added through the following mechanisms:
    1.  By specifying ch.ConfigPaths[cfgtype] = <config filepath>.
        ConfigPaths are read during autoRead().
        Specifying this way requires the config filepath to be directly or indirectly
        specified in the source code. This is thus only done for the 'system' and 'user' configs.

    2.  Specifying ch.Config_path_entries[cfgtype] = <config key name>
        During autoRead, if a config contains a key <config key name>, the value of that config entry
        is expected to be a file path for a config of type <cfgtype>.
        This is e.g. how 'exp' cfgtype is set up to load, obtaining the exp config filepath from the 'user' config.
        This requries a defining the cfgtype and config-key in the source code, but no hard-coding of filepaths.
        Note: These configs are added to AutoreadNewFnCache, which is used to
            a) Make sure not to load new configs many times, and
            b) Adding the config filepath to ch.ConfigPaths upon completing ch.autoRead().
        The latter ensures that e.g. 'exp' config can be saved with ch.saveConfig().

    3.  Using ch.addNewConfig(inputfn, cfgtype).
        This will also add the config filepath to ch.ConfigPaths, making it available for ch.saveConfigs().
        (Can be disabled by passing rememberpath=False).

    4.  In a config file by defining: config_define_new: <dict>
        where <dict> = {<cfgtype>: <config filepath>}
        This works as an ad-hoc alternative to setting ch.Config_path_entries.
        This does not require any hard-coding/changes in the source code, but might add some security
        concerns. Therefore, using this requries ch.AllowNewConfigDefinitions=True.
        This is used for e.g. defining 'templates' cfgtype.

        Defining a new config can be done in a yaml as:
        config_define_new = {<cfgtype> : <path>}
        where path if relative, is loaded relative to the current config path.

    """

    def __init__(self, systemconfigfn=None, userconfigfn=None):
        self.VERBOSE = 0
        self.ConfigPaths = OrderedDict()    # dict of <cfgtype>: <config filepath>
        self.Configs = OrderedDict()        # dict of <cfgtype>: <config dict>
        # For retrieving paths via config entries...
        self.Config_path_entries = dict(system='system_config_path', user='user_config_path')
        # Config_path_entries is used to map config entries to a config type,
        # for instance, with the setting above, the config key "system_config_path" can be used to
        # specify a file path for the 'system' config.
        self.ConfigPaths['system'], self.ConfigPaths['user'] = systemconfigfn, userconfigfn
        self.Configs['system'] = {}
        self.Configs['user'] = {}
        self.Singletons = {} # dict for singleton objects; makes it easy to share objects across application objects that already have access to the confighandler singleton.
        self.DefaultConfig = 'user' # which config to save new config items to.
        self.AutoreadNewFnCache = dict() #list()
        self.ReadFiles = set() # which files have been read.
        self.ReadConfigTypes = set() # which config types have been read. Used to avoid circular imports.
        self.Autosave = False # if set to true, will automatically save a config to file after
        self.CheckFileTimeBeforeSave = False # (not implemented) if set to True, will check the file's changetime before overwriting.
        self.CheckFileTimeAgainstCache = False # (not implemented) if set to True, will check
        # a config file's last-modified time status before returning the cached value.
        # If the file's last-modified time is later than the cache's last-save time, the
        # config is read from file and used to update the cache. If self.Autosave is True,
        # the updated config is then saved to file.

        # Setting either of these to true requires complete trust in the putative users:
        self.AllowChainToSameType = True # If one system config file has been loaded, allow loading another?
        self.AllowNextConfigOverrideChain = True # Similar, but does not alter the original config filepath.
        self.AllowNewConfigDefinitions = True   # Allow one config to define a new config.
        self.AllowCfgtypeOverwrite = False

        # Attributes for the callback system:
        self.EntryChangeCallbacks = dict()   # dict with: config_key : <list of callbacks>
        self.ChangedEntriesForCallbacks = set() # which config keys has been changed.
        logger.debug("ConfigPaths : %s", self.ConfigPaths)


    def getSingleton(self, key):
        """
        Return a registrered singleton by key, e.g.:
            getSingleton('ui') -> return registrered UI
        """
        return self.Singletons.get(key)

    def setSingleton(self, key, value):
        """
        Set application-wide singleton by key, e.g.:

        """
        if key in self.Singletons:
            logger.info("key '%s' already in self.Singletons, overriding with new singleton object '%s'.", key, value)
        self.Singletons[key] = value


    def addNewConfig(self, inputfn, cfgtype, rememberpath=True):
        """
        Add a new config to the list of configs, e.g.:
            addNewConfig('config/test_config.yml', 'test')
        """
        if cfgtype in set(self.Configs).union(self.ConfigPaths) and not self.AllowCfgtypeOverwrite:
            logger.warning("addNewConfig() :: cfgtype already present in configs and overwrite not allowed; aborting...")
            return
        if rememberpath:
            self.ConfigPaths[cfgtype] = inputfn
        self.Configs[cfgtype] = {}
        self.readConfig(inputfn, cfgtype)


    def getConfigPath(self, cfgtype='all'):
        """
        Get the path for a particular config, has three return values:
            getConfigPath('all') -> returns self.ConfigPaths.values()
            getConfigPath('system') -> return self.ConfigPaths['system']
            getConfigPath('system', aslist=True) -> return ( self.ConfigPaths['system'], ) tuple.
        Edit: Removed aslist argument.
        Instead, control using cfgtype argument: If cfgtype is a list, return a list, otherwise return string.
        """
        if cfgtype == 'all':
            return self.ConfigPaths.values()
        elif not isinstance(cfgtype, string_types):
            return [self.ConfigPaths.get(cfgtype, None) for cfgtype in cfgtype]
        return self.ConfigPaths.get(cfgtype, None)


    def getConfig(self, cfgtype=None):
        """
        Returns the config for a particular config type.
        Five return behaviours:
        0) getConfig('combined') -> returns the combined, effective config for all configs.
        1) getConfig('combined') -> Return 1-element list with the combined, effective config as sole element.
        2) getConfig('system') -> returns the 'system' config.
        3) getConfig('all') -> returns self.Configs.values() list.

        Edit: removed 'aslist' argument. If you want a list of configs, input a list as the cfgtype argument.
        Edit edit: This method can no longer return a list of configs. Use getConfigs (plural) to get more than one.

        # Regarding using ChainMap:
        # - Only native to Python 3.3, not available in python 2.7 (Although it can be added.)
        # - Only provides index access, not keyword, i.e. chainmap[0] or chainmap[-1], not chainmap['system'],
            so not a direct replacement for the current Configs ordereddict.
        # ChainMap Refs
        # - https://docs.python.org/3/library/collections.html#collections.ChainMap
        # - http://code.activestate.com/recipes/305268-chained-map-lookups/
        # - http://stackoverflow.com/questions/23392976/what-is-the-purpose-of-collections-chainmap
        # - http://bugs.python.org/issue11089
        # - http://bugs.python.org/issue11297
        """
        if cfgtype is None or cfgtype == 'combined':
            combined = {}
            for config in self.Configs.values():
                combined.update(config)
        return self.Configs.get(cfgtype, None)

    def getConfigs(self, cfgtypes):
        """
        Returns a list of the specified configs.
        cfgtypes is a list of config types/names.
        If cfgtypes is "all", then all configs are returned.
        """
        if cfgtypes == 'all':
            return self.Configs.values()
        return [self.Configs[cfg] for cfg in cfgtypes]


    def get(self, key, default=None):
        """
        Simulated the get method of a dict.
        Note that the ExpConfigHandler's get() adds a bit more options...
        """
        # This is not usually used, since we almost always use ExpConfigHandler as confighandler.
        return self.getConfig().get(key, default)

    def setdefault(self, key, value=None, autosave=None):
        """
        Mimicks dict.setdefault, will return configentry <key>.
        Use as:
            setdefault('username', 'niceuser')
        If configentry 'username' is not set in any of the loaded configs,
        a new configentry with key 'username' and value 'niceuser' will be
        set, using the default config.
        If a configentry already exists, will simply return that value,
        without setting anything.
        """
        if autosave is None:
            autosave = self.Autosave
        for config in self.Configs.values():
            if key in config:
                return config[key]
        # If key is not found, set default in default config (usually 'user')
        val = self.Configs[self.DefaultConfig].setdefault(key, value)
        self.ChangedEntriesForCallbacks.add(key)
        if autosave:
            self.saveConfig(self.DefaultConfig)
        return val

    def set(self, key, value):
        """
        Alias for setkey (I can never remember that one...).
        However, like a normal dict, set(key, value) will always return None,
        while setkey returns the config-type where the config entry (key) was found.
        """
        self.setkey(key, value)

    def setkey(self, key, value, cfgtype=None, check_for_existing_entry=True, autosave=None):
        """
        Sets a config key.
        If key is already set in one of the main configs, and check_for_existing_entry
        is set to True then update the config where entry is found. (RECOMMENDED)
        If key is not already set, store in config specified by <cfgtype> arg.
        If cfgtype is not provided, use default config (type), e.g. 'user'.

        PLEASE NOTE THAT setkey IS DIFFERENT FROM A NORMAL set METHOD, IN THAT setkey()
        returns the cfgtype where the key was persisted, e.g. 'user'.
        Also, setkey can be provided to arguments cfgtype, check_for_existing_entry and autosave
        to alter the function behaviour.
        """
        if autosave is None:
            autosave = self.Autosave
        if check_for_existing_entry:
            #for cfgtyp, config in self.Configs.items():
            #    if key in config:
            #        config[key] = value
            #        return cfgtyp
            cfgtype = next((cfgtype for cfgtype, config in self.Configs.items()
                            if key in config),
                           self.DefaultConfig)
        else:
            # If key is not found in any of the existing configs, set in default config type:
            if cfgtype is None:
                cfgtype = self.DefaultConfig
        # Set config key to value:
        try:
            self.Configs.get(cfgtype)[key] = value
        except TypeError:
            logger.warning("TypeError when trying to set key '%s' in cfgtype '%s', self.Configs.get('%s') returned: %s, self.Configs.keys(): %s",
                           key, cfgtype, cfgtype, self.Configs.get(cfgtype), self.Configs.keys())
            return False
        self.ChangedEntriesForCallbacks.add(key)
        logger.debug("cfgtype:key=type(value) | %s:%s=%s", cfgtype, key, type(value))
        if autosave:
            logger.debug("Autosaving config: %s", cfgtype)
            self.saveConfig(cfgtype)
        return cfgtype

    def popkey(self, key, cfgtype=None, check_all_configs=False):
        """
        Simulates the dict.pop method; If cfgtype is specified, only tries to pop from that cfgtype.
        If check_all_configs is True, pop from all configs; otherwise stop when the first is reached.
        Returns a tuple of (value, cfgtype[, value, cfgtype, ...]).
        """
        res = ()
        if cfgtype:
            return (self.Configs[cfgtype].pop(key, None), cfgtype)
        for cfgtype, config in self.Configs.items():
            val = config.pop(key, None)
            res = res + (val, cfgtype)
            logger.debug("popped value '%s' from config '%s'. res is now: '%s'", val, cfgtype, res)
            if val and not check_all_configs:
                break
        return res

    def readConfig(self, inputfn, cfgtype='user'):
        """
        Reads a (yaml-based) configuration file from inputfn, loading the
        content into the config given by cfgtype.
        Note: This is a relatively low-level method.
        Generally, you'd want to use addNewConfig().
        """
        VERBOSE = self.VERBOSE
        if cfgtype is None:
            cfgtype = next(iter(self.Configs.keys()))
        if not self.AllowChainToSameType and cfgtype in self.ReadConfigTypes:
            return
        if inputfn in self.ReadFiles:
            logger.warning("WARNING, file already read: %s", inputfn)
            return
        try:
            newconfig = loadConfig(inputfn)
        except IOError as e:
            logger.warning("readConfig() :: ERROR, could not load yaml config, cfgtype: %s, error: %s", cfgtype, e)
            return False
        self.ReadConfigTypes.add(cfgtype)
        self.ReadFiles.add(inputfn) # To avoid recursion...
        self.Configs[cfgtype].update(newconfig)
        logger.info("readConfig() :: New '%s'-type config loaded:", cfgtype)
        if VERBOSE > 3:
            logger.debug("Loaded config is: %s", newconfig)
            logger.debug("readConfig() :: Updated main '%s' config to be: %s", cfgtype, _printConfig(self.Configs[cfgtype]))
        if "next_config_override_fn" in newconfig and self.AllowNextConfigOverrideChain:
            # the next_config_override_fn are read-only, but their content will be persisted to the main configfile.when saved.
            logger.debug("readConfig() :: Reading config defined by next_config_override_fn entry: %s", newconfig["next_config_override_fn"])
            self.readConfig(newconfig["next_config_override_fn"], cfgtype)
        if "config_define_new" in newconfig and self.AllowNewConfigDefinitions:
            # The config_define_new entry can be used to link to one or more other configs that should be loaded.
            for newtype, newconfigfn in newconfig["config_define_new"].items():
                if not os.path.isabs(newconfigfn):
                    # isabs basically just checks if path[0] == '/'...
                    newconfigfn = os.path.normpath(os.path.join(os.path.dirname(inputfn), newconfigfn))
                logger.info("readConfig: Adding config-defined config '%s' using filepath '%s'", newtype, newconfigfn)
                self.addNewConfig(newconfigfn, newtype)

        # Inputting configs through Config_path_entries:
        # This is an alternative to using config_define_new.
        # Where config_define_new can specify an arbitrary nwe config (but requires self.AllowNewConfigDefinitions to be set to True),
        # Config_path_entries {cfgtype: <config_key>} is a pre-defined set of allowed cfgtypes and their corresponding config_keys.
        cfgtypes_linked_in_newconfig = set(newconfig.keys()).intersection(self.Config_path_entries.values())
        if cfgtypes_linked_in_newconfig:
            reversemap = dict((val, key) for key, val in self.Config_path_entries.items())
            for key in cfgtypes_linked_in_newconfig:
                logger.debug("Found the following path_entries key '%s' in the new config: %s", key, newconfig[key])
                self.readConfig(newconfig[key], reversemap[key])
                self.AutoreadNewFnCache[reversemap[key]] = newconfig[key]
        return newconfig


    def autoRead(self):
        """
        autoRead is used to read all config files defined in self.ConfigPaths.
        autoRead and the underlying readConfig() methods uses AutoreadNewFnCache attribute to
        keep track of which configs has been loaded and make sure to avoid cyclic config imports.
        (I.e. avoid the situation where ConfigA says "load ConfigB" and ConfigB says "load ConfigA).
        """
        logger.debug("ConfigPaths: %s", self.ConfigPaths.items())
        for (cfgtype, inputfn) in self.ConfigPaths.items():
            if inputfn:
                logger.debug("Will read config '%s' to current dict: %s", inputfn, cfgtype)
                self.readConfig(inputfn, cfgtype)
                logger.debug("Finished read config '%s' to dict: %s", inputfn, cfgtype)
            logger.debug("Autoreading done, chained with new filenames: %s", self.AutoreadNewFnCache)
        self.ConfigPaths.update(self.AutoreadNewFnCache)
        self.AutoreadNewFnCache.clear()
        logger.debug("Updated ConfigPaths: %s", self.ConfigPaths.items())

    def saveConfigForEntry(self, key):
        """
        Saves the config file that contains a particular entry.
        Useful if you have changed only a single config item and do not want to persist all config files.
        Example: The app changes the value of 'app_active_experiment' and invokes saveConfigForEntry('app_active_experiment')
        Notes:
         * In the example above, the app could also have used the 'autosave' argument when updating the key:
            >>> confighandler.setkey(key, value, autosave=True)
         * For Hierarchical configs, use the path-based save method in ExpConfigHandler.
        """
        for cfgtype, cfg in reversed(list(self.Configs.items())):
            if key in cfg:
                self.saveConfigs(cfgtype=cfgtype)
                return True
        logger.warning("saveConfigForEntry invoked with key '%s', but key not found in any of the loaded configs (%s)!",
                       key, ",".join(self.Configs))

    def saveConfigs(self, cfgtype='all'):
        """
        Persist config specified by cfgtype argument.
        Use as:
            saveConfigs('all') --> save all configs (default)
            saveConfigs('system') --> save the 'system' config. (or use the simpler: saveConfig(cfgtype))
            saveConfigs(('system', 'exp') --> save the 'system' and 'exp' config.
        """
        logger.info("saveConfigs invoked with configtosave '%s'", cfgtype)
        for cfgname, outputfn in self.ConfigPaths.items():
            if cfgtype == 'all' or cfgname in cfgtype or cfgname == cfgtype:
                if outputfn:
                    logger.info("Saving config '%s' to file: %s", cfgname, outputfn)
                    saveConfig(outputfn, self.Configs[cfgname])
                else:
                    logger.info("No filename specified for config '%s'", cfgname)
            else:
                logger.debug("configtosave '%s' not matching cfgtype '%s' with outputfn '%s'", cfgtype, cfgname, outputfn)

    def saveConfig(self, cfgtype):
        """
        Saves a particular config.
        saveConfig('system') --> save the 'system' config.
        """
        if cfgtype not in self.ConfigPaths or cfgtype not in self.Configs:
            logger.warning("cfgtype '%s' not found in self.Configs or self.ConfigPaths, aborting...")
            return False
        config = self.Configs[cfgtype]
        outputfn = self.ConfigPaths[cfgtype]
        if not outputfn:
            logger.warning("Outputfn for configtype '%s' is '%s', ABORTING. ", cfgtype, outputfn)
            return False
        logger.debug("Saving config %s using outputfn %s", cfgtype, outputfn)
        saveConfig(outputfn, config)
        return True


    def printConfigs(self, cfgtypestoprint='all'):
        """
        Pretty print of all configs specified by configstoprint argument.
        Default is 'all' -> print all configs.
        """
        for cfgtype, outputfn in self.ConfigPaths.items():
            if cfgtypestoprint == 'all' or cfgtype in cfgtypestoprint or cfgtype == cfgtypestoprint:
                print(u"\nConfig '{}' in file: {}".format(cfgtype, outputfn))
                print(_printConfig(self.Configs[cfgtype]))
        return "\n".join("\n".join([u"\nConfig '{}' in file: {}".format(cfgtype, outputfn),
                                    _printConfig(self.Configs[cfgtype])])
                         for cfgtype, outputfn in self.ConfigPaths.items()
                         if (cfgtypestoprint == 'all' or cfgtype in cfgtypestoprint or cfgtype == cfgtypestoprint)
                        )


    def getConfigDir(self, cfgtype='user'):
        """
        Returns the directory of a particular configuration (file); defaulting to the 'user' config.
        Valid arguments are: 'system', 'user', 'exp', etc.
        """
        cfgpath = self.getConfigPath(cfgtype)
        if cfgpath:
            return os.path.dirname(self.getConfigPath(cfgtype))
        else:
            logger.info("ConfigDir requested for config '%s', but that is not specified ('%s')", cfgtype, cfgpath)




    ######     ###    ##       ##       ########     ###     ######  ##    ##     ######  ##    ##  ######  ######## ######## ##     ##
   ##    ##   ## ##   ##       ##       ##     ##   ## ##   ##    ## ##   ##     ##    ##  ##  ##  ##    ##    ##    ##       ###   ###
   ##        ##   ##  ##       ##       ##     ##  ##   ##  ##       ##  ##      ##         ####   ##          ##    ##       #### ####
   ##       ##     ## ##       ##       ########  ##     ## ##       #####        ######     ##     ######     ##    ######   ## ### ##
   ##       ######### ##       ##       ##     ## ######### ##       ##  ##            ##    ##          ##    ##    ##       ##     ##
   ##    ## ##     ## ##       ##       ##     ## ##     ## ##    ## ##   ##     ##    ##    ##    ##    ##    ##    ##       ##     ##
    ######  ##     ## ######## ######## ########  ##     ##  ######  ##    ##     ######     ##     ######     ##    ######## ##     ##


    def registerEntryChangeCallback(self, configentry, function, args=None, kwargs=None, pass_newvalue_as=False):
        """
        Registers a callback for a particular config entry (key).

        If a callback sets pass_newvalue_as=<key>, this will cause the new config value to be passed to the
        callback in the kwargs, as:
            kwargs['pass_newvalue_as'] = new_configentry_value

        Use case:
          - objectA displays a list of ActiveExperimentIds, which is saved as the app_active_experiments config entry (key).
            and would like to be informed if app_activeexperimentids changes.
            Specifically, it would like to be invoked as objectA.activeExpidsChanged(updatedlist=<new list of activeexpids>)
            It thus registers as:
                confighandler.registerEntryChangeCallback('app_active_experiments', self.activeExpidsChanged, pass_newvalue_as='updatedlist')

          - Now, when objectB has changes app_active_experiments (e.g. indirectly by .append(<new expid>)-ing a new value to the list),
            objectB can invoke
                confighandler.invokeEntryChangeCallbacks('app_active_experiments', new_configentry_value=<new expids list>),
            which will call
                objectA.activeExpidsChanged(updatedlist=<new expids list>)

            Note that there is currently no guarantee that whoever calls
                invokeEntryChangeCallback(self, configentry=None, new_configentry_value=None)
            will actually set the new_configentry_value kwarg. I might add a 'if-set', option,
            but since None is also commonly used as a 'not specified' value for kwargs, I think it is ok.

        Currently, a callback is NOT invoked immediately when a config key is changed
        with e.g. setkey(cfgkey, value). Rather, the key is added to the ChangedEntriesForCallbacks list.
        It is then up to the modifying object to call invokeEntryChangeCallback() when complete.
        I have kept it that way for two reasons: Firstly so that multiple configentries can be
        modified before invoking callbacks. Secondly, to remind the user that invokeEntryChangeCallback()
        is not called automatically.
        It is vital to remember this, since many (most?) config entries are not modified directly,
        but rather indirectly as lists or dicts (e.g. ActiveExperimentIds, as well as all experiment-related stuff).


        ########################
        ## THOUGHTS AND NOTES: #
        ########################

        Note: I see no reason to add a 'registerConfigChangeCallback' method.
        Although this could provide per-config callbacks (e.g. an experiment that could subscribe to
        changes only for that experiment), I think it is better to code for this situation directly.

        Note that I am CURRENTLY CONSIDERING A NEW ARGUMENT, pass_newvalue_as_first_arg.
        If this is set to True, the new config value will be passed as the first argument:
            function(new_configentry_value, *args, **kwargs)

        Note that changes are not registrered automatically. It is really not possible to see if
        entries changes, e.g. dicts and lists which are mutable from outside the control of this confighandler.
        Instead, this is a curtesy service, that allows one user of the confighandler to inform
        the other objects subscribed with callbacks that something has changed.
        Use as:
            objB -> registers updateListWidget callable with 'app_active_experiments' using this method.
            objA -> adds an entry to ch.get('app_active_experiments')
            objA -> invokes invokeEntryChangeCallback('app_active_experiments')
            ch   -> calls updateListWidget.
        Alternative scheme:
            objB -> registers updateListWidget callable with 'app_active_experiments' using this method.
            objA -> adds an entry to ch.get('app_active_experiments')
            objA -> does ch.ChangedEntriesForCallbacks.add('app_active_experiments')
            < something else happens>
            objC -> figues it might be a good idea to call ch.invokeEntryChangeCallback() with no args.
            ch   -> searches through the ChangedEntriesForCallbacks set for changes since last callback.
            ch   -> calls updateListWidget.

        ## NOTE REGARDING IMPLEMENTAITON OF A GENERARAL-PURPOSE CALLBACK SYSTEM ##
        This can be used as a simple, general-purpose callback manager, across all objects
        that have access to the Confighandler singleton. The 'configentry' key does not have to
        correspond to an actual configentry, it can just be a name that specifies that particular
        callback by convention.
        Of cause, this is not quite as powerfull as using qt's QObject and signal connections and
        emitting, but is is ok for simple callbacks, especially for singleton-like objects and variables.
        (see http://pyqt.sourceforge.net/Docs/PyQt4/qobject.html for more info on QObject's abilities.)

        Edit: If you want a general-purpose callback system, this should probably NOT be crammed
        into the config handler.

        Instead, if you really want to implement a callback system in the model domain (which by the way
        sounds dangerously complex and in violation with MVC patterns) it might be better to
        implement a callback observer pattern as a generic mixin class (or class decorator?).

        (The callback system as I use it here already shares some similar traits with Kivy's callback system
        used to bind different widgets via the setter(<key>) --> key_setter_method )

        This could perhaps be done with a property-like decorator, where the setter checks if the new
        value is different from the old, and if it is, it (the setter method) looks in e.g.
            instance._propertiesCallbacks[self.__name__]
        for registrered callbacks -- which can now be called similarly to the system here in confighandler,
        but where the new value can actually be guaranteed to be passed.

        This could like something like:
            objA    registers   em.bind(ActiveExperimentIds, objA.activeExpidsChanged, pass_newvalue_as='updatedlist')
            objB    changes     em.ActiveExperimentIds
            em      calls       all callbacks registered for ActiveExperimentIds.

        However, this still would not call the callbacks if changed via em.ActiveExperimentIds.append('new expid')
        If you really want this, you would need to use a callback/event-aware list object,
        which would be able to invoke callbacks when modified. However, that really seems out of scope -- to modify
        basic list objects... It could be done by em making a hash whenever it returned the list, and
        then the next time it would return the list, it could check if the hash had changed.
        That would induce a "one-call delay", but might ensure that the callback was "eventually" called.

        One conclusion might be that you should completely stay away from implementing an in-model callback system.

        Another might be that you should *not* try to roll your own, but instead sub-class your model and
        implement a GUI-dependent callback system.
        For instance, Qt has a QList object, and Kivy has a SimpleListAdaptor, both of which might be used
        to add a callback system.

        Is it needed? Well, probably not. It is mostly because I find it more convenient to work in the model domain,
        and then have the UI update itself automatically to reflect changes to the model data.
        There are plenty of examples where data is updated in the model domain, without it being immediately
        obvious which UI widgets to update.
        For instance, if an experiment attaches a wiki page (because it is needed somewhere else),
        this could trigger a mergeSubentriesFromWikiPage() which may update the subentries of an experiment
        This is all done in the model domain, and the UI widget that caused the wiki-page to be attached in
        the first place is likely not aware about anything regarding subentries or the UI widgets that display
        subentries.

        This seems a good example of why I would want an in-model callback system. And again, this could be
        handled with a callback system as described above. If implemented in the confighandler, you could
        just register callbacks for "ad-hoc config-keys", e.g. "experiment-RS189.subentries", and
        have the experiment call
            confighandler.invokeEntryChangeCallbacks("experiment-RS189.subentries", self.Subentries)
        However, it does seem more elegant to have the callback registered in the experiment object,
        which after running mergeSubentriesFromWikiPage() would call either of
            self.invokeCallbacksForChangedProperty()    # and look for all changed properties flagged by mergeSubentriesFromWikiPage,
                                                        # similar to how ChangedEntriesForCallbacks is used by confighandler
            self.invokeChangedPropertyCallbacks('Subentries')   # only invoke callbacks for the 'Subentries' property.
            self.invokePropertyCallbacks('Subentries')

        And honestly, this is essentially what I already have here in the confighandler, so could easily
        be implemented with a mixin class / class decorator, plus possibly also a custom callback_property decorator,
        where the setter would automatically call registered callbacks (or, alternatively, just flag the property
        in the ChangedPropertiesForCallbacks list -- this could be customized similarly to how the ttl is customized
        for the cached_property decorator...)

        But maybe check out how Kivy implements its bind() method.

        """
        if args is None:
            args = list()
        elif not isinstance(args, collections.Iterable):
            logger.debug("registerEntryChangeCallback received 'args' argument with non-iterable value '%s', will convert to tuple.", args)
            args = (args, )
        if kwargs is None:
            kwargs = dict()
        # I would have liked this to be a set, but hard to implement set for dict-type kwargs and no frozendict in python2.
        # Just make sure not to register the same callback twice.
        self.EntryChangeCallbacks.setdefault(configentry, list()).append((function, args, kwargs, pass_newvalue_as))
        logger.debug("Registrered callback for configentry '%s': %s(*%s, **%s) with pass_newvalue_as=%s", configentry, function, args, kwargs, pass_newvalue_as)
        # I could also have implemented as dict based on the function is hashable, e.g.:
        #self.EntryChangeCallbacks.setdefault(configentry, dict()).set(function, (args, kwargs) )
        # and invoke with:
        # for function, (args, kwargs) in self.EntryChangeCallbacks[configentry].items():
        #     function(*args, **kwargs)

    def unregisterEntryChangeCallback(self, configentries=None, function=None, args=None, kwargs=None):
        """
        Notice that a function may be registered multiple times.
        self.EntryChangeCallbacks[configentry] = list of (function, args, kwargs) tuples.

        The unregister call is powerful and generic: callbacks can be removed based not only on the function,
        but also on the arguments passed to the function as well as the configentries.
        This means that, for instance, all callbacks that receives the keyword arguments {'hello': 'there'}
        can be removed by calling:
            unregisterEntryChangeCallback(configentries=None, function=None, args=None, kwargs={'hello': 'there'} )
        This is because all callbacks satisfying the filter:
            all( criteria in (None, callbacktuple[i]) for i, criteria in enumerate( (function, args, kwargs) ) )
        will be removed.
        Thus, if unregisterEntryChangeCallback() is called without arguments,
        ALL REGISTRERED CALLBACKS WILL BE REMOVED!
        """
        if all(a is None for a in (function, args, kwargs)):
            if configentries is None:
                logger.warning("NOTICE: unregisterEntryChangeCallback called without any arguments. All registrered callbacks will be removed.")
            else:
                logger.info("Removing all registrered callbacks for configentries '%s' - since unregisterEntryChangeCallback was called with configentries as only argument.", configentries)
        if configentries is None:
            configentries = self.EntryChangeCallbacks.keys()
        elif isinstance(configentries, string_types):
            configentries = (configentries, )

        for configentry in configentries:
            #removelist = filter(callbackfilter, self.EntryChangeCallbacks[configentry])
            # Changed, now using generator alternative instead of filter builtin (which is in bad
            # standing with the BDFL, http://www.artima.com/weblogs/viewpost.jsp?thread=98196)
            removelist = (callbacktuple for callbacktuple in self.EntryChangeCallbacks[configentry]
                          if all(criteria in (None, callbacktuple[i])
                                 for i, criteria in enumerate((function, args, kwargs)))
                         )
            logger.debug("Removing callbacks from self.EntryChangeCallbacks[%s]: %s", configentry, removelist)
            for callbacktuple in removelist:
                self.EntryChangeCallbacks[configentry].remove(callbacktuple)


    def invokeEntryChangeCallback(self, configentry=None, new_configentry_value=None):
        """
        Simple invokation of registrered callbacks.
        If configentry is provided, only callbacks registrered to that entry will be invoked.
        If configentry is None (default), all keys registrered in self.ChangedEntriesForCallbacks
        will have their corresponding callbacks invoked.
        When a configentry has had its callbacks invoked, it will be unregistrered from
        self.ChangedEntriesForCallbacks.

        ## Done: implement try clause in confighandler.invokeEntryChangeCallback and
        ## automatically unregister failing calls.
        ## Done: Implement ability to route the newvalue parameter to the callbacks.
        ##       As it is now, each of the callbacks have to invoke self.Confighandler.get(configkey)
        ## -fix: The new value is passed to callback as keyword argument 'pass_newvalue_as'.
        ##       The new value can also be injected by setting new_configentry_value
        ##       as kwargument when invoking this method (invokeEntryChangeCallback)
        ##
        """
        if configentry:
            if configentry in self.EntryChangeCallbacks:
                failedfunctions = list()
                for function, args, kwargs, pass_newvalue_as in self.EntryChangeCallbacks[configentry]:
                    if pass_newvalue_as:
                        kwargs[pass_newvalue_as] = new_configentry_value
                    logger.debug("invoking callback for configentry '%s': %s(*%s, **%s)", configentry, function, args, kwargs)
                    try:
                        function(*args, **kwargs)
                    except TclError as e:
                        logger.error("Error while invoking callback for configentry '%s': %s(*%s, **%s): %s",
                                     configentry, function, args, kwargs, e)
                        logger.info("Marking callback as failed: '%s': %s(*%s, **%s)", configentry, function, args, kwargs)
                        failedfunctions.append(function)
                for function in failedfunctions:
                    logger.info("Unregistrering callbacks for function: %s(...)", function)
                    self.unregisterEntryChangeCallback(function=function)
                # Erase this entry if registrered here. (discard does not do anything if the item is not a member of the set)
                self.ChangedEntriesForCallbacks.discard(configentry)
            else:
                logger.debug("invokeEntryChangeCallback called with configentry '%s', but no callbacks are registrered for that entry...", configentry)
        elif self.ChangedEntriesForCallbacks:
            # The ChangedEntriesForCallbacks will change during iteration, so using a while rather than for loop:
            while True:
                try:
                    entry = self.ChangedEntriesForCallbacks.pop() # Raises KeyError when dict is empty.
                    logger.debug("Popped configentry '%s' from ChangedEntriesForCallbacks...", configentry)
                    self.invokeEntryChangeCallback(entry)
                except KeyError: # raised when pop on empty set.
                    break



class ExpConfigHandler(ConfigHandler):
    """
    ExpConfigHandler adds four functionalities:
    1)  It enables a default 'exp' config, specifying an 'exp_config_path' config key,
        which specifies an 'exp' config file to be read.
    2)  It implements "Hierarchical" path-based configurations,
        by relaying many "path augmented" calls to a HierarchicalConfigHandler object.
        This makes it possible to have different configs for different experiment folders,
        i.e. for different years or for different projects or different experiments.
        In other words, if calling get(key=<a config key>, path=<dir>) with a <dir> value of
        '2013/ProjectB/ExpAB123 Important experiment v11', then the search path for a config with key <a config key>
        will be:
        1) '2013/ProjectB/ExpAB123 Important experiment v11/.labfluence.yml'
        2) '2013/ProjectB/.labfluence.yml'
        3) '2013/.labfluence.yml'
        4) Search the already loaded config types in order, e.g. 4.1) 'exp', 4.2) 'user', 4.3) 'system'.
    3)  Relative experiment paths will be returned as absolute paths, i.e. for the config keys:
        if local_exp_rootDir = '2013' --> return os.path.join(exp_path, cfg[key]
        and equivalent for local_exp_subDir and local_exp_ignoreDirs (which is a list of paths).
    4)  It employs a PathFinder to automatically locate config paths by searching local directories,
        according to a specified path scheme.
        The default path scheme, 'default1', will e.g. search for the user config in '~/.Labfluence/',
        while the path scheme 'test1' will search for both 'system' and 'user' configs
        in the relative directory 'setup/configs/test_configs/local_test_setup_1'.

    """
    def __init__(self, systemconfigfn=None, userconfigfn=None, expconfigfn=None,
                 readfiles=True, pathscheme='default1', hierarchy_rootdir_config_key='local_exp_rootDir',
                 enableHierarchy=True):
        self.Pathfinder = PathFinder()
        pschemedict = self.Pathfinder.getScheme(pathscheme) if pathscheme else dict()
        systemconfigfn = systemconfigfn or pschemedict.get('sys')
        userconfigfn = userconfigfn or pschemedict.get('user')
        expconfigfn = expconfigfn or pschemedict.get('exp')
        logger.debug("Pathfinder located system, user and exp configs: %s, %s, %s", systemconfigfn, userconfigfn, expconfigfn)
        if systemconfigfn and os.path.normpath('setup/configs/default') in systemconfigfn:
            print("\nWARNING: Pathfinder picked up config in deprechated location 'setup/configs/default/' -- PLEASE MOVE/COPY THE CONFIG FROM HERE TO <install-dir>/config/ !\n")
        # init super:
        ConfigHandler.__init__(self, systemconfigfn, userconfigfn)
        self.Configs['exp'] = dict()
        self.ConfigPaths['exp'] = expconfigfn
        self.Config_path_entries['exp'] = "exp_config_path"
        if readfiles:
            logger.debug("__init()__ :: autoreading...")
            self.autoRead()
        if enableHierarchy and hierarchy_rootdir_config_key:
            rootdir = self.get(hierarchy_rootdir_config_key)
            ignoredirs = self.get('local_exp_ignoreDirs')
            logger.debug("Enabling HierarchicalConfigHandler with rootdir: %s", rootdir)
            if rootdir:
                self.HierarchicalConfigHandler = HierarchicalConfigHandler(rootdir, ignoredirs, parent=self)
            else:
                logger.info("rootdir is %s; hierarchy_rootdir_config_key is %s; configs are (configpaths): %s",
                            rootdir, hierarchy_rootdir_config_key, self.ConfigPaths)

        else:
            self.HierarchicalConfigHandler = None
        logger.debug("ConfigPaths : %s", self.ConfigPaths)


    def getHierarchicalEntry(self, key, path, traverseup=True):
        """
        Much like self.get, but only searches the HierarchicalConfigHandler configs.
        This is useful if you need to retrieve options that must be defined at the path-level,
        e.g. an exp_pageId or exp_id.
        If traverseup is set to True, then the HierarchicalConfigHandler is allowed to return a
        config value from a config in a parent directory if none is found in the first looked directory.
        """
        return self.HierarchicalConfigHandler.getEntry(key, path, traverseup=traverseup)


    def getHierarchicalConfig(self, path, rootdir=None):
        """
        Returns a hierarchically-determined config, based on a path and rootdir.
        Relays to HierarchicalConfigHandler.getHierarchicalConfig
        """
        return self.HierarchicalConfigHandler.getHierarchicalConfig(path, rootdir=rootdir)


    def get(self, key, default=None, path=None):
        """
        Simulated the get method of a dict.
        If path is provided, will search HierarchicalConfigHandler for a matching config before
        resolving to the 'main' configs.
        """
        if path and self.HierarchicalConfigHandler:
            val = self.getHierarchicalEntry(key, path)
            # perhaps raise a KeyError if key is not found in the hierarchical confighandler;
            # None could be a valid value in some cases? But get*(key) immitates dict.get, and
            # dict.get do not raise KeyError. Implement __getitem__ if you want something that raises KeyError.
            if val is not None:
                return val
        # Optimized, and accounting for the fact that later added cfgs overrides the first added
        for cfg in reversed(list(self.Configs.values())):
            if key in cfg:
                return cfg[key]
        return default


    def getAbsExpPath(self, pathkey):
        """
        Returns an absolute path for paths defined relatively to the experiment root.
        Used for paths in the config that might be specified relatively to
        the root of the experiment directory tree.
        """
        path = self.get(pathkey)
        if os.path.isabs(path):
            return path
        if pathkey == 'local_exp_rootDir':
            # Return root dir path relative to the directory containing the experiment config file.
            # (This is the best guess, if local_exp_rootDir is not absolute...)
            return os.path.normpath(os.path.join(self.getConfigDir('exp'), path))
        rootdir = self.getAbsExpPath('local_exp_rootDir')
        if isinstance(path, string_types):
            return os.path.normpath(os.path.join(rootdir, path))
        elif isinstance(path, (list, tuple)):
            # Case 2, config keys specifying a list of paths:
            return [os.path.join(rootdir, elem) for elem in path]

    def getExpConfig(self, path):
        """
        Returns a hierarchically determined experiment config.
        Similar to getConfig, but will not fall back to use the standard
        (non-hierarchical) configs.
        """
        return self.HierarchicalConfigHandler.getConfig(path)

    def loadExpConfig(self, path, doloadparent=None, update=None):
        """
        Relay to self.HierarchicalConfigHandler.loadConfig(path)
        """
        return self.HierarchicalConfigHandler.loadConfig(path, doloadparent, update)

    def saveExpConfig(self, path, cfg=None):
        """
        Relay to self.HierarchicalConfigHandler.saveConfig(path)
        """
        logger.debug("invoked with path=%s, cfg=%s", path, cfg)
        keysupdatedfromfile, keysupdatedinmemory, changedkeys = self.HierarchicalConfigHandler.saveConfig(path, cfg)
        logger.info("self.HierarchicalConfigHandler.saveConfig(%s, %s) returned with tuple: (%s, %s, %s)",
                    path, cfg, keysupdatedfromfile, keysupdatedinmemory, changedkeys)
        self.invokeEntryChangeCallback(path, keysupdatedfromfile)
        logger.info("Invoking self.invokeEntryChangeCallback(%s, %s)", path, keysupdatedfromfile)
        return keysupdatedfromfile, keysupdatedinmemory, changedkeys

    def updateAndPersist(self, path, props=None, update=False):
        """
        If props are given, will update config with these.
        If update is a string, loadExpConfig is called before saving, forwarding update, where:
        - False = do not update, just load the config overriding config in memory if present.
        - 'timestamp' = use lastsaved timestamp to determine which config is main.
        - 'file' = file is updated using memory.
        - 'memory' = memory is updated using file.

        TODO: This should be consolidated with the check_config_and_merge function,
        so that you can inform config subscribers if one or more config keys is being
        updated based on the the config from file.
        """
        exps = self.HierarchicalConfigHandler.Configs
        cfg = exps.setdefault(path, dict())
        if props:
            cfg.update(props)
        if update:
            self.loadExpConfig(path, doloadparent='never', update=update)
        self.saveExpConfig(path)

    def renameConfigKey(self, oldpath, newpath):
        """
        There is probably not a need to do this for the 'system', 'user', 'exp' dicts;
        only the experiments managed by HierarchicalConfigHandler (i.e. after renaming a folder)
        """
        self.HierarchicalConfigHandler.renameConfigKey(oldpath, newpath)



class HierarchicalConfigHandler(object):
    r"""
    The point of this handler is to provide the ability of having individual configs in different
    branches of the directory tree.
    E.g., the main config might have
        exp_subentry_regex: (?P<exp_id>RS[0-9]{3})-?(?P<subentry_idx>[\ ]) (?P<subentry_titledesc>.*) \((?P<subentry_date>[0-9]{8})\)
    but in the directory 2012_Aarhus, you might want to use the regex:
        exp_subentry_regex: (?P<subentry_date>[0-9]{8}) (?P<exp_id>RS[0-9]{3})-?(?P<subentry_idx>[\ ]) (?P<subentry_titledesc>.*)

    How to implement/use?
    - As an object; use from parent object.     *currently selected*
    - As a "mixin" class, making methods available to parent.
    - As a parent, deriving from e.g. ExpConfigHandler
    - As a wrapper; instantiates its own ConfigHandler object.

    Notice that I originally intended to always automatically load the hierarchy;
    however, it is probably better to do this dynamically/on request, to speed up startup time.

    """
    def __init__(self, rootdir, ignoredirs=None, parent=None, doautoloadroothierarchy=False, VERBOSE=0):
        self.VERBOSE = VERBOSE
        self._parent = parent
        self.Configs = dict() # dict[path] --> yaml config
        self.ConfigSearchFn = '.labfluence.yml'
        self.Rootdir = rootdir
        if ignoredirs is None:
            ignoredirs = list()
        self.Ignoredirs = ignoredirs or [] # Using list, because set is not a yaml native.
        if doautoloadroothierarchy:
            self.loadRootHierarchy()

    def printConfigs(self):
        """
        Make a pretty string representation of the loaded configs for e.g. debugging.
        """
        return "\n".join(u"{} -> {}".format(path, cfg) for path, cfg in sorted(self.Configs.items()))

    def loadRootHierarchy(self, rootdir=None, clear=False):
        """
        Load all labfluence config/metadata files in the directory hierarchy using self.Rootdir as base.
        """
        if clear:
            self.Configs.clear()
        if rootdir is None:
            rootdir = self.Rootdir
        if self.VERBOSE or True:
            logger.debug("Searching for %s from rootdir %s; ignoredirs are: %s", self.ConfigSearchFn, rootdir, self.Ignoredirs)
        for dirpath, dirnames, filenames in os.walk(rootdir):
            if dirpath in self.Ignoredirs:
                del dirnames[:] # Avoid walking into child dirs. Do not use dirnames=list(), as os.walk would then still refer to the old list.
                logger.debug("Ignoring dir (incl children): %s", dirpath)
                continue
            if self.VERBOSE > 3:
                logger.debug("Searching for %s in %s", self.ConfigSearchFn, dirpath)
            if self.ConfigSearchFn in filenames:
                self.loadConfig(dirpath)


    def getConfig(self, path):
        """
        Implemented dynamic read; will try to load if config if not already loaded.
        """
        if path not in self.Configs:
            return self.loadConfig(path)
        else:
            return self.Configs[path]


    def getConfigFileAndDirPath(self, path):
        """
        returns dpath and fpath, where
        fpath = full path to config file
        dpath = directory in which config file resides.
        Always use dpath when searching for a config.
        """
        if not os.path.isabs(path):
            logger.debug("Edit, this should probably be concatenated using the exp-data-path;"+\
                         "doing this will use the current working directory as base path...")
            if self.Rootdir:
                path = os.path.realpath(os.path.join(self.Rootdir, path))
            else:
                path = os.path.abspath(path)
        if os.path.islink(path):
            path = os.path.realpath(path)
        if os.path.isdir(path):
            dpath = path
            fpath = os.path.join(path, self.ConfigSearchFn)
        elif os.path.isfile(path):
            fpath = path
            dpath = os.path.dirname(path)
        else:
            logger.error("Critical warning: Confighandler.getConfigFileAndDirPath() :: Could not find path: '%s'", path)
            raise ValueError(u"Confighandler.getConfigFileAndDirPath() :: Could not find path:\n{}".format(path))
        return dpath, fpath


    def loadConfig(self, path, doloadparent=None, update=None):
        """
        update can be either of False, 'file', 'memory', 'timestamp', where
        - False = do not update, just load the config overriding config in memory if present.
        - 'timestamp' = use lastsaved timestamp to determine which config is main.
        - 'file' = file is updated using memory.
        - 'memory' = memory is updated using file.
        The doloadparent can be either of 'never', 'new', or 'reload', where
        - 'never' means never try to load parent directory config,
        - 'new' means try to load parent config from file if not already loaded; and
        - 'reload' means always try load to parent directory config from file.
        """
        dpath, fpath = self.getConfigFileAndDirPath(path)
        if doloadparent is None:
            doloadparent = 'new'
        if update is None:
            update = 'file'
        try:
            #cfg = yaml.load(open(fpath))
            cfg = loadConfig(fpath)
            if update and dpath in self.Configs:
                if update == 'file':
                    cfg.update(self.Configs[dpath])
                    self.Configs[dpath] = cfg
                elif update == 'memory':
                    self.Configs[dpath].update(cfg)
            else:
                self.Configs[dpath] = cfg
        except IOError as e:
            if self.VERBOSE:
                logger.warning("HierarchicalConfigHandler.loadConfig() :: Could not open path '%s'. Error is: %s", path, e)
            if os.path.exists(fpath):
                logger.error("""HierarchicalConfigHandler.loadConfig() :: Critical WARNING -> Could not open path '%s',
                             but it does exists (maybe directory or broken link);
                             I cannot just create a new config then.""", path)
                raise IOError(e)
            cfg = self.Configs[dpath] = dict() # Best thing is probably to create a new dict then...
        parentdirpath = os.path.dirname(dpath)
        if (doloadparent == 'new' and parentdirpath not in self.Configs) or doloadparent == 'reload':
            self.loadConfig(parentdirpath, doloadparent='never')
        return cfg


    def saveConfig(self, path, cfg=None):
        """
        Save config <cfg> to path <path>.
        If cfg is not given, the method will check if a config from <path> was
        already loaded. In that case, that config will be saved to path.
        docheck argument can be used to force checking that the file provided by
        <path> has not been updated.
        Returns True if successful and False otherwise.
        """
        dpath, fpath = self.getConfigFileAndDirPath(path)
        # optionally perform a check to see if the config was changed since it was last saved...?
        # either using an external file, a timestampt, or something else...
        logger.debug("saveConfig invoked with path '%s' and type(cfg)=%s", path, cfg)
        if cfg is None:
            if path in self.Configs:
                cfg = self.Configs[path]
            elif dpath in self.Configs:
                cfg = self.Configs[dpath]
            else:
                logger.warning("HierarchicalConfigHandler.saveConfig() :: Error, no config found to save for path '%s'", fpath)
                return None, None, None
        #if docheck:
            #fileconfig = yaml.load(cfg, open(fpath))
            #if fileconfig.get('lastsaved'):
            #    if not cfg.get('lastsaved') or cfg.get('lastsaved') < fileconfig['lastsaved']:
            #        logger.warning("Attempted to save config to path '%s', but checking the existing file\
            #                       reveiled that the existing config has been updated (from another location). Aborting...")
            #        return False

        # EDIT: It is no longer possible to 'skip' the check, but you can
        # make sure that cfg will override, by setting cfg['lastsaved'] = datetime.now() - although that is a hack.
        try:
            cfgfromfile = loadConfig(fpath)
        except IOError as e:
            logger.debug("Could not load file '%s', it probably doesn't exists yet (will be the case for all newly created experiments): %s", fpath, e)
            keysupdatedfromfile, keysupdatedinmemory, changedkeys = None, None, None
        else:
            keysupdatedfromfile, keysupdatedinmemory, changedkeys = check_cfgs_and_merge(cfg, cfgfromfile) # This will merge cfg with the one from file...
        #if keysupdatedfromfile:
        #    if self._parent:
        #        self._parent.invokeEntryChangeCallback(path, keysupdatedfromfile)

        #cfg['lastsaved'] = datetime.now() # This is now added by saveConfig()
        res = saveConfig(fpath, cfg, updatelastsaved=True)
        logger.debug("%s :: saveConfig(%s, <cfg>) returned: '%s'", self.__class__.__name__, fpath, res)
        return keysupdatedfromfile, keysupdatedinmemory, changedkeys



    def renameConfigKey(self, oldpath, newpath):
        """
        Note: This only works for regular dicts;
        for OrderedDict you probably need to rebuild...
        """
        self.Configs[newpath] = self.Configs.pop(oldpath)




    def getEntry(self, key, path, traverseup=True, default=None, doload='new'):
        """
        If traverseup is set to True, then the HierarchicalConfigHandler is allowed to return a
        config value from a config in a parent directory if none is found in the first looked directory.
        doload can be either of 'never', 'new', or 'reload', where
        - 'never' means never try to load from file;
        - 'new' means try to load from file if not already loaded; and
        - 'reload' means always try to load from file.
        """
        if not traverseup:
            if doload in ('new', 'never'):
                if path in self.Configs:
                    return self.Configs[path].get(key)
            if doload == ('reload', 'new'):
                cfg = self.loadConfig(path)
                return cfg.get(key, default) if cfg else default
            elif doload == 'never': # and we have already loaded above...
                return default
        # end if not traverseup; begin traverseup case:
        for cand_path in getPathParents(path, topfirst=False):
            if cand_path in self.Configs and key in self.Configs[cand_path]:
                return self.Configs[cand_path][key]


    def getHierarchicalConfig(self, path, rootdir=None, traverseup=True, default=None):
        """
        Returns a config in the directory hierarchy, starting with path.
        If rootdir is not given, self.Rootdir is used.
        If traverseup is True (default), the search will progress upwards from path
        until a config file is found or the rootdir is reached.
        Will return default if no config were found.
        """
        if not traverseup:
            if path in self.Configs:
                return self.Configs[path]
            else:
                return default
        if rootdir is None:
            rootdir = self.Rootdir
        cfg = dict()
        _, path = os.path.splitdrive(os.path.normpath(path))
        # other alternative, with iterator and os.path.dirname
        def getparents(path):
            """
            Using the generator implementation defined in getPathParents(..., version=1)
            """
            _, path = os.path.splitdrive(path)
            while True:
                logger.debug("yielding path %s", path)
                yield path
                parent = os.path.dirname(path)
                if parent == path or path == rootdir:
                    break
                path = parent
        paths = list(getparents(path))
        for p in reversed(paths):
            if p in self.Configs:
                cfg.update(self.Configs[p])
        return cfg



class PathFinder(object):
    """
    Class used to find config files.
    Takes a 'defaultscheme' argument in init.
    This can be used to easily change the behavior of the object.
    I.e., if 'defaultscheme' is st to 'default1', then the following search paths are used:
    - sys config: '.', './config/', './setup/default/', '..', '../config' (all relative to the current working directory)
    - user config: '~/.Labfluence'.
    - exp config: path should be defined in the user config.
    If defaultscheme='test1':
    - sys config: setup/configs/test_configs/local_test_setup_1
    """
    def __init__(self, defaultscheme='default1', npathsdefault=3, VERBOSE=0):
        self.VERBOSE = VERBOSE
        self.Schemes = dict()
        self.Schemedicts = dict()
        self.Defaultscheme = defaultscheme
        self.Npathsdefault = npathsdefault
        # defautl1 scheme: sysconfig in 'config' folder in current dir;
        self._schemeSearch = dict()
        # notation is:
        # configtype : (<filename to look for>, (list of directories to look in))
        self._schemeSearch['default1'] = dict(sys=('labfluence_sys.yml',
                                                   (os.path.join(APPDIR, subdir) for subdir in
                                                    ('.', 'config', 'configs', os.path.join('setup', 'configs', 'default')))),
                                              user=('labfluence_user.yml',
                                                    (os.path.expanduser(os.path.join('~', dir)) for dir in
                                                     ('.labfluence', '.Labfluence', os.path.join('.config', '.labfluence')))
                                                   )
                                             )
        self._schemeSearch['test1'] = dict(sys=('labfluence_sys.yml', (os.path.join(APPDIR, 'setup/configs/test_configs/local_test_setup_1'),)),
                                           user=('labfluence_user.yml', (os.path.join(APPDIR, 'setup/configs/test_configs/local_test_setup_1'),))
                                          )

        self._schemeSearch['install'] = dict(sys=('labfluence_sys.yml', (os.path.join(APPDIR, 'setup/configs/new_install/'),)),
                                             user=('labfluence_user.yml', (os.path.join(APPDIR, 'setup/configs/new_install/'),)),
                                             exp=('labfluence_exp.yml', (os.path.join('setup/configs/new_install/'),))
                                            )

        #self.mkschemedict() # I've adjusted the getScheme() method so that this will happen on-request.
        logger.debug("%s initialized, self.Defaultscheme='%s'", self.__class__.__name__, self.Defaultscheme)

    def mkschemedict(self):
        """
        This will find all configs for all schemes defined in _schemeSearch.
        It might be a bit overkill to do this at every app start.
        Could be optimized so you only find the config filepaths when a particular scheme is requested.
        """
        for scheme, schemesearch in self._schemeSearch.items():
            self.Schemedicts[scheme] = dict((cfgtype, self.findPath(filename, dircands)) for cfgtype, (filename, dircands) in schemesearch.items())

    def getScheme(self, scheme=None, update=True):
        """
        I have updated this method, so instead of invoking mkschemedict() which will find configs for *all* schemes defined in _schemeSearch,
        it will only make a scheme for the specified scheme.
        Note that this is designed to fail with a KeyError if scheme is not specified in self._schemeSearch.
        """
        if scheme is None:
            scheme = self.Defaultscheme
        if update:
            self.Schemedicts[scheme] = dict((cfgtype, self.findPath(filename, dircands)) for cfgtype, (filename, dircands) in self._schemeSearch[scheme].items())
            logger.debug("PathFinder.Schemedicts updated to: %s", self.Schemedicts)
        logger.debug("PathFinder.getScheme('%s', update=%s) returns path scheme: %s", scheme, update, self.Schemedicts[scheme])
        return self.Schemedicts[scheme]

    def getSchemedict(self, scheme):
        """
        return self.Schemedicts.get(scheme, dict())
        """
        return self.Schemedicts.get(scheme, dict())

    def findPath(self, filename, dircands):
        """
        Given an ordered list of candidate directories, return the first directory
        in which filename is found. If filename is not present in either
        of the directory candidates, return None.
        Changes: replaced for-loops with a sequence of generators.
        """

        okdirs = (dircand for dircand in dircands if os.path.isdir(dircand))
        normdirs = (os.path.normpath(dircand) for dircand in okdirs)
        dirswithfilename = (dircand for dircand in normdirs if filename in os.listdir(dircand))
        firstdir = next(dirswithfilename, None)
        if firstdir:
            winnerpath = os.path.join(firstdir, filename)
            logger.debug("%s, config file found: %s", self.__class__.__name__, winnerpath)
            return winnerpath
        else:
            logger.debug("Warning, no config found for config filename: '%s'; tested: %s", filename, dircands)

    def printSchemes(self):
        """
        Returns a pretty string representation of all schemes in self.Schemedicts .
        """
        ret = "\n".join(u"scheme '{}': {}".format(scheme, ", ".join(u"{}='{}'".format(k, v) \
                    for k, v in schemedict.items())) \
                    for scheme, schemedict in self.Schemedicts.items())
        return ret
