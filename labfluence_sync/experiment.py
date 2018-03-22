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
# pylint: disable-msg=C0103,C0302,R0902,R0904,W0142
# messages:
#   C0103: Invalid attribute/variable name (too short/long/etc?)
#   C0111: Missing method docstring (pylint insists on docstrings, even for one-liner inline functions and properties)
#   C0301: Line too long (max 80), R0902: Too many instance attributes (includes dict())
#   C0302: too many lines in module; R0201: Method could be a function; W0142: Used * or ** magic
#   C0303: Trailing whitespace (happens if you have windows-style \r\n newlines)
#   R0904: Too many public methods (20 max); R0913: Too many arguments;
#   R0921: Abstract class not referenced. Pylint thinks any class that raises a NotImplementedError somewhere is abstract.
#   W0201: Attribute "_underscore_first_marks_insternal" defined outside __init__ -- yes, I use it in my properties.
#   W0221: Arguments differ from overridden method,
#   W0402: Use of deprechated module (e.g. string)
#   E1101: Instance of <object> has no <dynamically obtained attribute> member.
#   E0102: method already defined in line <...> (pylint doesn't understand properties well...)
#   E0202: An attribute affected in <...> hide this method (pylint doesn't understand properties well...)
# Regarding pylint failure of python properties: should be fixed in newer versions of pylint.
"""
Experiment module and its primary Experiment class
is the center for all "Experiment" related functionality.
It ties together two important helper objects:
* WikiPage
* JournalAssistant

It mostly models the local directory in which the experiment (data) is saved.
However, it is also the main node onto which other model parts of an experiment is attached, e.g.
- A WikiPage object, representing the wiki page on the server. This object should be capable of
  performing most server-related inqueries, particularly including wiki-page updates/appends.
  Experiment objects can also refer directly to the server object. However, this is mostly as
  a convenience and is mostly used before a WikiPage object is attached; in particular the
  server object is used to search for existing/matching wiki pages.
  Most other logic should be done by the ExperimentManager rather than individual experiments.

The Prop attribute is a dict which include the following info (is persisted as a .labfluence.yml file)
- localdir, relative to local_exp_rootDir
- expid, generated expid string e.g. 'RS123'
- exp_index
- exp_title_desc
- exp_series_longdesc, not used
- wiki_pageId
- wiki_pageTitle (cached)

Regarding "experiment entry" vs "experiment item" vs "-subitem":
 - 'subitem' returns 2e6 google hits, has entry at merriam-webster and wikitionary.
 - 'subentry' returns 4e5 google hits, but also has
 - 'item' returns 4e9 hits.
 - 'entry' returns 1.6e9 hits.
 --> I think I will go with entry, as that also relates to "entry in a journal/log".

Subentries attribute is a list of dicts, keyed alphabetically ('a','b',...) which each include:
- subentry_id, generated string e.g. 'RS123a'
- subentry_idx, index e.g. 'a'.
- subentry_titledesc,
- dirname, directory relative to the main experiment
- dirname should match expitem_title_desc
Note: The properties are related via config items exp_subentry_dir_fmt and exp_subentry_regex.
      The dir can be generated using exp_subentry_dir_fmt.format(...),
      while reversedly the items can be generated from the dirname using exp_subentry_regex,
      in much the same way as is done with the experiment (series).

Changes:
 - Trying to eliminate all the 'series' annotations and other excess;
   - exp_series_index --> exp_index
   - exp_series_shortdesc --> exp_title_desc
   - expid_str --> expid
 - Settled on term 'subentry' to designate 'RS123a'-like items.

 Considerations:
 - I recently implemented HierarchicalConfigHandler, which loads all '.labfluence.yml' files in the experiment tree.
   It seems counter-productive to also implement loading/saving of yaml files here

"""



import os
import yaml
import re
from datetime import datetime
from collections import OrderedDict
from operator import itemgetter
import logging
logger = logging.getLogger(__name__)

# Labfluence modules and classes:
from page import WikiPage, WikiPageFactory, make_page_url, make_subentry_anchor
from journalassistant import JournalAssistant
from filemanager import Filemanager
from utils import increment_idx, idx_generator, asciize
from decorators.cache_decorator import cached_property

from labfluencebase import LabfluenceBase


class Experiment(LabfluenceBase):
    """
    This class is the main model for a somewhat abstract "Experiment".
    See module docstring for further info.
    """

    def __init__(self, localdir=None, props=None, server=None, manager=None, confighandler=None, wikipage=None, regex_match=None,
                 doparseLocaldirSubentries=True, subentry_regex_prog=None, autoattachwikipage=True, savepropsonchange=True, makelocaldir=False, makewikipage=False):
        """
        Arguments:
        - localdir: path string
        - props: dict with properties
        - server: Server object
        - manager: parent manager object
        - confighandler: ExpConfigHandler object; this will be used to retrieve and save local .labfluence.yml properties
        - regex_match: A re.Match object, provided by reading e.g. the folder name or wiki pagetitle.
        - VERBOSE: The verbose level of the object, e.g. for debugging. (In addition to logging levels)
        - doparseLocaldirSubentries: Search the current directory for experiment subentries.
        - subentry_regex_prog: compiled regex pattern; saved for reuse.
          If not provided, will call confighandler.getEntry() to retrieve a regex.
        - loadYmlFn: filename to use when loading and persisting experiment props; only used if confighandler is not provided.
        """
        LabfluenceBase.__init__(self, confighandler, server)
        self.VERBOSE = 0
        #self.Confighandler = confighandler
        #self._server = server
        self._manager = manager
        if isinstance(wikipage, WikiPage) or wikipage is None:
            self._wikipage = wikipage
        else:
            # Assume page struct:
            self._wikipage = WikiPage(wikipage.get('id', wikipage.get('pageId', None)), self.Server, pagestruct=wikipage)
        # Attaching of wiki pages is done lazily on first call. _autoattachwikipage is not really used.
        self._autoattachwikipage = autoattachwikipage
        # NOTICE: Attaching wiki pages is done lazily using a property (unless makewikipage is not False)
        self.SavePropsOnChange = savepropsonchange
        self.PropsChanged = False # Flag,
        self._subentries_regex_prog = subentry_regex_prog # Allows recycling of a single compiled regex for faster directory tree processing.
        self.ConfigFn = '.labfluence.yml',
        # For use without a confighandler:
        self._props = dict()
        self._expid = None
        self._last_attachmentslist = None
        self._exp_regex_prog = None
        self._cache = dict() # cached_property cache
        self._allowmanualpropssavetofile = False # Set to true if you want to let this experiment handle Props file persisting without a confighandler.
        self._doserversearch = False
        localdir = localdir or props.get('localdir')
        if makelocaldir:
            logger.debug("makelocaldir is boolean True, invoking self.makeLocaldir(props=%s, localdir=%s)", props, localdir)
            localdir = self.makeLocaldir(props) # only props currently supported...
            logger.debug("localdir after makeLocaldir: %s", localdir)
        if localdir:
            self.setLocaldirpathAndFoldername(localdir)
        else:
            logger.debug("localdir is: %s (and makelocaldir was: %s), setting Localdirpath, Foldername and Parentdirpath to None.", localdir, makelocaldir)
            logger.info("NOTICE: No localdir provided for this experiment (are you in test mode?)\
functionality of this object will be greatly reduced and may break at any time.\
props=%s, regex_match=%s, wikipage='%s'", props, regex_match, wikipage)
            self.Localdirpath, self.Foldername, self.Parentdirpath = None, None, None

        ### Experiment properties/config related
        ### Manual handling is deprecated; Props are now a property that deals soly with confighandler.
        if props:
            self.Props.update(props)
            logger.debug("Experiment %s updated with props argument, is now %s", self, self.Props)
        if regex_match:
            gd = regex_match.groupdict()
            # In case the groupdict has multiple date fields, find out which one to use and discart the other keys:
            gd['date'] = next((date for date in [gd.pop(k, None) for k in ('date1', 'date2', 'date')] if date), None)
            ## regex is often_like "(?P<expid>RS[0-9]{3}) (?P<exp_title_desc>.*)"
            self.Props.update(gd)
        elif not 'expid' in self.Props:
            logger.debug("self.Props is still too empty (no expid field). Attempting to populate it using 1) the localdirpath and 2) the wikipage.")
            if self.Foldername:
                regex_match = self.updatePropsByFoldername()
            if not regex_match and wikipage: # equivalent to 'if wikipage and not regex_match', but better to check first:
                regex_match = self.updatePropsByWikipage()

        ### Subentries related - Subentries are stored as an element in self.Props ###
        # self.Subentries = self.Props.setdefault('exp_subentries', OrderedDict()) # Is now a property
        if doparseLocaldirSubentries and self.Localdirpath:
            self.parseLocaldirSubentries()

        if makewikipage and not self.WikiPage: # will attempt auto-attaching
            # page attaching should only be done if you are semi-sure that a page does not already exist.
            # trying to attach a wiki page will see if a page already exist.
            self.makeWikiPage()

        self.JournalAssistant = JournalAssistant(self)
        self.Filemanager = Filemanager(self)



    ### ATTRIBUTE PROPERTIES: ###
    @property
    def Props(self):
        """
        If localdirpath is provided, use that to get props from the confighandler.
        """
        if self.Localdirpath:
            props = self.Confighandler.getExpConfig(self.Localdirpath)
        else:
            props_cache = self.Confighandler.get('expprops_by_id_cache')
            _expid = getattr(self, '_expid', None)
            if props_cache and _expid:
                props = props_cache.setdefault(_expid, dict())
            else:
                if not hasattr(self, '_props'):
                    logger.debug("Setting self._props = dict()")
                    self._props = dict()
                props = self._props
                logger.debug("(test mode?) self.Localdirpath is '%s', props_cache type: %s, self._expid is: %s, returning props: %s",
                               getattr(self, 'Localdirpath', '<not set>'), type(props_cache), _expid, props)
        try:
            wikipage = self._wikipage # Do NOT try to use self.WikiPage. self.WikiPage calls self.attachWikiPage which calls self.Props -- circular loop.
            if not props.get('wiki_pagetitle') and wikipage and wikipage.Struct \
                        and props.get('wiki_pagetitle') != wikipage.Struct['title']:
                logger.info("Updating experiment props['wiki_pagetitle'] to '%s'", wikipage.Struct['title'])
                props['wiki_pagetitle'] = wikipage.Struct['title']
        except AttributeError as e:
            logger.debug("AttributeError: %s", e)
        return props
    @property
    def Subentries(self):
        """Should always be located one place and one place only: self.Props."""
        return self.Props.setdefault('exp_subentries', OrderedDict())
    @Subentries.setter
    def Subentries(self, subentries):
        """
        Re-set Subentries to <subentries>
        should be an OrderedDict alá subentries['a'] = dict-with-subentry-props.
        """
        if subentries != self.Props.get('exp_subentries'):
            self.Props['exp_subentries'] = subentries
            self.invokePropertyCallbacks('Subentries', subentries)
    @property
    def Expid(self):
        """Should always be located one place and one place only: self.Props."""
        return self.Props.get('expid')
    @Expid.setter
    def Expid(self, expid):
        """property setter"""
        if self.Props.get('expid') != expid:
            logger.info("Overriding old self.Expid '%s' with new expid '%s', localpath='%s'",
                        self.Expid, expid, self.Localdirpath)
            self.Props['expid'] = expid
            self.invokePropertyCallbacks('Expid', expid)
    @property
    def Wiki_pagetitle(self):
        """Should always be located one place and one place only: self.Props."""
        return self.Props.get('wiki_pagetitle')
    @property
    def PageId(self):
        """
        Should be located only as a property of self.WikiPage
        Uhm... should calling self.PageId trigger attachment of wiki page, with all
        of what that includes?
        No. Thus, using self._wikipage and not self.WikiPage.
        """
        if self._wikipage and self._wikipage.PageId:
            self.Props.setdefault('wiki_pageId', self._wikipage.PageId)
            return self._wikipage.PageId
        elif self.Props.get('wiki_pageId'):
            pageid = self.Props.get('wiki_pageId')
            if self._wikipage:
                # we have a wikipage instance attached, but it does not have a pageid??
                self._wikipage.PageId = pageid
            return pageid
    @PageId.setter
    def PageId(self, pageid):
        """
        Will update self.Props['wiki_pageId'], and make sure that self.WikiPage
        reflects the update.
        Uhm... should calling self.PageId trigger attachment of wiki page, with all
        of what that includes?
        """
        if self._wikipage:
            if self._wikipage.PageId != pageid:
                self._wikipage.PageId = pageid
                self._wikipage.reloadFromServer()
        self.Props['wiki_pageId'] = pageid
        self.flagPropertyChanged('PageId')


    @cached_property(ttl=60)
    def Attachments(self):
        """
        Returns list of attachment structs with metadata on attachments on the wiki page.
        Note that the list should be treated strictly as a read-only object:
        * It is not possible to set the Attachments list.
        * Any changes made to the list will be lost when the cache is expired.
        The property invokes the cached method listAttachments.
        To reset the cache and get an updated list, use getUpdatedAttachmentsList().
        """
        attachments = self.listAttachments()
        if self._last_attachmentslist != attachments:
            self.invokePropertyCallbacks('Attachments', attachments)
        else:
            logger.debug("The newly-fetched attachments list is identical to the old, not invoking property callbacks...")
        self._last_attachmentslist = attachments
        return attachments

    @property
    def Manager(self):
        """
        Retrieve manager from confighandler singleton registry if not specified manually.
        """
        return self._manager or self.Confighandler.Singletons.get('manager')
    @property
    def WikiPage(self):
        """
        Attempts to lazily attach a wikipage if none is attached.
        """
        if not self._wikipage:
            if self.attachWikiPage():
                logger.info("[%s] - Having just attached the wikipage (pageid=%s), I will now parse wikipage subentries and merge them...",
                            self, self._wikipage.PageId)
                self.mergeWikiSubentries(self._wikipage)
                self.invokePropertyCallbacks('WikiPage', self._wikipage)
        return self._wikipage
    @WikiPage.setter
    def WikiPage(self, newwikipage):
        """property setter"""
        if newwikipage != self._wikipage:
            self._wikipage = newwikipage
            self.flagPropertyChanged('WikiPage')
    @property
    def Fileshistory(self):
        """
        Invokes loadFilesHistory() lazily if self._fileshistory has not been loaded.
        """
        return self.Filemanager.Fileshistory
    @property
    def Exp_regex_prog(self):
        """
        Compiled regex for parsing experiments by e.g. foldername or wiki pagetitles.
        Might be different from one experiment to the next if the experiment's
        exp_series_regex config/property key has been customized.
        """
        if not getattr(self, '_exp_regex_prog', None):
            self._exp_regex_prog = re.compile(self.getConfigEntry('exp_series_regex'))
        return self._exp_regex_prog
    @property
    def Subentries_regex_prog(self):
        """
        Returns self._subentries_regex_prog if defined and not None, otherwise obtain from confighandler.
        Compiled regex for parsing subentries in this experiment by e.g. foldername or wiki page headers.
        Might be different from one experiment to the next if the experiment's
        exp_series_regex config/property key has been customized.
        """
        if not getattr(self, '_subentries_regex_prog', None):
            regex_str = self.getConfigEntry('exp_subentry_regex') #getExpSubentryRegex()
            if not regex_str:
                logger.warning("Warning, no exp_subentry_regex entry found in config, reverting to hard-coded default.")
                regex_str = r"(?P<date1>[0-9]{8})?[_ ]*(?P<expid>RS[0-9]{3})-?(?P<subentry_idx>[^_ ])[_ ]+(?P<subentry_titledesc>.+?)\s*(\((?P<date2>[0-9]{8})\))?$"
            self._subentries_regex_prog = re.compile(regex_str)
        return self._subentries_regex_prog
    @Subentries_regex_prog.setter
    def Subentries_regex_prog(self, subentry_regex_prog):
        """
        Compiled regex for parsing subentries in this experiment by e.g. foldername or wiki page headers.
        Might be different from one experiment to the next if the experiment's
        exp_subentry_regex config/property key has been customized.
        """
        self._subentries_regex_prog = subentry_regex_prog

    @property
    def Status(self):
        """
        Returns whether experiment is 'active' or 'recent'.
        Returns None if neither.
        """
        manager = self.Manager
        if manager:
            if self.Expid in manager.ActiveExperimentIds:
                return 'active'
            elif self.Expid in manager.RecentExperimentIds:
                return 'recent'
    def isactive(self):
        """Returns whether experiment is listed in the active experiments list."""
        return self.Status == 'active'
    def isrecent(self):
        """Returns whether experiment is listed in the recent experiments list."""
        return self.Status == 'recent'

    ## Non-property getters:
    def getUrl(self, mode="view"):
        """
        Returns a url to the wikipage.
        New argument mode, can be any of:
        - 'view' : Return a url to view the experiment's wiki page in a browser (default).
        - 'edit' : Returns link to edit page rather than just view it.
        - 'pageinfo' : Return a link to view the page's information.
        - 'pagehistory' : Return a link to view the page's history.
        - 'attachments' : Return a link to view the page's attachments.
        - 'subentry' : Return a link with an anchor to the currently selected subentry.
        """
        # Shortcut for the most ubuqutous use-case:
        if mode == 'view':
            url = self.Props.get('url', None)
            if url:
                return url
        if self.Server is None:
            return
        baseurl = self.Server.BaseUrl
        pageId = self.PageId
        anchor = None
        if mode == 'subentry':
            mode = 'view'
            pagetitle = self.Wiki_pagetitle
            # This will only work if the format has not been changed since the subentry was created on the page:
            subentryheader = self.getSubentryRepr(subentry_idx='current')
            if subentryheader:
                anchor = make_subentry_anchor(pagetitle, subentryheader)
        url = make_page_url(baseurl, pageId, mode, anchor)
        logger.debug("url obtained from (baseurl, pageId, mode, anchor) = (%s, %s, %s, %s): %s",
                     baseurl, pageId, mode, anchor, url)
        return url


    ################################
    ### MANAGER / MACRO methods: ###
    ################################

    def archive(self):
        """
        archive this experiment, relays through self.Manager.
        """
        mgr = self.Manager
        if not mgr:
            logger.debug("archive() invoked, but no ExperimentManager associated, aborting...")
            return
        self.Manager.archiveExperiment(self)


    def saveAll(self):
        """
        Method for remembering to do all things that must be saved before closing experiment.
         - self.Props, dict in .labfluence.yml
         - self.Fileshistory, dict in .labfluence/files_history.yml
        What else is stored in <localdirpath>/.labfluence/ ??
         - what about journal assistant files?

        Consider returning True if all saves suceeded and False otherwise...
        e.g.
            return self.saveProps() and self.saveFileshistory()
        """
        self.saveProps()
        self.Filemanager.saveFileshistory()



    """
    STUFF RELATED TO PROPERTY HANDLING/PERSISTING AND LOCAL DIR PARSING
    """

    def getConfigEntry(self, cfgkey, default=None):
        """
        Over-rides method from LabfluenceBase.
        self.Props is linked to Confighandler, so
            self.Confighandler.get(cfgkey, path=self.Localdirpath)
        should return exactly the same as
            self.Props.get(cfgkey)
        if cfgkey is in self.Props.
        However, probing self.Props.get(cfgkey) directly should be somewhat faster.
        """
        if cfgkey in self.Props:
            return self.Props.get(cfgkey)
        else:
            p = self.Localdirpath
            return self.Confighandler.get(cfgkey, default=default, path=p)

    def setConfigEntry(self, cfgkey, value):
        """
        Override method from LabfluenceBase.
        Sets config entry. If cfgkey is listed in self.Props, then set/update that,
        otherwise relay through to self.Confighandler.
        Notice: does not currently check the hierarchical config,
        only the explicidly loaded 'system', 'user', 'exp', 'cache', etc.
        """
        if cfgkey in self.Props:
            self.Props[cfgkey] = value
        else:
            self.Confighandler.setkey(cfgkey, value)

    def getAbsPath(self):
        """
        Returns the absolute path of self.Localdirpath. Not sure this is required?
        """
        return os.path.abspath(self.Localdirpath)

    def saveIfChanged(self):
        """
        Saves props if the self.PropsChanged flag has been switched to True.
        Can be invoked as frequently as you'd like.
        """
        if self.PropsChanged:
            self.saveProps()
            self.PropsChanged = False

    def saveProps(self, path=None):
        """
        Saves content of self.Props to file.
        If a confighandler is attached, allow it to do it; otherwise just persist as yaml to default location.
        Returns True if suceed and false if unsuccessful.
        """
        logger.debug("(Experiment.saveProps() triggered; confighandler: %s", self.Confighandler)
        if self.VERBOSE > 2:
            logger.debug("self.Props: %s", self.Props)
        path = path or self.Localdirpath
        if not path:
            logger.info("No path provided to saveProps and Experiment.Localdirpath is also '%s'", path)
            return False
        if self.Confighandler:
            if not os.path.isdir(path):
                path = os.path.dirname(path)
            logger.debug("Invoking self.Confighandler.saveExpConfig(path=%s)", path)#, self.Props)
            #self.Confighandler.updateAndPersist(path, self.Props)
            # Why use updateAndPersist? If there is a confighandler, just use
            ret = self.Confighandler.saveExpConfig(path)
            logger.debug("self.Confighandler.saveExpConfig returned value: %s", ret)
        elif self._allowmanualpropssavetofile:
            if os.path.isdir(path):
                path = os.path.normpath(os.path.join(self.Localdirpath, self.ConfigFn))
            logger.debug("Experiment.saveProps() :: No confighandler, saving manually to file '%s'", path)
            yaml.dump(self.Props, open(path, 'wb'))
            if self.VERBOSE > 4:
                logger.debug("Content of exp config/properties file after save:")
                logger.debug(open(os.path.join(path, self.ConfigFn)).read())
        else:
            return False
        return True


    def updatePropsByFoldername(self, regex_prog=None):
        """
        Update self.Props to match the meta info provided by the folder name,
        e.g. expid, titledesc and date.
        Returns the matching regex, in case it might be useful to the caller.
        """
        if regex_prog is None:
            regex_prog = self.Exp_regex_prog
        regex_match = regex_prog.match(self.Foldername)
        if regex_match:
            # will groupdict update Props?
            gd = regex_match.groupdict()
            if next((True for key, value in gd.items() if key not in self.Props or value != self.Props[key]), False):
                props = self.Props
                props.update(gd)
                logger.debug("Props updated using foldername %s and regex, returning groupdict %s", self.Foldername, regex_match.groupdict())
                if self.SavePropsOnChange:
                    self.saveProps()
                self.invokePropertyCallbacks('Props', props)
            else:
                logger.debug("Groupdict %s from parsed foldername '%s' does not seem to update self.Props with keys: %s",
                             gd, self.Foldername, self.Props.keys())
        return regex_match

    def updatePropsByWikipage(self, regex_prog=None):
        """
        Update self.Props to match the meta info provided by the wiki page (page title),
        e.g. expid, titledesc and date.
        """
        if regex_prog is None:
            regex_prog = self.Exp_regex_prog
        wikipage = self.WikiPage
        if not wikipage.Struct:
            wikipage.reloadFromServer()
        regex_match = regex_prog.match(wikipage.Struct.get('title'))
        if regex_match:
            gd = regex_match.groupdict()
            if next((True for key, value in gd.items() if key not in self.Props or value != self.Props[key]), False):
                props = self.Props
                props.update(regex_match.groupdict())
                logger.debug("Props updated using wikipage.Struct['title'] %s and regex, returning groupdict %s", regex_match.string, regex_match.groupdict())
                if self.SavePropsOnChange:
                    self.saveProps()
                self.invokePropertyCallbacks('Props', props)
            else:
                logger.debug("Groupdict %s from parsed foldername '%s' does not seem to update self.Props with keys: %s",
                             gd, self.Foldername, self.Props.keys())
        return regex_match


    def makeFormattingParams(self, subentry_idx=None, props=None):
        """
        Returns a dict containing all keys required for many string formatting interpolations,
        e.g. makes a dict that includes both expid and subentry props.
        """
        fmt_params = dict(datetime=datetime.now())
        # datetime always refer to datetime.datetime objects; 'date' may refer either to da date string or a datetime.date object.
        # edit: 'date' must always be a string date, formatted using 'journal_date_format'.
        fmt_params.update(self.Props) # do like this to ensure copy and not override, just to make sure...
        if subentry_idx:
            fmt_params['subentry_idx'] = subentry_idx
            fmt_params['next_subentry_idx'] = increment_idx(subentry_idx)
            if self.Subentries and subentry_idx in self.Subentries:
                fmt_params.update(self.Subentries[subentry_idx])
        if props:
            fmt_params.update(props) # doing this after to ensure no override.
        fmt_params['date'] = fmt_params['datetime'].strftime(self.Confighandler.get('journal_date_format', '%Y%m%d'))
        return fmt_params


    ###
    ### Methods related to the local directory:
    ###

    def setLocaldirpathAndFoldername(self, localdir):
        r"""
        Takes a localdir, either absolute or relative (to local_exp_subDir),
        and use this to set self.Foldername, self.Parentdirpath and self.Localdirpath.
        # We have a localdir. Local dirs may be of many formats, e.g.:
        #   /some/abosolute/unix/folder
        #   C:\some\absolute\windows\folder
        #   relative/unix/folder
        #   relative\windows\folder
        # More logic may be required, e.g. if the dir is relative to e.g. the local_exp_rootDir.
        """
        foldername, parentdirpath, localdirpath = self._getFoldernameAndParentdirpath(localdir)
        logger.debug("self._getFoldernameAndParentdirpath(%s) returned: %s, %s, %s", localdir, foldername, parentdirpath, localdirpath)
        self.Parentdirpath = parentdirpath
        if not foldername:
            logger.warning("Experiment.__init__() :: Warning, could not determine foldername...????")
        if getattr(self, 'Foldername', None):
            self.flagPropertyChanged('Foldername')
        self.Foldername = foldername
        if getattr(self, 'Localdirpath', None):
            self.flagPropertyChanged('Localdirpath')
        self.Localdirpath = localdirpath
        logger.debug("self.Parentdirpath=%s, self.Foldername=%s, self.Localdirpath=%s", self.Parentdirpath, self.Foldername, self.Localdirpath)

    def _getFoldernameAndParentdirpath(self, localdir):
        """
        Takes a localdir, either absolute or relative (to local_exp_subDir),
        and returns the foldername and parentdirpath of the localdir.
        """
        localdir = os.path.expanduser(localdir)
        if not os.path.isabs(localdir):
            # The path provided was relative, e.g.:
            # "RS102 Strep-col11 TR annealed with biotin",
            # or "2012_Aarhus/RS065 something".
            try:
                # Note: To avoid circular reference, using confighandler to obtain these.
                basedircandidates = [self.Confighandler.get(k) for k in ('local_exp_rootDir', 'local_exp_subDir')]
            except AttributeError as e:
                logger.debug(e)
                basedircandidates = list()
            if getattr(self, 'Parentdirpath', None):
                basedircandidates.append(self.Parentdirpath)
            localdircandidates = [os.path.join(basedircand, localdir) for basedircand in basedircandidates]
            localdircandidates.append(os.path.abspath(localdir))
            try:
                localdir = next(path for path in localdircandidates if os.path.isdir(path))
            except StopIteration:
                # No localdir found by searching the most obvious candidates. Trying a bit extra using local_exp_subDir:
                local_exp_subdir = self.Confighandler.getAbsExpPath('local_exp_subDir')
                logger.warning("localdir '%s' was not found, attempting to use local_exp_subDir (%s) as base.", localdir, local_exp_subdir)
                common = os.path.commonprefix([local_exp_subdir, localdir])
                if common:
                    # localdir might be a long relative dir, that shares most in comon with local_exp_subDir.
                    org = localdir
                    localdir = os.path.abspath(os.path.join(local_exp_subdir, os.path.relpath(localdir, local_exp_subdir)))
                    logger.info("EXPERIMENTAL: localdir set using os.path.abspath(os.path.join(local_exp_subdir, os.path.relpath(localdir, local_exp_subdir))):\
\n-localdir: %s\n-local_exp_subDir: %s\n-localdir: %s", org, local_exp_subdir, localdir)
                else:
                    localdir = os.path.join(local_exp_subdir, localdir)
                    logger.info("Setting localdir by joining local_exp_subdir and localdir, result is: %s", localdir)
        parentdirpath, foldername = os.path.split(localdir)
        return foldername, parentdirpath, localdir


    def makeLocaldir(self, props, basedir=None):
        """
        Alternatively, 'makeExperimentFolder' ?
        props:      Dict with props required to generate folder name.
        Edit: Instead of supporting wikipage argument, use updatePropsByWikipage
        and then pass self.Props.
        """
        # Note: If this is called as part of __init__, it is called as one of the first things,
        # before setting self.Props, and before pretty much anything.
        logger.debug("Experiment makeLocaldir invoked with props=%s, basedir=%s", props, basedir)
        localexpsubdir = basedir or self.Confighandler.getAbsExpPath('local_exp_subDir')
        try:
            foldername = self.getFoldernameFromFmtAndProps(props)
            localdirpath = os.path.join(localexpsubdir, foldername)
            os.mkdir(localdirpath)
            #logger.info("Created new localdir: %s", localdirpath)
        except (KeyError, TypeError, OSError, IOError) as e:
            logger.warning("%r making new folder, ABORTING...", e)
            return False
        logger.info("Created new localdir for experiment: %s", localdirpath)
        return localdirpath


    def getFoldernameFromFmtAndProps(self, props=None, foldername_fmt=None):
        """
        Generates a foldername formatted using props and the format string in
        confighandler's exp_series_dir_fmt config entry.
        """
        if props is None:
            props = self.Props
        if foldername_fmt is None:
            foldername_fmt = self.Confighandler.get('exp_series_dir_fmt')
        foldername = foldername_fmt.format(**props)
        return foldername



    def renameLocaldir(self, newfolder):
        """
        Renames the folder of the experiment's local folder.
        Will also rename path-based exp key in confighandler.
        newfolder can be either an absolute path, or relative compared to either of:
        - local_exp_rootDir
        - local_exp_subDir
        - self.Parentdirpath (in case the experiment was previously initialized).
        """
        oldlocaldirpath = self.Localdirpath
        if os.path.isabs(newfolder):
            newlocaldirpath = newfolder
        else:
            if getattr(self, 'Parentdirpath', None):
                newlocaldirpath = os.path.join(self.Parentdirpath, newfolder)
            else:
                logger.warning("renameLocaldir with relative foldername is only allowed if exp Parentdirpath has been set; ABORTING... (newfolder = %s)", newfolder)
                return False
        try:
            os.rename(oldlocaldirpath, newlocaldirpath)
        except (OSError, IOError) as e:
            logger.warning("%r while renaming old folder %s to new folder %s.", e, oldlocaldirpath, newlocaldirpath)
        self.Confighandler.renameConfigKey(oldlocaldirpath, newlocaldirpath)
        logger.info("Renamed old folder %s to new folder %s", oldlocaldirpath, newlocaldirpath)
        self.setLocaldirpathAndFoldername(newlocaldirpath)
        return newlocaldirpath


    def renameFolderByFormat(self):
        """
        Renames the local directory folder to match the formatting dictated by exp_series_dir_fmt.
        Also takes care to update the confighandler.

        NOT COMPLETELY IMPLEMENTED OR TESTED!
        """
        dir_fmt = self.Confighandler.get('exp_series_dir_fmt')
        if not dir_fmt:
            logger.warning("No 'exp_series_dir_fmt' found in config; aborting")
            return
        newname = dir_fmt.format(self.Props)
        newpath = os.path.join(self.Parentdirpath, newname)
        oldpath = self.Localdirpath
        logger.info("Renaming exp folder: %s -> %s", oldpath, newpath)
        #os.rename(oldname_full, newname_full)
        self.Localdirpath = newpath
        self.Foldername = newname
        # Note: there is NO reason to have a key 'dirname' in self.Props;
        if self.Confighandler:
            self.Confighandler.renameConfigKey(oldpath, newpath)


    ###
    ### STUFF RELATED TO SUBENTRIES ###
    ###

    def renameSubentriesFoldersByFormat(self, createNonexisting=False):
        """
        Renames all subentries folders to match the configured format
        specified with config key exp_subentry_dir_fmt.
        """
        logger.warning("This method is temporarily disabled while testing...")
        dir_fmt = self.getConfigEntry('exp_subentry_dir_fmt')
        if not dir_fmt:
            logger.warning("No 'exp_subentry_dir_fmt' found in config; aborting")
            return
        for subentry in self.Subentries.values():
            # subentry is a dict
            newname = dir_fmt.format(subentry)
            newname_full = os.path.join(self.Localdirpath, newname)
            if 'dirname' in subentry:
                oldname_full = os.path.join(self.Localdirpath, subentry['dirname'])
                logger.info("Renaming subentry folder: %s -> %s", oldname_full, newname_full)
                #os.rename(oldname_full, newname_full)
            elif createNonexisting:
                logger.info("Making new subentry folder: %s", newname_full)
                #os.mkdir(newname_full)
            subentry['dirname'] = newname

    def sortSubentrires(self):
        """
        Make sure the subentries are properly sorted. They might not be, e.g. if subentry f was created locally
        while subentry e was created on the wiki page and only read in later.
        """
        #org_keyorder = self.Subentries.keys()
        if self.Subentries.keys() == sorted(self.Subentries.keys()):
            # no change...
            return
        self.Subentries = OrderedDict(sorted(self.Subentries.items()))
        # The Subentries setter will invoke callbacks...


    def addNewSubentry(self, subentry_titledesc, subentry_idx=None, subentry_date=None,
                       extraprops=None, makefolder=False, makewikientry=False,
                       batchmode=False):
        """
        Adds a new subentry and add it to the self.Props['subentries'][<subentry_idx>].
        Optionally also creates a local subentry folder and adds a new subentry section to the wiki page,
        by relaying to self.makeSubentryFolder() and self.makeWikiSubentry()
        Setting batchmode to True will not call invokePropertyCallbacks, but
        just flagPropertyChanged('Subentries')
        """
        if subentry_idx is None:
            subentry_idx = self.getNewSubentryIdx()
        if subentry_idx in self.Subentries:
            logger.error("Experiment.addNewSubentry() :: ERROR, subentry_idx '%s' already listed in subentries, aborting...", subentry_idx)
            return
        if subentry_date is None:
            subentry_datetime = datetime.now()
            subentry_date = "{:%Y%m%d}".format(subentry_datetime)
        elif isinstance(subentry_date, datetime):
            subentry_datetime = subentry_date
            subentry_date = "{:%Y%m%d}".format(subentry_datetime)
        elif isinstance(subentry_date, basestring):
            date_format = self.getConfigEntry('journal_date_format')
            subentry_datetime = datetime.strptime(subentry_date, date_format)
        subentry = dict(subentry_idx=subentry_idx, subentry_titledesc=subentry_titledesc, date=subentry_date, datetime=subentry_datetime)
        if extraprops:
            subentry.update(extraprops)
        self.Subentries[subentry_idx] = subentry
        if makefolder:
            self.makeSubentryFolder(subentry_idx)
        if makewikientry:
            self.makeWikiSubentry(subentry_idx)
        self.saveIfChanged()
        if batchmode:
            self.flagPropertyChanged('Subentries')
        else:
            self.invokePropertyCallbacks('Subentries', self.Subentries)
        return subentry


    def getSubentryFoldername(self, subentry_idx):
        """
        Returns the foldername for a particular subentry, relative to the experiment directory.
        Returns None in case of e.g. KeyError.
        Returns False if foldername is an existing path, but not a directory.
         -- Edit: This is currently not implemented; I will cross that bridge if it ever becomes an issue.
        """
        try:
            subentry = self.Subentries[subentry_idx]
        except KeyError:
            logger.info("subentry_idx not in self.Subentries, returning None")
            return
        if 'foldername' in subentry:
            foldername = subentry['foldername']
            if os.path.isdir(os.path.join(self.Localdirpath, foldername)):
                return foldername
            else:
                logger.warning("Subentry '%s' exists in self.Props and has a 'foldername' key with \
                               value '%s', but this is not a foldername!", subentry_idx, foldername)
        # No existing folder specified; make one from the format provided in configentry:
        fmt_params = self.makeFormattingParams(subentry_idx=subentry_idx, props=subentry)
        subentry_foldername_fmt = self.getConfigEntry('exp_subentry_dir_fmt')
        subentry_foldername = subentry_foldername_fmt.format(**fmt_params)
        return subentry_foldername


    def existingSubentryFolder(self, subentry_idx):
        """
        Serves two purposes:
        1) To tell whether a particular subentry exists,
        2) If returntuple is True, will return a tuple consisting of:
        Returns tuple of:
            (whether_subentry_folder_exist, subentry_folder_path, subentry_foldername)
        where the first element is True if a folder exists for subentry_idx,
        the second element is the complete path to the subentry folder
        and the third element is the basename of the subentry folder.

        This means you CANNOT simply use as:
            if existingSubentryFolder('a'):     # THIS WILL ALWAYS BE TRUE,
                (...)
        instead, probe the first element of the returned tuple:
            if existingSubentryFolder('a')[0]:
                (...)
        """
        subentry_foldername = self.getSubentryFoldername(subentry_idx)
        folderpath = os.path.realpath(os.path.join(self.Localdirpath, subentry_foldername))
        if os.path.isdir(folderpath):
            return (True, folderpath, subentry_foldername)
        elif os.path.exists(folderpath):
            logger.warning("The folder specified by subentry '%s' exists, but is not a directory: %s ", subentry_idx, folderpath)
        return (False, folderpath, subentry_foldername)

    def makeSubentryFolder(self, subentry_idx):
        """
        Creates a new subentry subfolder in the local experiment directory,
        with a foldername matching the format dictated in the config
        as config key 'exp_subentry_dir_fmt'.
        """
        try:
            subentry = self.Subentries[subentry_idx]
        except KeyError:
            logger.warning("Experiment.makeSubentryFolder() :: ERROR, subentry_idx '%s' not listed in subentries, aborting...", subentry_idx)
            return
        folder_exists, newfolderpath, subentry_foldername = self.existingSubentryFolder(subentry_idx)
        if folder_exists:
            logger.error("Experiment.makeSubentryFolder() :: ERROR, newfolderpath (%s) already exists, aborting...", newfolderpath)
            return
        try:
            os.mkdir(newfolderpath)
        except OSError as e:
            logger.error("ERROR making new folder: '%s'; Will return False; OSError is: '%s'", newfolderpath, e)
            return False
        subentry['foldername'] = subentry_foldername
        if self.SavePropsOnChange:
            self.saveProps()
        return subentry_foldername


    def initSubentriesUpTo(self, subentry_idx):
        """
        Make sure all subentries are initiated up to subentry <subentry_idx>.
        """
        count = 0
        for idx in idx_generator(subentry_idx):
            if idx not in self.Subentries:
                self.Subentries[idx] = dict()
                count += 1
        if count:
            self.invokePropertyCallbacks('Subentries', self.Subentries)



    def parseLocaldirSubentries(self, directory=None):
        """
        make self.Subentries by parsing local dirs like '20130106 RS102f PAGE of STV-col11 TR staps (20010203)'.
        # Consider using glob.re
        """
        directory = directory or self.Localdirpath
        if directory is None:
            logger.error("Experiment.parseLocaldirSubentries() :: ERROR, no directory provided and no localdir in Props attribute.")
            return
        regex_prog = self.Subentries_regex_prog
        localdirs = sorted(dirname for dirname in os.listdir(directory) if os.path.isdir(os.path.abspath(os.path.join(directory, dirname))))
        subentries = self.Subentries
        logger.debug("Parsing directory '%s' for subentries using regex = '%s', localdirs = %s, subentries before parsing = %s",
                     directory, regex_prog.pattern, localdirs, subentries)
        matchsubdirs = sorted(((match.group('subentry_idx'), match) for match in (regex_prog.match(subdir) for subdir in localdirs) if match),
                              key=itemgetter(0))
        count = 0
        for idx, match in matchsubdirs:
            gd = match.groupdict()
            logger.debug("MATCH found when for folder '%s', groupdict = %s", match.string, gd)
            # I allow for regex with multiple date entries, i.e. both at the start end end of filename.
            datekeys = sorted(key for key in gd.keys() if 'date' in key)
            gd['date'] = next((date for date in [gd.pop(k) for k in datekeys] if date), None)
            gd['foldername'] = match.string
            # If subentry_idx is not in gd, then the regex is wrong and it is ok to fail with KeyError
            # Note that if subentry_idx is not present, simply making a new index could be dangerous; what if the directories are not sorted and the next index is not right?
            # check whether something will be updated:
            if next((True for key, value in gd.items() if key not in subentries or value != subentries[key]), False):
                subentries.setdefault(idx, dict()).update(gd)
                count += 1
        if count:
            self.invokePropertyCallbacks('Subentries', subentries)
        return subentries


    def parseSubentriesFromWikipage(self, wikipage=None, xhtml=None, return_subentry_xhtml=False):
        """
        # TODO: Move to experimentpage (currently only two methods, so stays here for now...)
        Arguments:
          wikipage  : the wikipage (object) to parse
          xhtml     : parse directly this xhtml

        If wikipage and xhtml is None, then self.WikiPage will be used.

        Returns an OrderedDict with subentries, similar to the subentries in self.Props['exp_subentries'],
            i.e.     subentry['a'] = {'expid': ...}

        Returns None if something failed.

        If return_subentry_xhtml is set to True, then the subentry-dict in the returned subentries dict
        will include the current xhtml source for that subentry. This is generally NOT desired.
        Note: wikipage is a WikiPage object, not a page struct.
        """
        ### Uh, it would seem that the wiki_experiment_section config entry has gone missing,
        ### returning none until it is back up.
        if xhtml is None:
            if wikipage is None:
                wikipage = self.WikiPage
            xhtml = wikipage.Content
        # GENERATE required regex programs:
        try:
            expsection_regex_prog = re.compile(self.getConfigEntry('wiki_experiment_section'), flags=re.DOTALL+re.MULTILINE)
            logger.debug("wiki_experiment_section regex is: %s", expsection_regex_prog.pattern)
        except TypeError as e:
            logger.warning("TypeError: %s while creating regex prog; self.getConfigEntry('wiki_experiment_section')=%s; (If None, then 'wiki_experiment_section' is probably not set in config) - ABORTING...",
                           e, self.getConfigEntry('wiki_experiment_section'))
            return
        try:
            subentry_regex_fmt = self.getConfigEntry('wiki_subentry_regex_fmt')
            logger.debug("wiki_subentry_regex_fmt is: '%s'", subentry_regex_fmt)
            subentry_regex = subentry_regex_fmt.format(expid=self.Expid, subentry_idx=r"(?P<subentry_idx>[a-zA-Z]+)") # alternatively, throw in **self.Props
            logger.debug("Subentry regex after format substitution: '%s'", subentry_regex)
            subentry_regex_prog = re.compile(subentry_regex, flags=re.DOTALL+re.MULTILINE)
        except (TypeError, KeyError) as e:
            logger.warning("%r while creating wiki subentry regex prog; self.getConfigEntry('wiki_subentry_regex_fmt')=%s; ABORTING...",
                           e, self.getConfigEntry('wiki_subentry_regex_fmt'))
            return
        # PARSE the wiki xhtml:
        expsection_match = expsection_regex_prog.match(xhtml) # consider using search instead of match?
        if not expsection_match:
            logger.warning("NO MATCH ('%s') for expsubsection_regex '%s' in xhtml of length %s, aborting",
                           expsection_match, expsection_regex_prog.pattern, len(xhtml))
            logger.debug("xhtml is: %s", xhtml)
            return
        exp_xhtml = expsection_match.groupdict().get('exp_section_body')
        if not exp_xhtml:
            logger.warning("Aborting, exp_section_body is empty: %s", exp_xhtml)
            return
        wiki_subentries = OrderedDict()
        for match in subentry_regex_prog.finditer(exp_xhtml):
            gd = match.groupdict()
            logger.debug("Match groupdict: {%s}", ", ".join(u"{} : {}".format(key, value[0:20]+' (....) '+value[-20:] if value and len(value) > 50 else value)
                                                            for key, value in gd.items()))
            if not return_subentry_xhtml:
                gd.pop('subentry_xhtml')
            # If a datestring is present, convert it to a datetime type.
            datestring = gd.pop('subentry_date_string')
            if datestring:
                gd['date'] = datetime.strptime(datestring, "%Y%m%d")
            if gd['subentry_idx'] in wiki_subentries:
                logger.warning("Duplicate subentry_idx '%s' encountered while parsing subentries from xhtml.", gd['subentry_idx'])
            wiki_subentries[gd['subentry_idx']] = gd
        return wiki_subentries


    def mergeWikiSubentries(self, wikipage=None):
        """
        Used to parse existing subentries (in self.Props) with subentries
        obtained by parsing the wiki page.
        Returns the number of created subentries (count)
        and False if something fails.
        """
        wiki_subentries = self.parseSubentriesFromWikipage(wikipage)
        if wiki_subentries is None:
            return False
        # OrderedDict returned
        subentries = self.Subentries
        count = 0
        for subentry_idx, subentry_props in wiki_subentries.items():
            if subentry_idx in subentries:
                logger.debug("Subentry '%s' from wikipage already in Subentries", subentry_idx)
            else:
                subentries[subentry_idx] = subentry_props
                count += 1
                logger.debug("Subentry '%s' from wikipage added to Subentries, props are: %s",
                             subentry_idx, subentry_props)
        if count:
            self.invokePropertyCallbacks('Subentries', subentries)
        return count


    def getNewSubentryIdx(self):
        """
        Returns the next subentry idx, e.g.:
        if 'a', 'b', 'd' are existing subentries --> return 'e'
        """
        if not self.Subentries:
            return 'a'
        return increment_idx(sorted(self.Subentries.keys())[-1])


    ###
    ### STUFF related to local file management
    ###


    def getPathFor(self, pathkey):
        """
        Returns the path for various elements, e.g.
        - 'exp'  (default)      -> returns self.Localdirpath
        - 'local_exp_subDir'
        - 'local_exp_rootDir'
        """
        if pathkey is None or pathkey == 'exp':
            relstart = self.Localdirpath
        elif pathkey in ('local_exp_rootDir', 'local_exp_subDir'):
            relstart = self.Confighandler.getAbsExpPath(pathkey)
        else:
            relstart = pathkey
        return relstart

    def listLocalFiles(self, relative=None):
        """
        Lists all local files, essentially a lite version of getLocalFilelist
        that makes it clear that the task can be accomplished as a one-liner :-)
        """
        return self.Filemanager.listLocalFiles(relative)

    def getLocalFilelist(self, fn_pattern=None, fn_is_regex=False, relative=None, subentries_only=True, subentry_idxs=None):
        """
        Returns a filtered list of local files in the experiment directory and sub-folders,
        filtering by:
        - fn_pattern
        - fn_is_regex   -> if True, will enterpret fn_pattern as a regular expression.
        - relative       -> relative to what ('exp', 'local_exp_subDir', 'local_exp_rootDir')
        - subentries_only -> only return files from subentry folders and not other files.
        - subentries_idxs -> only return files from from subentries with these subentry indices (sequence)
        """
        return self.Filemanager.getLocalFilelist(fn_pattern, fn_is_regex, relative, subentries_only, subentry_idxs)


    ###
    ### CODE RELATED TO WIKI PAGE HANDLING
    ###

    def reloadWikipage(self):
        """
        Reload the attached wiki page from server.
        """
        self.WikiPage.reloadFromServer()
        self.invokePropertyCallbacks('WikiPage', self.WikiPage)


    def getWikiXhtml(self, ):
        """
        Get xhtml for wikipage.
        """
        if not self.WikiPage or not self.WikiPage.Struct:
            logger.warning("WikiPage or WikiPage.Struct is None, aborting...")
            logger.warning("-- %s is %s", 'self.WikiPage.Struct' if self.WikiPage else self.WikiPage, self.WikiPage.Struct if self.WikiPage else self.WikiPage)
            return
        content = self.WikiPage.Struct['content']
        return content


    def getWikiSubentryXhtml(self, subentry=None):
        """
        # TODO: Move to experimentpage
        Get xhtml (journal) for a particular subentry on the wiki page.
        subentry defaults to self.JournalAssistant.Current_subentry_idx.
        """
        subentry = subentry or self.getCurrentSubentryIdx()
        if not subentry:
            logger.info("No subentry set/selected/available, aborting...")
            return None
        #xhtml = self.WikiPage.getWikiSubentryXhtml(subentry)
        regex_pat_fmt = self.Confighandler.get('wiki_subentry_parse_regex_fmt')
        fmt_params = self.makeFormattingParams(subentry_idx=subentry)
        regex_pat = regex_pat_fmt.format(**fmt_params)
        if not regex_pat:
            logger.warning("No regex pattern found in config, aborting...")
            return
        if not self.WikiPage or not self.WikiPage.Struct:
            logger.info("WikiPage or WikiPage.Struct is None, aborting...")
            logger.info("-- %s is %s", 'self.WikiPage.Struct' if self.WikiPage else self.WikiPage, self.WikiPage.Struct if self.WikiPage else self.WikiPage)
            return
        content = self.WikiPage.Struct['content']
        regex_prog = re.compile(regex_pat, flags=re.DOTALL)
        match = regex_prog.search(content)
        if match:
            gd = match.groupdict()
            return "\n".join(gd[k] for k in ('subentry_header', 'subentry_xhtml'))
        else:
            logger.debug("No subentry xhtml found matching regex_pat '%s', derived from regex_pat_fmt '%s'. len(self.WikiPage.Struct['content']) is: %s",
                         regex_pat, regex_pat_fmt, len(self.WikiPage.Struct['content']))
            return None

    def getCurrentSubentryIdx(self):
        """
        Returns current subentry index (which is held and managed by the JournalAssistant...)
        """
        return getattr(self.JournalAssistant, 'Current_subentry_idx', None)


    def attachWikiPage(self, pageId=None, pagestruct=None):
        """
        Searches the server for a wiki page using the experiment's metadata,
        if pageid is already stored in the Props, then using that, otherwise
        searching the servier using e.g. expid, title, etc.
        A WikiPage is only attached (and returned) if a page was found on the server
        (yielding a correct pageid).
        """
        if pageId is None:
            if pagestruct and 'id' in pagestruct:
                pageId = pagestruct['id']
            else:
                pageId = self.Props.get('wiki_pageId', None)
        if (pageId is None and self._wikipage) or (self._wikipage and self._wikipage.PageId == pageId):
            logger.warning("attachWikiPage invoked, but no (new) pageId was provided, and existing wikipage already attached, aborting. If a wrong pageId is registrered, you need to remove it manually.")
            return
        if not pageId:
            logger.info("(exp with expid=%s), pageId is boolean false, invoking self.searchForWikiPage()...", self.Expid)
            pagestruct = self.searchForWikiPage()
            if pagestruct:
                logger.debug("searchForWikiPage returned a pagestruct with id: %s", pagestruct['id'])
                self.Props['wiki_pageId'] = pageId = pagestruct['id']
                if self.SavePropsOnChange:
                    self.saveProps()
        logger.debug("Params are: pageId: %s  server: %s   pagestruct: %s", pageId, self.Server, pagestruct)
        # Does it make sense to create a wikiPage without pageId? No. This check should take care of that:
        if not pageId:
            logger.info("Notice - no pageId found for expid %s (self.Server=%s)...", self.Props.get('expid'), self.Server)
            return pagestruct
        self.WikiPage = wikipage = WikiPage(pageId, self.Server, pagestruct)
        struct = wikipage.Struct
        # Update self.Props for offline access to the title of the wiki page:
        if struct:
            logger.debug("Setting experiment props['wiki_pagetitle'] = %s", struct['title'])
            self.Props['wiki_pagetitle'] = struct['title']
        else:
            logger.info("Wiki page struct obtained with pageId %s is: %s", pageId, struct)
        return wikipage

    def searchForWikiPage(self):
        """
        Argument <extended> is used to control how much search you want to do.
        Search strategy:
        1) Find page on wiki in space with pageTitle matching self.Foldername.
        2) Query manager for CURRENT wiki experiment pages and see if there is one that has matching expid.
        3) Query exp manager for ALL wiki experiment pages and see if there is one that has matching expid.
        3) Find pages in space with user as contributor and expid in title.
        Hmm... being able to define list with multiple spaceKeys and wiki_exp_root_pageId
        would make it a lot easier for users with wikipages scattered in several spaces...?
        Also, for finding e.g. archived wikipages...
        """
        # Start by querying manager's cache:
        expid = self.Expid  # uses self.Props
        if self.Manager:
            currentwikipagesbyexpid = self.Manager.CurrentWikiExperimentsPagestructsByExpid # cached_property
            if currentwikipagesbyexpid and expid in currentwikipagesbyexpid:
                return currentwikipagesbyexpid[expid]
        else:
            logger.warning("Experiment %s has no ExperimentManager.", expid)
        # Query server:
        server = self.Server
        if not server:
            logger.info("self.Server is: %s, ABORTING.", server)
            return None
        spaceKey = self.Confighandler.get('wiki_exp_root_spaceKey')
        pageTitle = self.Foldername or self.getFoldernameFromFmtAndProps() # No reason to make this more complicated...
        user = self.Confighandler.get('wiki_username') or self.Confighandler.get('username')
        optional = {'creator': (user, ), 'modifier': (user, )}
        required = {'title': (expid, )}
        logger.info("Searching for page with title=%s, space=%s on server...", pageTitle, spaceKey)
        pagestruct = server.searchForWikiPage(spaceKey, pageTitle, required, optional)
        return pagestruct


    def makeWikiPage(self, pagefactory=None):
        """
        Unlike attachWikiPage which attempts to attach an existing wiki page,
        this method creates a new wiki page and persists it to the server.
        Changes:
        - Removed , dosave=True argument
            Props should always be saved/persisted after making a wiki page,
            otherwise the pageId might be lost.
        """
        if not (self.Server and self.Confighandler):
            logger.error("Experiment.makeWikiPage() :: FATAL ERROR, no server and/or no confighandler.")
            return
        if pagefactory is None:
            pagefactory = WikiPageFactory(self.Server, self.Confighandler)
        current_datetime = datetime.now()
        fmt_params = dict(datetime=current_datetime, date=current_datetime)
        fmt_params.update(self.Props)
        self.WikiPage = wikipage = pagefactory.new('exp_page', fmt_params=fmt_params)
        self.Props['wiki_pageId'] = self.WikiPage.Struct['id']
        # Always save/persist props after making a wiki page, otherwise the pageId might be lost.
        self.saveProps()
        self.invokePropertyCallbacks('WikiPage', wikipage)
        return wikipage


    def makeWikiSubentry(self, subentry_idx, subentry_titledesc=None, updateFromServer=True, persistToServer=True):
        """
        Edit: This has currently been delegated to self.JournalAssistant, which specializes in inserting
        content at the right location using regex'es.
        """
        if subentry_idx not in self.Subentries:
            logger.warning("Experiment.makeWikiSubentry() :: ERROR, subentry_idx '%s' not in self.Subentries; make sure to first add the subentry to the subentries list and _then_ add a corresponding subentry on the wikipage.", subentry_idx)
            return
        res = self.JournalAssistant.newExpSubentry(subentry_idx, subentry_titledesc=subentry_titledesc, updateFromServer=updateFromServer, persistToServer=persistToServer)
        #pagetoken = self.getConfigEntry('wiki_exp_new_subentry_token') # I am no longer using tokens, but relying on regular expressions to find the right insertion spot.
        if res:
            self.saveIfChanged()
            # I am NOT invoking 'Subentries' callbacks; this is done elsewhere.
        return res


    def uploadAttachment(self, filepath, att_info=None, digesttype='md5'):
        """
        Upload attachment to wiki page.
        Returns True if succeeded, False if failed and None if no attemt was made to upload due to a local Error.
        Fields for attachmentInfo are:
            Key         Type    Value
            id          long    numeric id of the attachment
            pageId      String  page ID of the attachment
            title       String  title of the attachment
            fileName    String  file name of the attachment (Required)
            fileSize    String  numeric file size of the attachment in bytes
            contentType String  mime content type of the attachment (Required)
            created     Date    creation date of the attachment
            creator     String  creator of the attachment
            url         String  url to download the attachment online
            comment     String  comment for the attachment (Required)
        #attachment = self.WikiPage.addAttachment(attachmentInfo, attachmentData)
        """
        attachment = self.Filemanager.uploadAttachment(filepath, att_info, digesttype)
        return attachment


    def getUpdatedAttachmentsList(self):
        """
        Updates the attachments cache by resetting the listAttachements cache
        and then returning self.Attachments.
        Returns updated list of attachments (or empty list if server query failed).
        """
        # Reset the cache:
        del self._cache['Attachments']
        structs = self.Attachments # The Attachments cached property invokes callbacks whenever cache has expired.
        if not structs:
            logger.info("Attachments property / listAttachments() returned '%s'", structs)
        return structs

    # edit: cached_property makes the method a property and can no longer be used as a method.
    # I have moved the caching to the Attachments property instead...
    def listAttachments(self):
        """ Returns a list of attachments from the associated wikipage. """
        return self.Filemanager.getAttachments()

    def getAttachmentList(self, fn_pattern=None, fn_is_regex=False, **filterdict):
        """
        Note: This does not simply return a list of wiki attachments.
        This is the wiki-attachments equivalent to getLocalFileslist(),
        Returns a tuple list of (<display>, <identifier>, <complete struct>) elements.
        Like getLocalFileslist, the returned list can be filtered based on
        filename pattern (glob; or regex if fn_is_regex is True).
        The filterdict kwargs are currently not used.
        """
        return self.Filemanager.getAttachmentList(fn_pattern, fn_is_regex, **filterdict)



    ###
    ### String representations:
    ###
    def __repr__(self):
        try:
            return "e>"+self.Confighandler.get('exp_series_dir_fmt').format(**self.Props)
        except KeyError:
            logger.debug("KeyError trying to obtain string representation using self.Confighandler.get('exp_series_dir_fmt').format(**self.Props). Returning default.")
            return "e>"+str(getattr(self, 'Foldername', '<no-foldername>'))+str(self.Props)

    def getExpRepr(self, default=None):
        """
        Returns a string representation of this exp object.
        Used by self.__repr__()
        """
        if 'foldername' in self.Props:
            return self.Props['foldername']
        else:
            fmt = self.Confighandler.get('exp_series_dir_fmt')
            fmt_params = self.makeFormattingParams()
            try:
                return fmt.format(**fmt_params)
            except KeyError:
                if default:
                    return default
                else:
                    return "{} {}".format(asciize(self.Props.get('expid', "(no expid)")), asciize(self.Props.get('exp_titledesc', "(no title)")))

    def getSubentryRepr(self, subentry_idx=None, default=None):
        """
        Returns a string representation for a particular subentry,
        formatted according to config entry 'exp_subentry_dir_fmt'
        Is used to create new subentry folders, and display subentries in e.g. lists, etc.
        """
        if subentry_idx == 'current':
            subentry_idx = self.getCurrentSubentryIdx()
        if subentry_idx:
            subentry = self.Subentries.get(subentry_idx, None)
            if subentry:
                if 'foldername' in subentry:
                    return subentry['foldername']
                else:
                    fmt = self.Confighandler.get('exp_subentry_dir_fmt')
                    fmt_params = self.makeFormattingParams(subentry_idx=subentry_idx)
                    try:
                        return fmt.format(**fmt_params)
                    except KeyError:
                        if default:
                            return default
                        else:
                            return "{}{} {}".format(
                                asciize(self.Props.get('expid', "(no expid)")),
                                asciize(subentry.get('subentry_idx', "(no subentryidx)")),
                                asciize(self.Props.get('subentry_titledesc', "(no subentry title)")))
        if default == 'exp':
            return self.getExpRepr()
        else:
            return default
