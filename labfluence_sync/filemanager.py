#!/usr/bin/env python
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
# x-pyxxlint: disable-msg=C0103,C0301,C0302,R0902,R0201,W0142,R0913,R0904,W0221,W0402,E0202,W0201
# pylint: disable-msg=C0103,C0301
# xpylint: attr-rgx=[A-Z_][a-z0-9_]{2,30}$
# xpylint: good-names=logger
"""
Module for all file- and attachment related functions.
Implemented as a single class which has a 1:1:1 relationship as
                        Experiment
                       /          \
            Filemanager             WikiPage
           /
SatelliteMgr
           \
            SatelliteLocation
"""
from __future__ import print_function
#from six import string_types
import os
from datetime import datetime
import yaml
import fnmatch
import re
import logging
logger = logging.getLogger(__name__)
from utils import filehexdigest, attachmentTupFromFilepath


class Filemanager(object):
    """
    Class for all (most) file- and attachment related operations.
    Note that this should actually be named "ExpFileManager":
        It is NOT a generic class to deal with all files.
        It is specifically designed to deal with a single experiment's files.
        It is basically just refactoring out file-specific logic from the (very large) experiment module.
        It is implemented as a class, because it is also designed to keep track of file history, etc.
    """
    def __init__(self, experiment=None):
        """
        Init doc
        """
        # Other params: localdir=None, wikipage, pageid, ?
        self.Experiment = experiment
        self._fileshistory = None

    @property
    def Localdirpath(self):
        """ Path to experiment directory on the local filesystem. """
        return self.Experiment.Localdirpath

    @property
    def Subentries(self):
        """ Experiment subentries, dict, stored in Experiment.Props dict. """
        return self.Experiment.Subentries

    @property
    def Confighandler(self):
        """ The universal confighandler """
        return self.Experiment.Confighandler

    @property
    def Attachments(self):
        """ Attachments. Currently obtained from Experiment."""
        return self.Experiment.Attachments

    @property
    def Fileshistory(self):
        """
        Invokes loadFilesHistory() lazily if self._fileshistory has not been loaded.
        ###I plan to allow for saving file histories, having a dict
        ###Fileshistory['RS123d subentry_titledesc/RS123d_c1-grid1_somedate.jpg'] -> list of {datetime:<datetime>, md5:<md5digest>} dicts.
        ###This will make it easy to detect simple file moves/renames and allow for new digest algorithms.
        """
        if not self._fileshistory:
            ok = self.loadFileshistory() # Make sure self.loadFileshistory does NOT refer to self.Fileshistory (cyclic reference)
            if not ok:
                logger.error("Critical error encountered while trying to load fileshistory; returning fake empty dict() to prevent complete failover.")
                return dict()
        return self._fileshistory

    @property
    def WikiPage(self):
        """
        I *was* trying out a slightly different paradigm here in filemanager:
        Instead of getting a wiki-page that might be None and checking "if wikipage is None: ..."
        I raise an attribute error:
        try:
            wikipage = self.WikiPage
        except AttributeError as e:
            logger.error("Could not get wikipage, returning fake None to avoid failover; error was: %s", e)
            return None
        HOWEVER, that is not really any better than:
        wikipage = self.WikiPage
        if wikipage is None:
            logger.error("Could not get wikipage, returning fake None to avoid failover; error was: %s", e)
            return None
        """
        return self.Experiment.WikiPage


    #####
    ##### General methods:
    #####

    def getPathFor(self, relative):
        """
        Returns the relative path for various elements, e.g.
        - 'exp'  (default)      -> returns self.Localdirpath
        - 'local_exp_subDir'
        - 'local_exp_rootDir'
        None will return the same as 'exp'.
        """
        return self.Experiment.getPathFor(relative)

    def hashFile(self, filepath, digesttypes=('md5', )):
        """
        Default is currently md5, although e.g. sha1 is not that much slower.
        The sha256 and sha512 are approx 2x slower than md5, and I dont think that is requried.

        Returns digestentry dict {datetime:datetime.now(), <digesttype>:digest }
        """
        logger.info("Experiment.hashFile() :: Not tested yet - take care ;)")
        if not os.path.isabs(filepath):
            filepath = os.path.normpath(os.path.join(self.Localdirpath, filepath))
        relpath = os.path.relpath(filepath, self.Localdirpath)
        fileshistory = self.Fileshistory
        digestentry = {digesttype: filehexdigest(filepath, digesttype) for digesttype in digesttypes}
        digestentry['datetime'] = datetime.now()
        if relpath in fileshistory:
            # if hexdigest is present, then no need to add it...? Well, now that you have hashed it, just add it anyways.
            #if hexdigest not in [entry[digesttype] for entry in fileshistory[relpath] if digesttype in entry]:
            fileshistory[relpath].append(digestentry)
        else:
            fileshistory[relpath] = [digestentry]
        return digestentry

    def saveFileshistory(self):
        """
        Persists fileshistory to file.
        """
        fileshistory = self.Fileshistory # This is ok; if _fileshistory is empty, it will try to reload to make sure not to override.
        if not fileshistory:
            logger.info("No fileshistory ('%s')for experiment '%s', aborting saveFileshistory.", fileshistory, self)
            return
        savetofolder = os.path.join(self.Localdirpath, '.labfluence')
        if not os.path.isdir(savetofolder):
            try:
                os.mkdir(savetofolder)
            except OSError as e:
                logger.warning(e)
                return
        fn = os.path.join(savetofolder, 'files_history.yml')
        yaml.dump(fileshistory, open(fn, 'wb'), default_flow_style=False)

    def loadFileshistory(self):
        """
        Loads the fileshistory from file.
        """
        if not self.Localdirpath:
            logger.warning("loadFileshistory was invoked, but experiment has no localfiledirpath. (%s)", self)
            return
        savetofolder = os.path.join(self.Localdirpath, '.labfluence')
        fn = os.path.join(savetofolder, 'files_history.yml')
        try:
            if self._fileshistory is None:
                self._fileshistory = dict()
            self._fileshistory.update(yaml.load(open(fn)))
            return True
        except (OSError, IOError, yaml.YAMLError) as e:
            logger.info("loadFileshistory error: %s", e)


    #####
    ##### Local files:
    #####

    def listLocalFiles(self, relative=None):
        """
        Lists all local files, essentially a lite version of getLocalFilelist
        that makes it clear that the task can be accomplished as a one-liner :-)
        Arg :relative: can be either a path, or a keyword
        'exp', 'local_exp_rootDir', 'local_exp_subDir'.
        Default (None) returns the same as as 'exp' (in getPathFor method).
        """
        if not self.Localdirpath:
            logger.info("No localdirpath? Is: %s", self.Localdirpath)
            return list()
        relstart = self.getPathFor(relative)
        return [os.path.relpath(os.path.join(dirpath, filename), relstart)
                for dirpath, _, filenames in os.walk(self.Localdirpath) for filename in filenames]

    def getLocalFilelist(self, fn_pattern=None, fn_is_regex=False, relative=None, subentries_only=True, subentry_idxs=None):
        """
        Returns a filtered list of local files in the experiment directory and sub-folders,
        filtering by:
        - fn_pattern
        - fn_is_regex   -> if True, will enterpret fn_pattern as a regular expression.
        - subentries_only -> only return files from subentry folders and not other files.
        - subentries_idxs -> only return files from from subentries with these subentry indices (sequence)

        The argument :relative: is used to return the filenames relative to a fixed dir (to truncate uninteresting parts).
        :relative: can also be a keyword ('exp', 'local_exp_subDir', 'local_exp_rootDir', 'filename-only').
        Default (None) is the same as 'exp'.

        # oneliner for listing files with os.walk:
        print("\n".join(u"{}:\n{}".format(dirpath, "\n".join(os.path.join(dirpath, filename) for filename in filenames))
                        for dirpath, dirnames, filenames in os.walk('.')))
        """
        ret = list()
        if not self.Localdirpath:
            return ret
        if subentry_idxs is None:
            subentry_idxs = list()
        relstart = self.getPathFor(relative)
        # I am not actually sure what is fastest, repeatedly checking "if include_prog and include_prog.match()
        if relative == 'filename-only':
            def file_repr(path, filename, relstart): # pylint: disable=W0613
                return filename
            def make_tuple(path, filename):
                return (filename, path, dict(fileName=filename, filepath=path))
        else:
            def file_repr(dirpath, filename, relstart):
                path = os.path.join(dirpath, filename)
                return os.path.join(os.path.relpath(path, relstart))
            def make_tuple(dirpath, filename):
                path = os.path.join(dirpath, filename)
                return (os.path.join(os.path.relpath(path, relstart)), path, dict(fileName=filename, filepath=path))
        if fn_pattern:
            if not fn_is_regex:
                # fnmatch translates into equivalent regex, offers the methods fnmatch, fnmatchcase, filter, re and translate
                fn_pattern = fnmatch.translate(fn_pattern)
            include_prog = re.compile(fn_pattern)
            def appendfile(dirpath, filename):
                # tuple format is (<list_repr>, <identifier>, <metadata>)
                if include_prog.match(filename):
                    ret.append( make_tuple(dirpath, filename ) )
        else:
            # alternative, just do if include_prog is None or include_prog.match(...)
            def appendfile(dirpath, filename):
                ret.append( make_tuple(dirpath, filename ) )

        if subentry_idxs or subentries_only:
            logger.debug("returning filelist using subentries...")
            if not self.Subentries:
                logger.warning("getLocalFilelist() :: subentries requested, but no subentries loaded, aborting.")
                return ret
            for idx, subentry in self.Subentries.items():
                if (subentries_only or idx in subentry_idxs) and 'foldername' in subentry:
                    # perhaps in a try-except clause...
                    for dirpath, dirnames, filenames in os.walk(os.path.join(self.Localdirpath, subentry['foldername'])):
                        for filename in filenames:
                            appendfile(dirpath, filename)
            return ret
        ignore_pat = self.Confighandler.get('local_exp_ignore_pattern')
        if ignore_pat:
            logger.debug("returning filelist by ignore pattern '%s'", ignore_pat)
            ignore_prog = re.compile(ignore_pat)
            for dirpath, dirnames, filenames in os.walk(self.Localdirpath):
                # http://stackoverflow.com/questions/18418/elegant-way-to-remove-items-from-sequence-in-python
                # remember to modify dirnames list in-place:
                #dirnames = filter(lambda d: ignore_prog.search(d) is None, dirnames) # does not work
                #dirnames[:] = filter(lambda d: ignore_prog.search(d) is None, dirnames) # works
                dirnames[:] = ( d for d in dirnames if ignore_prog.search(d) is None ) # works, using generator
                # alternatively, use list.remove() in a for-loop, but remember to iterate backwards.
                # or perhaps even better, iterate over a copy of the list, and remove items with list.remove().
                # if you can control the datatype, you can also use e.g. collections.deque instead of a list.
                logger.debug("filtered dirnames: %s", dirnames)
                for filename in filenames:
                    if ignore_prog.search(filename) is None:
                        appendfile(dirpath, filename)
                    else:
                        logger.debug("filename %s matched ignore_pat %s, skipping.", filename, ignore_pat)
        else:
            logger.debug("Experiment.getLocalFilelist() - no ignore_pat, filtering from complete filelist...")
            #return [(path, os.path.relpath(path) for dirpath,dirnames,filenames in os.walk(self.Localdirpath) for filename in filenames for path in (appendfile(dirpath, filename), ) if path]
            for dirpath, dirnames, filenames in os.walk(self.Localdirpath):
                for filename in filenames:
                    appendfile(dirpath, filename)
        logger.debug("Experiment.getLocalFilelist() :: Returning list: %s", ret)
        return ret




    ###
    ### Wiki-page related methods
    ###

    def downloadAttachment(self, filename, version=0, subentry=None):
        """
        Download attachment
        # NOTE: CONF-31169 and CONF-30024.
        # - attachment title ignored when adding attachment
        # - RemoteAttachment.java does not have a comment setter.
        """
        wikipage = self.WikiPage
        if wikipage is None:
            logger.error("Could not get wikipage, returning fake None to avoid failover; error was: %s")
            return None
        attdata = wikipage.getAttachmentData(filename, version)
        filedir = self.Localdirpath
        filepath = os.path.join(filedir, filename)
        with open(filepath, 'rb') as fd:
            fd.write(attdata.data)
        return attdata


    def uploadAttachment(self, filepath, att_info=None, digesttype='md5'):
        """
        Upload attachment to wiki page.
        Returns True if succeeded, False if failed and None if no attemt was made to upload due to a local Error.
        # NOTE: CONF-31169 and CONF-30024.
        # - attachment title ignored when adding attachment
        # - RemoteAttachment.java does not have a comment setter.
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
        """
        logger.warning("This method id not tested yet - take care ;)")
        wikipage = self.WikiPage
        if wikipage is None:
            logger.error("ERROR, no wikipage attached to this experiment object (%s)", self)
            return None
        if not os.path.isabs(filepath):
            filepath = os.path.normpath(os.path.join(self.Localdirpath, filepath))
        # path relative to this experiment, e.g. 'RS123d subentry_titledesc/RS123d_c1-grid1_somedate.jpg'
        attachmentInfo, attachmentData = attachmentTupFromFilepath(filepath)
        attachment = wikipage.addAttachment(attachmentInfo, attachmentData)
        #relpath = os.path.relpath(filepath, self.Localdirpath)
        #mimetype = getmimetype(filepath)
        ##attachmentInfo['contentType'] = mimetype
        ##attachmentInfo.setdefault('comment', os.path.basename(filepath) )
        ##attachmentInfo.setdefault('fileName', os.path.basename(filepath) )
        ##attachmentInfo.setdefault('title', os.path.basename(relpath) )
        #attachmentInfo.update(att_info)
        #if digesttype:
        #    digestentry = self.hashFile(filepath, (digesttype, ))
        #    # INFO: Setting attachment comment with xmlrpc does not currently work (confluence bug)
        #    attachmentInfo['comment'] = attachmentInfo.get('comment', '') \
        #        + "; {}-hexdigest: {}".format(digesttype, digestentry[digesttype])
        #with open(filepath, 'rb') as f:
        #    # To use an xmlrpclib.Binary object, just pass the bytes during init. (Can also be loaded afterwards)
        #    attachmentData = xmlrpclib.Binary(f.read())# as seen in https://confluence.atlassian.com/display/DISC/Upload+attachment+via+Python+XML-RPC
        #
        return attachment


    def getAttachments(self):
        """
        Lists attachments on the wiki page.
        Returns a list of attachments (structs) if succeeded, and empty list if failed.
        This method is used by self.Attachments property. If you require an updated list,
        do not use this method directly, but instead use getUpdatedAttachmentsList()
        which will update the cache.
        The returned attachment-struct dict has the following entries:
        - comment (string, required)
        - contentType (string, required)
        - created (date)
        - creator (string username)
        - fileName (string, required)
        - fileSize (string, number of bytes)
        - id (string, attachmentId)
        - pageId (string)
        - title (string)
        - url (string)
        """
        wikipage = self.WikiPage
        if wikipage is None:
            logger.error("Could not get wikipage, returning fake new list() to avoid failover.")
            return list()
        attachment_structs = wikipage.getAttachments()
        if attachment_structs is None:
            logger.info("exp.WikiPage.getAttachments() returned None, likely because the server it not connected,\
                         returning fake empty list to avoid fail.")
            return list()
        return attachment_structs


    def getAttachmentList(self, fn_pattern=None, fn_is_regex=False, **filterdict):
        """
        The wiki-attachments equivalent to getLocalFileslist(),
        Returns a tuple list of (<display>, <identifier>, <complete struct>) elements.
        Like getLocalFileslist, the returned list can be filtered based on
        filename pattern (glob; or regex if fn_is_regex is True).
        The filterdict kwargs are currently not used.
        However, when needed, this could be used to filter the returned list based on
        attachment metadata, which includes:
        - comment (string)
        - contentType (string)
        - created (date)
        - creator (string username)
        - fileName (string, required)
        - fileSize (string, number of bytes)
        - id (string, attachmentId)
        """
        struct_list = self.Attachments
        if not struct_list:
            return list()
        # Returned tuple of (<display>, <identifier>, <complete struct>)
        # I think either filename or id would work as identifier.
        if fn_pattern:
            if not fn_is_regex:
                fn_pattern = fnmatch.translate(fn_pattern)
            regex_prog = re.compile(fn_pattern)
        else:
            regex_prog = None
        # attachment struct_list might be None or False, so must check before trying to iterate:
        return [ (struct['fileName'], struct['id'], struct) for struct in struct_list \
                    if regex_prog is None or regex_prog.match(struct['fileName']) ]
