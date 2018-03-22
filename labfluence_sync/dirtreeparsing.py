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
"""

Utility functions for parsing directory trees.

"""

from __future__ import print_function
from six import string_types
import os
import logging
logger = logging.getLogger(__name__)




def getFolderschemeUpTo(folderscheme, rightmost):
    """
    Lets say that folderscheme is './year/experiment/subentry'.
    However, I only want the part that includes './year/experiment',
    in order to get the experiment folders.
    Does not include trailing '/'.
    """
    schemekeys = [elem for elem in folderscheme.split('/') if elem] # do not include empty
    left = "/".join(schemekeys[:schemekeys.index(rightmost)+1])
    return left



def genPathmatchTupsByPathscheme(basepath, folderscheme, regexs,
                                 filterfun=None, matchcombiner=None, matchinit=None,
                                 rightmost=None, fs=None):
    """
    Args:
        :basepath:      Where to start, e.g. '/User/me/experiments/'
        :folderscheme:  Specifies how the files are organized, e.g. './year/experiment/subentry'
                        The result only returns tuples for the elements at the deepest/rightmost level
                        of the pathscheme, e.g. subentries in the example above.
        :regexs:        A dict with keys matching the 'schemekeys' in the pathscheme,
                        e.g. 'year', 'experiment', and 'subentry' in the example above.
                        The dict values must be compiled regex programs.
        :filterfun:     A function that determines whether the path is included in the result. Default is self.isdir.
        :matchcombiner: Can be used control what is returned as the second item in the two-tuples:
                        (path, matchcombiner(basematch, schemekey, match))
                        The default is to return a dict with schemekeys: match-object, i.e.:
                            matchcombiner = lambda basematch, schemekey, match: dict(basematch, **{schemekey: match})
        :matchinit:     Initial basematch when starting recursion. Alternative to having matchcombiner
                        account for a None start value.
        :rightmost:     Convenience parameter to truncate the folderscheme, e.g. with rightmost='experiment'
                        the folderscheme above is converted to './year/experiment'
        :fs:            The filesystem module to use. By default, this is just the 'os' standard python module.


    Edits/Changelog:
        The includefiles argument has been removed in favor of the more flexible filterfun functional argument.
        (:includefiles: If set to True, the files (and not just folders) are also included in the result)

        Introduced :matchcombiner: argument to control what is returned as the second item in the two-tuples:
            (path, matchcombiner(basematch, schemekey, match))

    Returns a sequnce/generator of two-item 'matchtuples':
        (folderpath, dict-of-regex-matches)
    where each match-items-dict has keys matching each scheme item in folderscheme
    and each value is a regex match found during traversal at that scheme level.

    Usage:
        >>> regexs={'year': r'(?P<year>[0-9]{4})',
                    'experiment': r'(?P<expid>RS[0-9]{3})[_ ]+(?P<exp_titledesc>.+)',
                    'subentry': r'(?P<expid>RS[0-9]{3})(?P<subentry_idx>[a-Z])[_ ]+(?P<subentry_titledesc>.+)'}
        >>> genPathmatchTupsByPathscheme(regexs=regexs,
                                         basepath='/mnt/data/nanodrop/',
                                         folderscheme='./year/experiment/subentry')
        (returns generator with two-tuples similar to
            ('/mnt/data/nanodrop/2014/RS123 My experiment/RS123a subentryA':
             {'year': re.Match(year='2014'),
              'experiment': re.Match(expid='RS123', exp_titledesc='My experiment')
              'subentry': re.Match(expid='RS123', subentry_idx='a', subentry_titledesc='subentryA')})

    Discussions:
        I was considering returning a single, combined match dict, i.e. tuples similar to:
            ('/mnt/data/nanodrop/2014/RS123 My experiment/RS123a subentryA':
            {'year': '2014', 'exp_titledesc': 'My experiment', 'expid': 'RS123', \
             'subentry_idx': 'a', 'subentry_titledesc': 'subentryA'))

        Here, matches for later/deeper elements would overwrite previous match dict entries.
        In the example above, if path had been '/mnt/data/nanodrop/2014/RS123 My experiment/RS124a subentryA'
        then the resulting dict would include 'expid': 'RS124' for the /subentry/ level match.

    While the result might be simpler, this would also cause a loss of information, so I decided to include all matches.
    Producing a single dict should be trivial for the consumer.

    Performance:
        For a very deep pathscheme of 'year/experiment/subentry/filename'
            {'year': <year match>, 'experiment': <exp-match>, 'subentry': <subentry-match>, 'filename' : <fn-match>}
        (Usually, you do not want to include <filename> in the folderscheme,
         but perhaps parse that separately if required...)

    Question: Do you save for all levels, or only for the final part? --> Only the last part. <--

    """
    if rightmost is not None:
        folderscheme = getFolderschemeUpTo(folderscheme, rightmost)
    if fs is None:
        fs = os
    if filterfun is None:
        filterfun = fs.path.isdir
    schemekeys = [key for key in folderscheme.split('/') if key and key != '.'] \
                 if isinstance(folderscheme, string_types) else folderscheme
    logger.debug("genPathmatchTupsByPathscheme invoked with, regexs=%s, basepath=%r, folderscheme=%r, filterfun=%s",
                 regexs, basepath, folderscheme, filterfun)

    def default_matchcombiner(basematch, schemekey, match):
        """
        Make a shallow copy of basematch and add schemekey=match to it.
        Note: This might mot be pypy compatible if schemekey is not a string.
        Creating dict copies should not be a big memory issue, especially within a generator.
        And, since the pathscheme should only go two maybe three steps deep,
        recursing shouldn't be an issue either.
        """
        if basematch is None:
            basematch = {}
        return dict(basematch, **{schemekey: match})
    if matchcombiner is None:
        matchcombiner = default_matchcombiner

    def genitems(schemekeys, basefolder, basematch=None):
        """
        Recursively traverse the directory tree from basefolder according to scheme keys.
        Args:
            :schemekeys: Are the remaining items in the pathscheme, starting at basefolder.
            :basefolder: Is the current folder from where we transverse recursively.
            :basematch:  Is the match object of the parent, as produced by matchcombiner.

        For the default matchcombiner, if pathscheme is
            ./year/experiment/subentry and we are at ./2013/RS160.../
        then basematch will be a dict : {'year': <year match>, 'experiment': <exp match>}.

        Returns a sequnce/generator of two-item 'matchtuples' for each element* in basefolder
        (that passes filterfun check and matches the current schemekey's regex):
            (folderpath, match-structure)
        Where the match-structure is created by matchcombiner functional argument.
        """
        schemekey, remainingschemekeys = schemekeys[0], schemekeys[1:] # slicing does not raise indexerrors:
        regexpat = regexs[schemekey]

        ## Make initial (path, match) generator. Hard to factor out because of basematch and schemekey
        # Produce list of folders and/or files:
        foldernames = (foldername for foldername in fs.listdir(basefolder)
                       if fs.path.isdir(basefolder) and filterfun(fs.path.join(basefolder, foldername)))
        # Make tuples with folder path and regex match
        pathmatchtup = ((fs.path.join(basefolder, foldername), regexpat.match(foldername))
                        for foldername in foldernames)
        # Filter out non-matches and create result with matchcombiner.
        foldertups = ((folderpath, matchcombiner(basematch, schemekey, match))
                      for folderpath, match in pathmatchtup if match)

        """
        # This is the part that actually produces the flat/linear two-tuple output.
        # If, instead of simply having subfoldertup, you had ((folderpath, matchdict), subfoldertup),
        # You would get outputs a ((('.../2014', year-match), # All matches under 2014 folder:
        #                           ((('.../2014/RS123 Exp', exp-match), # All matches under RS123 folder:
        #                             ((('.../2014/RS123 Exp/RS123a SubA', sub-match), None),
        #                              (('.../2014/RS123 Exp/RS123a SubB', sub-match), None))),
        #                            (('.../2014/RS125 Exp', exp-match),
        #                             ((('.../2014/RS125 Exp/RS125a Suba', sub-match), None),
        #                              (('.../2014/RS125 Exp/RS125a Subb', sub-match), None))),
        #                           )
        #Which could be unravelled with:
        #for (yearpath, yearmatch), experiments in output:
        #    for (exppath, expmatch), subentries in experiments:
        #        for (subpath, submatch), files in subentries:
        #            # Do something, e.g.
        #            subfoldersbyexpidsubexpid[expmatch['expid'][submatch['subid']]] = subpath
        #But the point is that you wouldn't *have* to recurse the subentries...
        #And if you wanted to save, you could just do ((folderpath, matchdict), list(subfoldertup))
        #Still, that's kind of exsotic.
        """
        if remainingschemekeys:
            # Recurse into subfolders:
            matchitems = (subfoldertup
                          for folderpath, matchdict in foldertups
                          for subfoldertup in genitems(remainingschemekeys, folderpath, matchdict))
            #logger.debug("Received matching items from remainingschemekeys: %s", len(matchitems))
        else:
            #logger.debug("No remaining items, returning foldertups at this level.")
            matchitems = foldertups
        return matchitems

    # Outer function
    # foldermatchtups = genitems(schemekeys, basepath, matchinit)
    return genitems(schemekeys, basepath, matchinit)



def genPathGroupdictTupByPathscheme(basepath, folderscheme, regexs,
                                    fs=None, filterfun=None, rightmost=None):
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
    return genPathmatchTupsByPathscheme(basepath=basepath, folderscheme=folderscheme, regexs=regexs, fs=fs,
                                        filterfun=filterfun, matchcombiner=matchcombiner,
                                        matchinit={}, rightmost=rightmost)


def makeFolderByMatchgroupForScheme(group, basepath, folderscheme, regexs,
                                    fs=None, filterfun=None, rightmost=None):
    """
    Like satellite_location.getExpfoldersByExpid.
    Note: If group is a list/tuple, then the returned dict is keyed by corresponding keys,
    e.g. dict[(expid, subidx)] = subentry_path
    """
    foldermatchtuples = genPathGroupdictTupByPathscheme(basepath, folderscheme, regexs,
                                                        fs=fs, filterfun=filterfun, rightmost=rightmost)
    if isinstance(group, (tuple, list)):
        foldersbyexpid = {tuple(gd.get(g) for g in group): path for path, gd in foldermatchtuples}
    else:
        foldersbyexpid = {gd.get(group): path for path, gd in foldermatchtuples}
    return foldersbyexpid


def getFoldersWithSameProperty(group, basepath, folderscheme, regexs,
                               fs=None, filterfun=None, rightmost=None, countlim=1):
    """
    Returns a dict with list of paths for folders with duplicate match group values.
    Set countlim=2 to only get duplicates.
    """
    foldermatchtuples = genPathGroupdictTupByPathscheme(basepath, folderscheme, regexs,
                                                        fs=fs, filterfun=filterfun, rightmost=rightmost)
    listfoldersbyexp = {}
    if isinstance(group, (tuple, list)):
        def groupgetter(match):
            """ Return tuple (list is not hashable) """
            return tuple(match[g] for g in group)
    else:
        def groupgetter(match):
            " Just return group "
            return match[group]
    for folderpath, matchdict in foldermatchtuples:
        listfoldersbyexp.setdefault(groupgetter(matchdict), []).append(folderpath)
    # Create new dict, where we only have elements with more than one, i.e. where we actually have duplicates:
    if countlim:
        listfoldersbyexp = {expid: folderlist for expid, folderlist in listfoldersbyexp.items() if len(folderlist) >= countlim}
    return listfoldersbyexp
