#!/usr/bin/env python 
# -*- coding: utf-8 -*-

import logging
import pprint
import collections
import xml.dom.minidom

from manifest.libmets import fileGrpType, fileType, divType


################################################################################
# Manifest comparator Class #
################################################################################
class MetsManifestComparator():

    STRUCT_MAP_CHANGES = "STRUCT_MAP_CHANGES"
#    FILE_SEC_CHANGES = "FILE_SEC_CHANGES"
    
    ADDED_LOGICAL_COLLECTION = "added_new_logical_collection_"
    DELETED_LOGICAL_COLLECTION = "deleted_logical_collection_"
    
    ADDED_DIVS = "added_divs"
    DELETED_DIVS = "deleted_divs"
    LOGICAL_COLLECTION_CHANGE = "changes_in_logical_collection_"
    
    ADDED_DEFAULT_DIV = "added_default_div_"
    DELETED_DEFAULT_DIV = "deleted_default_div_"

    def __init__(self, configuration, logger):
        
        self.logger = logger
        self.debug = configuration.debug
        self.dryrun = configuration.dryrun
        self.log_level = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG, \
                          'ERROR': logging.ERROR, 'WARNING': logging.WARNING, \
                          'CRITICAL': logging.CRITICAL}
        loglevel = 'INFO'
        if self.debug:
            loglevel = 'DEBUG'
        self.logger.setLevel(self.log_level[loglevel])


    def compareMetsManifestFiles(self, oldmets, newmets):
        self.logger.debug('Begin comparing manifest files')
        changes = {}
        
        #fileSec changes are not needed for now
        #changesInFileSec = self.compareFileSecInfo(oldmets.fileSec, newmets.fileSec)
        #changes[self.FILE_SEC_CHANGES] = changesInFileSec
        
        changesInLogicCollections = self.compareStructMapInfo(oldmets.structMap, newmets.structMap)
        changes[self.STRUCT_MAP_CHANGES] = changesInLogicCollections
        
        #changes = changesInFileSec.copy()
        #changes.update(changesInLogicCollections)
        
        return changes

    def compareStructMapInfo(self, oldStructMap, newStructMap):
        oldlLogicalCollections = {}
        oldDefaultDivs = {}
        
        self.contentsFromStructMap(oldStructMap, oldlLogicalCollections, oldDefaultDivs)

        newLogicalCollections = {}
        newDefaultDivs = {}
        
        self.contentsFromStructMap(newStructMap, newLogicalCollections, newDefaultDivs)
        
        changes = {}
        self.createChangesFromLogicalCollections(oldlLogicalCollections, newLogicalCollections, changes)
        self.logger.debug('Changes related to logical collections: ' + pprint.pformat(changes))
        self.createChangesFromDefaultDivs(oldDefaultDivs, newDefaultDivs, changes)
        self.logger.debug('Changes related to default divs: ' + pprint.pformat(changes))
        
        return changes
    
    def contentsFromStructMap(self, oldStructMap, oldlLogicalCollections, oldDefaultDivs):
        for smap in oldStructMap:
            mainDiv = smap.div
            for innerDiv in mainDiv.content() :
                if innerDiv.TYPE == "entityRelation":
                    #name = self.getCollectionNameFrom(innerDiv.LABEL)
                    name = innerDiv.LABEL
                    oldlLogicalCollections[name] = innerDiv
                else:
                    oldDefaultDivs[self.createIdFrom(innerDiv)] = innerDiv
    
    def createIdFrom(self, div):
        fptr_id = ""
        if len(div.fptr) == 1:
            for fptr_element in div.fptr:
                fptr_id = fptr_element.FILEID
        else:
            print "Could not extract div id from div: "+ div.LABEL
        return fptr_id
    
    def getCollectionNameFrom(self, label):
        li = label.split('_')
        collectionName = ""
        for x in range(0, len(li)-1):
            if collectionName == "":
                collectionName = li[x]
            else:
                collectionName = collectionName + '_' + li[x]
        return collectionName
    
    def createChangesFromLogicalCollections(self, oldlLogicalCollections, newLogicalCollections, changes):
        deletedLogCollections = oldlLogicalCollections.keys()
        for key in newLogicalCollections.keys():
            if key not in oldlLogicalCollections.keys():
                changes[self.ADDED_LOGICAL_COLLECTION+key] = newLogicalCollections[key]
                #create node for collection _createUniqueNode Aggregation
            else:
                deletedLogCollections.remove(key)
                self.createChangesForCollection(oldlLogicalCollections[key], newLogicalCollections[key], changes)
        
        for key in deletedLogCollections:
            #for deletedDivs DETACH DELETE n, Delete a node and all relationships connected to it.
            changes[self.DELETED_LOGICAL_COLLECTION+key] = oldlLogicalCollections[key]
    
    def createChangesForCollection(self, oldlLogicalCollection, newLogicalCollection, changes):
        oldMap = self.createMapFromDiv(oldlLogicalCollection)
        newMap = self.createMapFromDiv(newLogicalCollection)
        deletedDivkeys = oldMap.keys()
        
        addedDivs = {}
        deletedDivs = {}
        for key in newMap.keys():
            if key not in oldMap.keys():
                addedDivs[key] = newMap[key]
            else:
                deletedDivkeys.remove(key)
        
        for key in deletedDivkeys:
            deletedDivs[key] = oldMap[key]
        
        innerChanges = {}
        innerChanges[self.ADDED_DIVS] = addedDivs
        innerChanges[self.DELETED_DIVS] = deletedDivs
        changes[self.LOGICAL_COLLECTION_CHANGE+oldlLogicalCollection.LABEL] = innerChanges
    
    def createMapFromDiv(self, div):
        mapFromDiv = {}
        for entry in div.content():
            if type(entry) == divType:
                mapFromDiv[self.createIdFrom(entry)] = entry
            else:
                print "Invalid format"
        
        return mapFromDiv

    def createChangesFromDefaultDivs(self, oldDefaultDivs, newDefaultDivs, changes):
        deletedDivIds = oldDefaultDivs.keys()
        
        for newkey in newDefaultDivs.keys():
            if newkey not in oldDefaultDivs.keys():
                changes[self.ADDED_DEFAULT_DIV+newkey] = newDefaultDivs[newkey]
            else:
                deletedDivIds.remove(newkey)
        
        for key in deletedDivIds:
            changes[self.DELETED_DEFAULT_DIV+key] = oldDefaultDivs[key]

    def leafToString(self, d):
        for k, v in d.iteritems():
            if isinstance(v, collections.Mapping):
                r = self.leafToString(v)
                d[k] = r
            elif (isinstance(v, fileGrpType)
                  or isinstance(v, fileType)
                  or isinstance(v, divType)):
                dom = xml.dom.minidom.parseString(v.toxml("utf-8"))
                d[k] = dom.toxml("utf-8")
            else:
                d[k] = str(v)
        return d
    
    def compareFileSecInfo(self, oldFileSec, newFileSec):
        oldFilesAndDirectories = self.getFilesAndDirectories(oldFileSec.fileGrp)
        newFilesAndDirectories = self.getFilesAndDirectories(newFileSec.fileGrp)
        
        fileSecChanges = {};
        self.compareFiles(oldFilesAndDirectories, newFilesAndDirectories, fileSecChanges);
        self.logger.debug('Compared files - results: ' + pprint.pformat(fileSecChanges))
        self.compareFolders(oldFilesAndDirectories, newFilesAndDirectories, fileSecChanges);
        self.logger.debug('Compared folders - results: ' + pprint.pformat(fileSecChanges))
        
        return fileSecChanges
    
    def compareFiles(self, oldFilesAndDirectories, newFilesAndDirectories, fileSecChanges):  
        newFiles = newFilesAndDirectories["files"];
        oldFiles = oldFilesAndDirectories["files"];

        deletedFileIDs = list(oldFiles.keys())
        addedFilesSet = {}
        for newFileID in newFiles.keys():
            if newFileID not in oldFiles.keys() :
                addedFilesSet[newFileID] = newFiles.get(newFileID)
            else:
                deletedFileIDs.remove(newFileID)
        
        deletedFilesSet = {}
        for deletedFileId in deletedFileIDs:
            deletedFilesSet[deletedFileId] = oldFiles.get(deletedFileId)
        
        fileSecChanges["added files:"] = addedFilesSet
        fileSecChanges["deleted files:"] = deletedFilesSet
    
    def compareFolders(self, oldFilesAndDirectories, newFilesAndDirectories, fileSecChanges):
        newDirectories = newFilesAndDirectories["directories"];
        oldDirectories = oldFilesAndDirectories["directories"];

        deletedDirectoriesIDs = list(oldDirectories.keys())
        addedDirectories = {}
        for newDirectoryID in newDirectories.keys():
            if newDirectoryID not in oldDirectories.keys():
                addedDirectories[newDirectoryID] = newDirectories.get(newDirectoryID)
            else:
                deletedDirectoriesIDs.remove(newDirectoryID)
        
        deletedDirectories = {}
        for deletedDirectoryId in deletedDirectoriesIDs:
            deletedDirectories[deletedDirectoryId] = oldDirectories.get(deletedDirectoryId)
        
        fileSecChanges["added directories:"] = addedDirectories
        fileSecChanges["deleted directories:"] = deletedDirectories

    def getFilesAndDirectories(self, groups):
        resultDict = {}
        
        for fileGrpElement in groups:
            if fileGrpElement.ID is None:
                conten = fileGrpElement.content()
                if len(conten) != 1:
                    #throw new RuntimeException("Invalid Input file.");
                    print "Invalid Input file."
                fileGrpElement = conten[0]
            
            directories = {}
            files = {}
            
            if (fileGrpElement.ID is not None) & (len(fileGrpElement.content()) != 0):
                self.recursiveGetFilesAndFolders(fileGrpElement, directories, files)
            
            resultDict["directories"] = directories
            resultDict["files"] = files
        
        
        return resultDict

    def recursiveGetFilesAndFolders(self, fileSystemEntity, directories, files):
        if type(fileSystemEntity) == fileType:
            files[fileSystemEntity.ID] = fileSystemEntity
        else: 
            if type(fileSystemEntity) == fileGrpType:
                directories[fileSystemEntity.ID] = fileSystemEntity
                for entry in fileSystemEntity.content():
                    self.recursiveGetFilesAndFolders(entry, directories, files)
            else:
                print "EROR: unknown mets element found"
