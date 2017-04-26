#!/usr/bin/env python 
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import logging

from manifest.libmets import fileGrpType, fileType, CreateFromDocument
from manifest import IRODSUtils
import manifest

from b2safe_neo4j_client_new import Configuration

logger = logging.getLogger('MetsManifestComparator')
logger.setLevel(logging.INFO)
logfilepath = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 
                           'manifestComparator.log')
rfh = logging.handlers.RotatingFileHandler(logfilepath, \
                                           maxBytes=10000000, \
                                           backupCount=10)
formatter = logging.Formatter('%(asctime)s %(levelname)s: '
                              + '[%(funcName)s] %(message)s')
rfh.setFormatter(formatter)
logger.addHandler(rfh)

class MetsManifestValidator():
    
    def __init__(self, irods_home_path, debug, dryrun, logger):
        self.irods_home_path = irods_home_path
        self.logger = logger
        self.debug = debug
        self.dryrun = dryrun
        self.log_level = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG, \
                          'ERROR': logging.ERROR, 'WARNING': logging.WARNING, \
                          'CRITICAL': logging.CRITICAL}
        loglevel = 'INFO'
        if self.debug:
            loglevel = 'DEBUG'
        self.logger.setLevel(self.log_level[loglevel])
        
    def recursiveGetFilesAndFolders(self, fileSystemEntity, directories, files):
        if type(fileSystemEntity) == fileType:
            files[fileSystemEntity.ID] = fileSystemEntity
        else: 
            if type(fileSystemEntity) == fileGrpType:
                if not "__files__" in fileSystemEntity.ID:
                    directories[fileSystemEntity.ID] = fileSystemEntity
                for entry in fileSystemEntity.content():
                    self.recursiveGetFilesAndFolders(entry, directories, files)
            else:
                print "EROR: unknown mets element found"
                
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

    def getIrodsFilesRec(self, files, path, map):
        for key, val in map.iteritems():
            if "__files__" in key:
                filePrefix = path+"/"
                filePrefix = "_"+filePrefix[self.irods_home_path.rindex("/")+1:]
                for f in val:
                    files.append((filePrefix+f).replace("/","_"))
            else:
                self.getIrodsFilesRec(files, key, val)

    def getIrodsFiles(self, irodsFilesMap):
        files = []
        
        self.getIrodsFilesRec(files, None, irodsFilesMap)
        
        return files
        
    def createFileIdListRec(self, fileIdList, div):
        if hasattr(div, "fptr"):
            for fptr_element in div.fptr:
                fileIdList.append(fptr_element.FILEID)
        if div.content:
            for innerDiv in div.content() :
                self.createFileIdListRec(fileIdList, innerDiv)
        
    def createFileIdList(self, smaps):
        fileIdList=[]
        for smap in smaps:
            self.createFileIdListRec(fileIdList, smap.div)
        return fileIdList
    
    def validateManifestConsistency(self, files, mets_from_manifest):
        #itter over strucktMap get the files and loock if the file is in the fileSec
        smaps = mets_from_manifest.structMap
        
        if (len(smaps) > 1 | len(smaps) <= 0):
            return False

        fileIdList = self.createFileIdList(smaps)
        
        for id in fileIdList:
            if id not in files.keys():
                return False
        
        return True
    
    def validateFilesExistence(self, files, irodsFiles):
        for file in files:
            if file not in irodsFiles:
                return False
        return True
    
    def validateMetsManifestFile(self, mets_from_manifest, irodsFilesMap):
        self.logger.debug('Begin comparing manifest files')
        
        #collect all files from fileSec
        fileSec = mets_from_manifest.fileSec
        filesAndDirectories = self.getFilesAndDirectories(fileSec.fileGrp)
        
        files = filesAndDirectories["files"]
        
        isConsistent = self.validateManifestConsistency(files, mets_from_manifest)
        
        allFilesExisting = None
        
        if isConsistent:
            irodsFiles = self.getIrodsFiles(irodsFilesMap)
            allFilesExisting = self.validateFilesExistence(files, irodsFiles)
        
        return isConsistent, allFilesExisting

def executeValidation(args):
 
    logger.info ('Starting manifest comparison')
    
    configuration = Configuration(args.confpath, args.debug, args.dryrun, logger)
    configuration.parseConf();
    irodsu = manifest.IRODSUtils(configuration.irods_home_dir, logger, configuration.irods_debug)
    if args.user:
        irodsu.setUser(args.user[0])
        
    xmltext = irodsu.getFile(args.path + '/manifest.xml')
    mets_from_manifest = CreateFromDocument(xmltext)
    
    irodsFilesMap = irodsu.deepListDir(args.path)
    
    validator = MetsManifestValidator(args.path, args.debug, args.dryrun, logger)
    isValid = validator.validateMetsManifestFile(mets_from_manifest, irodsFilesMap[1])
        
    print("Is a valid manifest: "+str(isValid))
        
    logger.info('Manifest validation completed') 
 
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='B2SAFE manifest validation')
    parser.add_argument("-confpath", help="path to the configuration file")
    parser.add_argument("-dbg", "--debug", action="store_true", 
                        help="enable debug")
    parser.add_argument("-d", "--dryrun", action="store_true",
                        help="run without performing any real change")
    
    parser.add_argument("-path", help="irods path to the data")
    
    parser.add_argument("-u", "--user", nargs=1, help="irods user")
     
    parser.set_defaults(func=executeValidation) 
 
    args = parser.parse_args()
    args.func(args)