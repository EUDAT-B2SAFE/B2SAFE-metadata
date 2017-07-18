#!/usr/bin/env python 
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import logging
import datetime
import time
import tempfile
import xml.dom.minidom

from manifest.libmets import fileGrpType, fileType, CreateFromDocument
from manifest import IRODSUtils

from b2safe_neo4j_client_new import Configuration

logger = logging.getLogger('MetsManifestValidator')
logger.setLevel(logging.INFO)
logfilepath = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), 
                           'metsManifestValidator.log')
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
                logger.error("unknown mets element found")
                
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

    def getIrodsFilesRec(self, files, path, irodsFilesMap):
        for key, val in irodsFilesMap.iteritems():
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
            if len(div.content()) > 1:
                for innerDiv in div.content() :
                    self.createFileIdListRec(fileIdList, innerDiv)
        
    def createFileIdList(self, smaps):
        fileIdList=[]
        for smap in smaps:
            self.createFileIdListRec(fileIdList, smap.div)
        return fileIdList
    
    def validateManifestConsistency(self, files, mets_from_manifest):
        #iterate over strucktMap get the files and look if the file is in the fileSec
        smaps = mets_from_manifest.structMap
        
        if (len(smaps) > 1 | len(smaps) <= 0):
            return False

        fileIdList = self.createFileIdList(smaps)
        
        missingFilesIDs = []
        for file_id in fileIdList:
            if file_id not in files.keys():
                missingFilesIDs.append(file_id)
        
        return missingFilesIDs
    
    def validateFilesExistence(self, files, irodsFiles):
        notExistingFiles = []
        for fileURL in files:
            if fileURL not in irodsFiles:
                notExistingFiles.append(fileURL)
        return notExistingFiles
    
    def validateMetsManifestFile(self, mets_from_manifest, irodsFilesMap):
        self.logger.debug('Begin comparing manifest files')
        
        #collect all files from fileSec
        fileSec = mets_from_manifest.fileSec
        filesAndDirectories = self.getFilesAndDirectories(fileSec.fileGrp)
        
        files = filesAndDirectories["files"]
        missingFilesIDs = self.validateManifestConsistency(files, mets_from_manifest)
        
        isConsistent = True
        if missingFilesIDs:
            isConsistent = False
        
        if isConsistent:
            irodsFiles = self.getIrodsFiles(irodsFilesMap)
            notExistingFiles = self.validateFilesExistence(files, irodsFiles)
        
        return missingFilesIDs, notExistingFiles

def executeValidation(args):
    logger.info ('Starting manifest comparison at: '+ datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'))
    collectionPath = args.path.rsplit('/',1)[0]
    manifestPath = args.path
    
    configuration = Configuration(args.confpath, args.debug, args.dryrun, logger)
    configuration.parseConf();
    irodsu = IRODSUtils(configuration.irods_home_dir, logger, configuration.irods_debug)
    if args.user:
        irodsu.setUser(args.user[0])
    
    xmltext = irodsu.getFile(manifestPath)
    mets_from_manifest = CreateFromDocument(xmltext)
    
    irodsFilesMap = irodsu.deepListDir(collectionPath)
    
    validator = MetsManifestValidator(collectionPath, args.debug, args.dryrun, logger)
    validationResults = validator.validateMetsManifestFile(mets_from_manifest, irodsFilesMap[1])
    
    missingFilesIDs = validationResults[0]
    notExistingFiles = validationResults[1]
    
    #TODO: decide what is the best way to store the validation results
    irodsu.assing_metadata(manifestPath, "VALIDATION_STATUS", "COMPLETED")
    irodsu.assing_metadata(manifestPath, "IS_CONSISTENT", str(len(validationResults[0]) > 0))
    irodsu.assing_metadata(manifestPath, "ALL_FILES_EXISTING", str(len(validationResults[1]) > 0))
    
    logger.info("IS_CONSISTENT: "+str(len(missingFilesIDs) > 0))
    logger.info("Missing files for ID's: "+str(missingFilesIDs))   
    logger.info("ALL_FILES_EXISTING: "+str(len(notExistingFiles) > 0))
    logger.info("Missing files with path's: "+str(notExistingFiles))
    
    manifestXML = setValidationStatusInMetsHdr(xmltext, "VALIDATED")
    
    if args.dryrun:
        print(manifestXML)
    else:
        logger.info('Writing the manifest to a file')
        
        temp = tempfile.NamedTemporaryFile()
        try:
            temp.write(manifestXML)
            temp.flush()
            logger.info('in the irods namespace: {}'.format(manifestPath))
            try: 
                irodsu.putFile(temp.name, manifestPath, configuration.irods_resource)
                #irodsu.putFile(temp.name, manifestPath+"_"+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'), configuration.irods_resource)
            except:
                out = irodsu.putFile(temp.name, manifestPath)
                #out = irodsu.putFile(temp.name, manifestPath+"_"+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'))
                print(str(out))
        finally:
            temp.close()
                
    logger.info('Manifest validation completed at: '+ datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'))
    
def setValidationStatusInMetsHdr(xmltext, status):
    dom = xml.dom.minidom.parseString(xmltext)
    metsHdr = dom.getElementsByTagName("ns1:metsHdr")
    if not metsHdr:
        rootElem = dom.getElementsByTagName("ns1:mets")[0]
        metsHdr = dom.createElementNS(rootElem.namespaceURI, "ns1:metsHdr")
        rootElem.insertBefore(metsHdr, rootElem.firstChild)
    else:
        metsHdr[0].setAttribute("RECORDSTATUS", status)
    manifestXML = dom.toprettyxml(encoding="utf-8")
    return manifestXML

#python validate_mets_manifest.py -dbg -conf conf/b2safe_neo4j.conf -path /JULK_ZONE/home/irods/julia/collection_A/EUDAT_manifest_METS.xml
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='B2SAFE manifest validation')
    parser.add_argument("-confpath", help="path to the configuration file")
    parser.add_argument("-dbg", "--debug", action="store_true", 
                        help="enable debug")
    parser.add_argument("-d", "--dryrun", action="store_true",
                        help="run without performing any real change")
    
    parser.add_argument("-path", help="irods path to the manifest to validate")
    
    parser.add_argument("-u", "--user", nargs=1, help="irods user")
     
    parser.set_defaults(func=executeValidation) 
 
    args = parser.parse_args()
    args.func(args)