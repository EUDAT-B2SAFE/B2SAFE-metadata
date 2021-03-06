#!/usr/bin/env python 
# -*- coding: utf-8 -*-

import argparse
import os
import sys
import logging
import datetime
import time
#import tempfile
import xml.dom.minidom
import pyxb
import traceback
from xml import sax

from manifest.libmets import fileGrpType, fileType, CreateFromDocument
from manifest import IRODSUtils

import ConfigParser
#from b2safe_neo4j_client import Configuration

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
        
    def recursiveGetFiles(self, fileEntity, groupID, files):
        if type(fileEntity) == fileType:
            fileName = self.getFileNameFromID(fileEntity.ID)
            files[fileEntity.ID] = groupID + fileName
        else: 
            if type(fileEntity) == fileGrpType:
                for entry in fileEntity.content():
                    if "__files__" in fileEntity.ID:
                        self.recursiveGetFiles(entry, groupID, files)
                    else: 
                        self.recursiveGetFiles(entry, groupID+self.getFileNameFromID(fileEntity.ID), files)
            else:
                logger.error("unknown mets element found")
                
    def getFiles(self, groups):
        for fileGrpElement in groups:
            if fileGrpElement.ID is None:
                conten = fileGrpElement.content()
                if len(conten) != 1:
                    #throw new RuntimeException("Invalid Input file.");
                    print "Invalid Input file."
                fileGrpElement = conten[0]
            
            files = {}
            if (fileGrpElement.ID is not None) & (len(fileGrpElement.content()) != 0):
                self.recursiveGetFiles(fileGrpElement, "", files)
            
        return files
    
    def getFileNameFromID(self, FileID):
            fileName = FileID.rsplit('_',1)[0]
            return fileName

    def getIrodsFilesRec(self, files, path, irodsFilesMap):
        for key, val in irodsFilesMap.iteritems():
            if "__files__" in key:
                filePrefix = path+"/"
                filePrefix = "_"+filePrefix[self.irods_home_path.rindex("/")+1:]
                for f in val:
                    files.append((filePrefix+f).replace("/","_").replace(":","___"))
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
        self.logger.debug("Validating the consistency")
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
        self.logger.debug('Validating file existence')
        notExistingFiles = []
        for key, fileElem in files.iteritems():
            if fileElem not in irodsFiles:
                notExistingFiles.append(fileElem)
        return notExistingFiles
    
    def validateMetsManifestFile(self, mets_from_manifest, irodsFilesMap):
        self.logger.debug('Begin validation of manifest'+str(mets_from_manifest))
        
        #collect all files from fileSec
        fileSec = mets_from_manifest.fileSec
        files = self.getFiles(fileSec.fileGrp)
        missingFilesIDs = self.validateManifestConsistency(files, mets_from_manifest)
        
        isConsistent = True
        if missingFilesIDs:
            self.logger.debug('missingFilesIDs: ' + str(missingFilesIDs))
            isConsistent = False
        
        notExistingFiles = []
        if isConsistent:
            irodsFiles = self.getIrodsFiles(irodsFilesMap)
            notExistingFiles = self.validateFilesExistence(files, irodsFiles)
            self.logger.debug('notExistingFiles: ' + str(notExistingFiles))
        return missingFilesIDs, notExistingFiles

def executeValidation(args):
    logger.info ('Starting manifest comparison at: '+ datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'))
    logger.debug('Manifest path: ' + args.path)
    collectionPath = args.path.rsplit('/',1)[0]
    manifestPath = args.path
    
    #configuration = Configuration(args.confpath, args.debug, args.dryrun, logger)
    #configuration.parseConf();
    
    config = ConfigParser.RawConfigParser()
    with open(args.confpath, "r") as confFile:
        config.readfp(confFile)
            
    irods_home_dir = ""
    if (config.has_option('iRODS', 'irods_home_dir')):
        irods_home_dir = config.get('iRODS', 'irods_home_dir')
    
    irods_debug = False
    if (config.has_option('iRODS', 'irods_debug')):
        opt = config.get('iRODS', 'irods_debug')
        if opt in ['True', 'true']: 
            irods_debug = True
        
    irodsu = IRODSUtils(irods_home_dir, logger, irods_debug)
    if args.user:
        irodsu.setUser(args.user[0])
    
    xmltext = irodsu.getFile(manifestPath)
    try:
        mets_from_manifest = CreateFromDocument(xmltext)
        
        irodsFilesMap = irodsu.deepListDir(collectionPath)
        
        validator = MetsManifestValidator(collectionPath, args.debug, args.dryrun, logger)
        missingFilesIDs, notExistingFiles = \
            validator.validateMetsManifestFile(mets_from_manifest, 
                                               irodsFilesMap[1])
        
        is_consistent_flag = True
        all_files_existing_flag = True
        if len(missingFilesIDs) > 0:
            is_consistent_flag = False
        if len(notExistingFiles) > 0:
            all_files_existing_flag = False
        
        
        #TODO: decide what is the best way to store the validation results
        irodsu.adding_metadata(manifestPath, "VALIDATION_STATUS", "COMPLETED")
        irodsu.adding_metadata(manifestPath, "IS_CONSISTENT", str(is_consistent_flag))
        irodsu.adding_metadata(manifestPath, "ALL_FILES_EXISTING", str(all_files_existing_flag))
        
        logger.info("IS_CONSISTENT: "+str(is_consistent_flag))
        logger.info("Missing files for ID's: "+str(missingFilesIDs))   
        logger.info("ALL_FILES_EXISTING: "+str(all_files_existing_flag))
        logger.info("Missing files with path's: "+str(notExistingFiles))
        
#        manifestXML = setValidationStatusInMetsHdr(xmltext, "VALIDATED")
        
#        if args.dryrun:
#            print(manifestXML)
#        else:
#            logger.info('Writing the manifest to a file')
            
#            temp = tempfile.NamedTemporaryFile()
#            try:
#                temp.write(manifestXML)
#                temp.flush()
#                logger.info('in the irods namespace: {}'.format(manifestPath))
#                try: 
#                    irodsu.putFile(temp.name, manifestPath, configuration.irods_resource)
#                    #irodsu.putFile(temp.name, manifestPath+"_"+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'), configuration.irods_resource)
#                except:
#                    out = irodsu.putFile(temp.name, manifestPath)
#                    #out = irodsu.putFile(temp.name, manifestPath+"_"+datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'))
#                    print(str(out))
#            finally:
#                temp.close()
                    
        logger.info('Manifest validation completed at: '+ datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d.%H:%M:%S'))
    except pyxb.exceptions_.UnrecognizedContentError:
        logger.error("UnrecognizedContentError: "+traceback.format_exc())
    except sax._exceptions.SAXParseException:
        logger.error("SAXParseException: "+traceback.format_exc())
    except:
        logger.error(traceback.format_exc())
            
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
    parser.add_argument("confpath", help="path to the configuration file")
    parser.add_argument("-dbg", "--debug", action="store_true", 
                        help="enable debug")
    parser.add_argument("-d", "--dryrun", action="store_true",
                        help="run without performing any real change")
    
    parser.add_argument("-path", required=True,
                        help="irods path to the manifest")
    
    parser.add_argument("-u", "--user", nargs=1, help="irods user")
     
    parser.set_defaults(func=executeValidation) 
 
    args = parser.parse_args()
    args.func(args)
