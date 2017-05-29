#!/usr/bin/env python 
# -*- coding: utf-8 -*- 

import argparse
import base64
import hashlib
import json
import logging.handlers

from py2neo import Graph, Node, Relationship, authenticate, GraphError
import requests

import ConfigParser
from compare_mets_manifests import MetsManifestComparator
import manifest
from manifest.libmets import CreateFromDocument


#import logging
#import pprint
logger = logging.getLogger('GraphDBClient')

##################################################
# Person(name:'guybrush')
# Person - [:IS_DEFINED_IN] -> Zone
# DigitalEntity (EudatChecksum:'xyz', location:'/Zone/path/to/file')
# DigitalEntity - [:STORED_IN] -> Resource - [:IS_AVAILABLE_IN] -> Zone
# DigitalEntity - [:IS_OWNED_BY] -> Person
# DigitalEntity - [:BELONGS_TO] -> Aggregation
# DigitalEntity - [:IS_REPLICA_OF{PPID, ROR}] -> Pointer{type}
# DigitalEntity - [:IS_MASTER_OF{Replica}] -> Pointer{type}
# DigitalEntity - [:UNIQUELY_IDENTIFIED_BY] -> PID{EudatChecksum:'xyz'}

"""It represents the client to interact with the graph DB."""

class GraphDBClient():
    
    def __init__(self, conf, rootPath):
        """
        Graph initialization
        """
        self.conf = conf
        self.root = rootPath.rsplit('/',1)[0]
        self.collPath = rootPath
        # initializing the iRODS commands
        self.irodsu = manifest.IRODSUtils(self.conf.irods_home_dir,
                                          logger,
                                          self.conf.irods_debug)
        # authentication
        logger.info("Connecting to " + self.conf.graphdb_addr
                    + " with user " + self.conf.graphdb_user)    

        authenticate(self.conf.graphdb_addr, self.conf.graphdb_user, 
                     self.conf.graphdb_passwd)
        logger.debug("Authenticated, now connecting ...")
        self.graph = Graph( self.conf.graphdb_scheme + "://" 
                          + self.conf.graphdb_addr + self.conf.graphdb_path)
        # set up of the nodes related to the local B2SAFE service
        logger.debug("Searching if the initial nodes have been already created")
        self.zone = self.graph.find_one("Zone", "name", self.conf.irods_zone_name)
        if self.zone is None:
            logger.info('Node "Zone" ' + self.conf.irods_zone_name
                      + ' not found, so it will be created')
            try:
                self.graph.schema.create_uniqueness_constraint("DigitalEntity", 
                                                               "location")
                self.graph.schema.create_uniqueness_constraint("Aggregation",
                                                               "name")
                self.graph.schema.create_uniqueness_constraint("Zone", "name")
                self.graph.schema.create_uniqueness_constraint("Resource", "name")
            except GraphError as ge:
                logger.warning('Graph error: %s', ge)
            self.zone = Node("Zone", name=self.conf.irods_zone_name, 
                                     endpoint=self.conf.irods_zone_ep)
            self.graph.create(self.zone)
            resources = self.conf.irods_res.split(',')
            for res in resources:
                res_name,res_path = res.split(':')
                self.resNode = Node("Resource", name=res_name, path=res_path)
                res_is_located_in_zone = Relationship(self.resNode, 
                                                      "IS_AVAILABLE_IN", 
                                                      self.zone)
                try:
                    self.graph.create_unique(res_is_located_in_zone)
                except GraphError as ge:
                    logger.warning('Graph error: %s', ge)
        else:
            logger.info('Node "Zone" found')

        self.metadata = self.pullMessage()
        print (str(self.metadata))


    def pullMessage(self):
        """Pulls a set of messages from a message queue, according to a 
           predefined buffer length, and removes them from the queue.
           Return a dictionary of the messages' key-value pairs.
        """
        metadata = {}
        num = self.conf.msg_buffer
        sub = self.conf.msg_subscription
        logger.info('Reading {} messages from subscription {}'.format(num,sub))
        if sub is not None: 
            session = requests.Session()
            payload = {'key': self.conf.msg_token}
            endpoint = self.conf.msg_endpoint
            headers = {'Content-Type': 'application/json'}
            data = {'maxMessages': num}
            postdata = json.dumps(data)
            res = session.post(endpoint + '/subscriptions/' + sub.strip() + ':pull',
                           data=postdata, headers=headers, params=payload)
            logger.debug('Status code: {}'.format(str(res.status_code)))
            logger.debug('Response: {}'.format(res.text))
            res_dict = json.loads(res.text)
            dataids = {'ackIds': []}
            for rmessage in res_dict['receivedMessages']:
                content = base64.standard_b64decode(rmessage['message']['data'])
                metadata.update(self.msgToDict(content))
                dataids['ackIds'].append(rmessage['ackId'])
            logger.debug('Removing messages')
            postdataids = json.dumps(dataids)
            resids = session.post(endpoint + '/subscriptions/' + sub.strip()
                             + ':acknowledge', data=postdataids, headers=headers,
                             params=payload)
            logger.debug('Status code: {}'.format(str(resids.status_code)))
            logger.debug('Response: {}'.format(resids.text))
        
        return metadata


    def msgToDict(self, message):
        """Transforms the message content to a dictionary"""
        #msg_dict = {}
        pair = message.split(":", 1)
        collection = pair[0].strip(" {")
        msg_dict = {collection: {}}
        second_pair = pair[1].split("objects:", 1)
        coll_attrs = second_pair[0].strip(" ,{").split(",")
        for attr in coll_attrs:
            key,val = attr.split(":")
            msg_dict[collection][key] = val
        objs_attrs = second_pair[1].strip(" []}").split(",")
        for attr in objs_attrs:
            name,key,val = attr.split("=:=")
            path = collection + "/" + name
            if path in msg_dict.keys():
                msg_dict[path][key] = val
            else:
                msg_dict[path] = {key:val}
        return msg_dict


# dynamic data ###################################

    def push(self, structuralMap):
        """It uploads new data to the graphDB"""
        logger.info('Start to upload the new metadata to the Graph DB')
        self._structRecursion(structuralMap)

    def _structRecursion(self, d):
        """This is the main function responsible to read the manifest 
           dictionary in input and create the graph as result.
        """
        # start to look if the node has nested objects
        if len(d['nestedObjects']) > 0:

            # add the root path of the collection. 
            # It should happen just once, for the root collection,
            # the only one of the type 'digitalCollection'
            if (d['type'] == 'digitalCollection'):
                path = self.collPath
            else:
                path = ''
                d['name'] = self.collPath + ':' + d['name']
            sumValue = ''
            agg = self._createUniqueNode("Aggregation", d['name'],
                                                        path,
                                                        sumValue,
                                                        d['type'])
            # if the aggregation has a file path, it means that it is a package
            if len(d['filePaths']) > 0:
                if (len(d['filePaths']) == 1 
                    and len(d['filePaths'][d['filePaths'].keys()[0]]) == 1):
                    pathId = d['filePaths'].keys()[0]
                    path = d['filePaths'][pathId][0]
                    if self.conf.dryrun: 
                        print "get the checksum based on path: " + str(path)
                    else:
                        agg.properties['location'] = path[7:]
                        agg.push()
                        logger.debug('Updated location of entity: ' + str(agg))
                        agg = self._defineDigitalEntity(None, path[7:], 
                                                        d['type'], d['name'], agg)
                # the manifest supports multiple file paths, 
                # but they are not yet supported by this script                
                else:                
                    logger.warning('multiple file paths not allowed')
            # check the nested objects in a recursive way
            for elem in d['nestedObjects']:
                nodes = self._structRecursion(elem)
                for n in nodes:
                    if self.conf.dryrun:
                        print ("create the graph relation ["+str(n)+","
                               "BELONGS_TO,"+str(agg)+"]")
                    else:
                        de_belongs_to_agg = Relationship(n, "BELONGS_TO", agg)
                        self.graph.create_unique(de_belongs_to_agg)
                        logger.debug('Created relation: ' + str(de_belongs_to_agg))

            return [agg]               
        # if nested objects are not present, then this is a leaf in the tree
        else:
            leafs = []
            if len(d['filePaths']) > 0:
                for fid in d['filePaths']:
                    for fp in d['filePaths'][fid]:
                        de = self._defineDigitalEntity(d['name'], fp[7:], 
                                                       d['type'], fid)
                        leafs.append(de)
            # if there is not a path, the leaf is an aggregation, even if an empty one.
            else:
                path = ''
                sumValue = ''
                agg = self._createUniqueNode("Aggregation", d['name'],
                                                            path[7:],
                                                            sumValue,
                                                            d['type'])
                leafs.append(agg)

            return leafs
     
 
    def _defineDigitalEntity(self, fmt, path, dtype, name, de=None, absolute=False):
        """Defines the graph node which corresponds to an EUDAT Digital Entity"""
        absolutePath = path
        # if the path of the files in the manifest is relative
        # then the absolute path has to be built to get the file properties.
        if not absolute:
            absolutePath = self.root + '/' + path
        # calculate the checksum
        if self.conf.dryrun: 
            sumValue = ''
        else:
            sumValue = ''
            if absolutePath in self.metadata.keys():
                if 'checksum' in self.metadata[absolutePath].keys():
                    sumValue = self.metadata[absolutePath]['checksum']            
        if de is not None:
            if sumValue:
                de.properties['checksum'] = sumValue
                de.push()
                logger.debug('Updated checksum of entity: ' + str(de))
#TODO what if checksum is null?
        else:
            # build the digital entity
            de = self._createUniqueNode("DigitalEntity", name, absolutePath, sumValue,
                                                         dtype)
            de.properties['format'] = fmt
            if not self.conf.dryrun:
                de.push()
            logger.debug('Created node: ' + str(de))
        # check the entity relationships
        self._defineResourceRelation(de, absolutePath)
        self._definePIDRelation(de, absolutePath)
        self._defineMasterRelation(de, absolutePath)
        self._defineReplicaRelation(de, absolutePath)
        self._defineOwnershipRelation(de, absolutePath)
        return de


    def _definePIDRelation(self, de, absolutePath):
        """Defines the graph relation which associates a PID with an EUDAT 
           Digital Entity.
        """
        if self.conf.dryrun:
            print ("create the graph relation ["+str(de)+","
                   "UNIQUELY_IDENTIFIED_BY,persistent_identifier]")
            return True

        #path = de.properties["location"]
        sumValue = de.properties["checksum"]
        pid = None
        if absolutePath in self.metadata.keys():
            pid = self.metadata[absolutePath]['PID']
        if pid:
            pidNode = Node("PersistentIdentifier", value = pid,
                                                   checksum = sumValue)
            de_is_uniquely_identified_by_pid = Relationship(de,
                                                            "UNIQUELY_IDENTIFIED_BY",
                                                            pidNode)
            self.graph.create_unique(de_is_uniquely_identified_by_pid)
            logger.debug('Created relation: ' + str(de_is_uniquely_identified_by_pid))
            return True
    
        return False


    def _defineMasterRelation(self, de, absolutePath):
        """Defines the graph relation which associates a master copy with its 
           replica.
        """
        if self.conf.dryrun:
            print ("create the graph relation ["+str(de)+","
                   "IS_MASTER_OF,replica]")
            return True

        if absolutePath in self.metadata.keys():
            if 'Replica' in self.metadata[absolutePath].keys():
                replicas = self.metadata[absolutePath]['Replica']
                for rpointer in replicas: 
                    po = self._createPointer('iRODS', rpointer)
                    de_is_master_of_po = Relationship(de, "IS_MASTER_OF", po)
                    self.graph.create_unique(de_is_master_of_po)
                    logger.debug('Created relation: ' + str(de_is_master_of_po))
                return True

        return False


    def _defineReplicaRelation(self, re, absolutePath):
        """Defines the graph relation which associates a replica with its 
           master copy.
        """
        if self.conf.dryrun:
            print ("create the graph relation ["+str(re)+","
                   "IS_REPLICA_OF,replica]")
            return True

        parent = None
        master = None
        if absolutePath in self.metadata.keys():
            master = self.metadata[absolutePath]['EUDAT/ROR']
        if master:
            pid = self.metadata[absolutePath]['PID']
            if pid == master:
                return False
            if 'PPID' in self.metadata[absolutePath].keys():
                parent = self.metadata[absolutePath]['PPID']
        else:
            return False

        pid = self.metadata[absolutePath]['PID']
        po = self._createPointer('unknown', master)
        re_is_replica_of_po = Relationship(re, "IS_REPLICA_OF", po)
        re_is_replica_of_po.properties["relation"] = 'ROR'
        self.graph.create_unique(re_is_replica_of_po)
        logger.debug('Created relation: ' + str(re_is_replica_of_po))
        if parent:
            po = self._createPointer('unknown', parent)
            re_is_replica_of_po = Relationship(re, "IS_REPLICA_OF", po)
            re_is_replica_of_po.properties["relation"] = 'PPID'
            self.graph.create_unique(re_is_replica_of_po)
            logger.debug('Created relation: ' + str(re_is_replica_of_po))

        return True


    def _defineOwnershipRelation(self, de, absolutePath):
        """Defines the graph relation which associates an owner with an EUDAT
           Digital Entity.
        """      
        if self.conf.dryrun:
            print ("create the graph relation ["+str(de)+",IS_OWNED_BY,"
                                               "b2safe_owner_name]")
            return True
 
#TODO add multiple owners management
        owners = None
        if absolutePath in self.metadata.keys():
            owner = self.metadata[absolutePath]['owner']
            owners = [owner]
        logger.debug('Got the list of owners: ' + str(owners))
        if owners:
            for owner in owners:
                person = self.graph.merge_one("Person", "name", owner)
                logger.debug('Got the person: ' + str(person))
                person_is_defined_in_zone = Relationship(person, "IS_DEFINED_IN",
                                                         self.zone)
                self.graph.create_unique(person_is_defined_in_zone)
                logger.debug('Created relation: ' + str(person_is_defined_in_zone))
                node_is_owned_by_person = Relationship(de, "IS_OWNED_BY", person)
                self.graph.create_unique(node_is_owned_by_person)
                logger.debug('Created relation: ' + str(node_is_owned_by_person))
            return True
        return False


    def _defineResourceRelation(self, de, absolutePath):
        """Defines the graph relation which associates an iRODS resource with an
           EUDAT Digital Entity.
        """
        if self.conf.dryrun:
            print ("create the graph relation ["+str(de)+",IS_STORED_IN,"
                                               "b2safe_resource_name]")
            return True

#TODO add multiple resources management
        resources = None
        if absolutePath in self.metadata.keys():
            res = self.metadata[absolutePath]['resource']
            resources = [res]
        if resources:
            for res in resources:
                resN = self.graph.find_one('Resource', 'name', res)
                if resN:
                    de_is_stored_in_res = Relationship(de, "IS_STORED_IN", resN)
                    self.graph.create_unique(de_is_stored_in_res)
                    logger.debug('Created relation: ' + str(de_is_stored_in_res))
            return True
        return False


    def _createUniqueNode(self, eudat_type, name, path, checksum, d_type):
        """Build a graph node enforcing its uniqueness constraints"""   
        entityNew = Node(eudat_type, location = path,
                                     name = name,
                                     checksum = checksum,
                                     nodetype = d_type)
        if self.conf.dryrun: 
            print ("create the graph node ["+str(entityNew)+"]")
            return entityNew
        else:
            # check if the node is already stored in the graph DB
            # basing the search on the property which is unique: the name
            entity = self.graph.find_one(eudat_type, "name", name)
            if entity is None:
                entity = entityNew
            self.graph.create(entity)
            logger.debug('Entity created: ' + str(entity))
            return entity


    def _createPointer(self, pointer_type, value):
        """Defines a graph node which represents a pointer to nodes stored
           in a outer domain.
        """
        hashVal = hashlib.md5(value).hexdigest()
        pointerNew = Node('Pointer', type = pointer_type,
                                     value = value,
                                     name = hashVal)
        if self.conf.dryrun:
            print ("create the graph node ["+str(pointerNew)+"]")
        else:
            pointer = self.graph.find_one('Pointer', "name", hashVal)
            if pointer is None:
                pointer = pointerNew
            self.graph.create(pointer)
            logger.debug('Created pointer: ' + str(pointer))

        return pointer
    
    def updateGraphAddingNodes(self, addetedDivs, collectionName, filePaths):
        if self.conf.dryrun: 
            print("add nodes and relations to the aggregation node")
        else: 
            collectionNodeName = self.collPath + ':' + collectionName
            collectionNode = self.graph.find_one("Aggregation", "name", collectionNodeName)
            if collectionNode is None:
                newCollectionNode = Node("Aggregation", location = '', name = collectionNodeName, checksum = '', nodetype = 'entityRelation')
                self.graph.create(newCollectionNode)
                collectionNode = newCollectionNode
            
            for key, div in addetedDivs.iteritems():
                path = filePaths[key]
                fileName = (path[0].replace("file://","")).replace(collectionName+'/',"")
                de = self._defineDigitalEntity(div.LABEL, fileName, div.TYPE, key)
                de_belongs_to_agg = Relationship(de, "BELONGS_TO", collectionNode)
                self.graph.create_unique(de_belongs_to_agg)
                logger.debug('Created relation: ' + str(de_belongs_to_agg))
        
    def updateGraphAddingDefaultDiv(self, defaultDiv, smap, filePaths):
        if self.conf.dryrun:
            print("add default div node to collection node")
        else: 
            agg = self._createUniqueNode("Aggregation", smap['name'], self.collPath, '', smap['type'])
            
            for fptr_elem in defaultDiv.fptr: 
                fileId = fptr_elem.FILEID
                path = filePaths[fileId]
                fileName = (path[0].replace("file://","")).replace(smap['name'],"")
                
                de = self._defineDigitalEntity(defaultDiv.LABEL, fileName, defaultDiv.TYPE, fileId)
                
                de_belongs_to_agg = Relationship(de, "BELONGS_TO", agg)
                self.graph.create_unique(de_belongs_to_agg)
                logger.debug('Created relation: ' + str(de_belongs_to_agg))
        
    def updateGraphDeletingDefaultDiv(self, defaultDiv, smap, filePaths):
        if self.conf.dryrun:
            print("detach, delete the node from aggregation node, check if there are loose nodes after detach")
        else:
            for fptr_elem in defaultDiv.fptr: 
                nodeName = fptr_elem.FILEID
                cypher = self.graph.cypher
                #get all nodes with only one relation to the given node and detach delete them
                cypher.execute("MATCH (n{name:{nName}})-[r]-(m)-[t]-(x) WITH n, m, count(t)+1 AS mrel WHERE mrel = 1 DETACH DELETE m", {"nName":str(nodeName)})
                #than detach delete the given node
                cypher.execute("MATCH (n{name:{nName}}) DETACH DELETE n", {"nName":str(nodeName)})
            
#           #delete disconnected nodes
#           MATCH (n) WHERE NOT (n)--() DELETE n
#             
#           #get all nodes with only one relation to the given node and detach delete them
#           MATCH (n)-[r]-(m) WITH n, m count((m)--()) AS mrel WHERE mrel = 1 DETACH DELETE m
#             
#           #than detach delete the given node
#           MATCH (n) DETACH DELETE n
        
    def updateGraphDeletingNodes(self, deletedDivs, smap, filePaths):
        if self.conf.dryrun: 
            print("detach, delete the node from aggregation node, check if there are loose nodes after detach")
        else: 
            for nodeName, divToDelete in deletedDivs.iteritems(): 
                cypher = self.graph.cypher
                cypher.execute("MATCH (n{name:{nName}})-[r]-(m)-[t]-(x) WITH n, m, count(t)+1 AS mrel WHERE mrel = 1 DETACH DELETE m", {"nName":str(nodeName)})
                cypher.execute("MATCH (n{name:{nName}}) DETACH DELETE n", {"nName":str(nodeName)})
    
################################################################################
# Configuration Class #
################################################################################
 
class Configuration():
    """ 
    Get properties from filesystem
    """

    def __init__(self, conffile, debug, dryrun, logger):
   
        self.conffile = conffile
        self.debug = debug
        self.dryrun = dryrun
        self.logger = logger
        self.log_level = {'INFO': logging.INFO, 'DEBUG': logging.DEBUG, \
                          'ERROR': logging.ERROR, 'WARNING': logging.WARNING, \
                          'CRITICAL': logging.CRITICAL}

    def parseConf(self):
        """Parse the configuration file."""

        self.config = ConfigParser.RawConfigParser()
        with open(self.conffile, "r") as confFile:
            self.config.readfp(confFile)
        
        logfilepath = self._getConfOption('Logging', 'log_file')
        loglevel = self._getConfOption('Logging', 'log_level')
        if self.debug:
            loglevel = 'DEBUG'
        logger.setLevel(self.log_level[loglevel])
        rfh = logging.handlers.RotatingFileHandler(logfilepath, \
                                                   maxBytes=8388608, \
                                                   backupCount=9)
        formatter = logging.Formatter('%(asctime)s %(levelname)s: '
                                    + '[%(funcName)s] %(message)s')
        rfh.setFormatter(formatter)
        logger.addHandler(rfh)
        
        self.graphdb_scheme = self._getConfOption('GraphDB', 'scheme')
        self.graphdb_addr = self._getConfOption('GraphDB', 'address')
        self.graphdb_user = self._getConfOption('GraphDB', 'username')
        self.graphdb_passwd = self._getConfOption('GraphDB', 'password')
        self.graphdb_path = self._getConfOption('GraphDB', 'path')

        self.msg_token = self._getConfOption('MessageSystem', 'token')
        self.msg_endpoint = self._getConfOption('MessageSystem', 'endpoint')
        self.msg_buffer = self._getConfOption('MessageSystem', 'buffer')
        self.msg_subscription = self._getConfOption('MessageSystem', 'subscription')

        self.irods_zone_name = self._getConfOption('iRODS', 'zone_name')
        self.irods_zone_ep = self._getConfOption('iRODS', 'zone_ep')
        self.irods_res = self._getConfOption('iRODS', 'resources')
        self.irods_home_dir = self._getConfOption('iRODS', 'irods_home_dir')
        self.irods_debug = self._getConfOption('iRODS', 'irods_debug', True)

        
    def _getConfOption(self, section, option, boolean=False):
        """
        get the options from the configuration file
        """

        if (self.config.has_option(section, option)):
            opt = self.config.get(section, option)
            if boolean:
                if opt in ['True', 'true']: return True
                else: return False
            return opt
        else:
            self.logger.warning('missing parameter %s:%s' % (section,option))
            return None


################################################################################
# B2SAFE GraphDB client Command Line Interface #
################################################################################
def getFilePathMapFrom(smap):
    filePaths = {}
    for obj in smap['nestedObjects']:
        if len(obj['nestedObjects']) > 0 :
            getFilePathMapRecursively(obj['nestedObjects'], filePaths)
        else:
            for fileId, path in obj['filePaths'].iteritems():
                filePaths[fileId] = path
    
    return filePaths
    
def getFilePathMapRecursively(nestedObjectsList, filePaths):
    for obj in nestedObjectsList:
        if len(obj['nestedObjects']) > 0 :
            getFilePathMapRecursively(obj['nestedObjects'], filePaths)
        else:
            for fileId, path in obj['filePaths'].iteritems():
                filePaths[fileId] = path

def sync(args):
    configuration = Configuration(args.confpath, args.debug, args.dryrun, logger)
    configuration.parseConf();
    
    gdbc = GraphDBClient(configuration, args.path)
    
    logger.info("Sync starting ...")
    mp = manifest.MetsParser(configuration, logger)
    logger.info("Reading METS manifest ...")
    irodsu = manifest.IRODSUtils(configuration.irods_home_dir, logger,
                             configuration.irods_debug)
    if args.user:
        logger.info('Working as user ' + args.user[0])
        irodsu.setUser(args.user[0])
    
    #get the 'old' manifest from the collection already puted into iRODS
    xmltext = irodsu.getFile(args.path + '/manifest.xml')
    if xmltext is None:
        logger.error('manifest.xml file not found')
    
    structuralMaps = mp.parse(xmltext)
    for smapName, smap in structuralMaps.iteritems():
        
        oldXMLtext = irodsu.getFile(args.path + '/manifest.xml.old')
        if oldXMLtext: 
            mets_from_old_manifest = CreateFromDocument(oldXMLtext) 
            mets_from_new_manifest = CreateFromDocument(xmltext)
            oldStructuralMaps = mp.parse(oldXMLtext)
            if not oldStructuralMaps:
                logger.error('no strucktMap in new manifest.xml found')
                break
            
            if (mets_from_old_manifest is not None) & (mets_from_new_manifest is not None):
                metsComparator = MetsManifestComparator(configuration, logger)
                diff = metsComparator.compareMetsManifestFiles(mets_from_old_manifest, mets_from_new_manifest);
                
                oldSmap = oldStructuralMaps[smapName]
                if oldSmap is None:
                    logger.error('no smap with name '+ smapName + ' found in the new manifest.xml')
                    break
                newFilePathMap = getFilePathMapFrom(smap)
                oldFilePathMap = getFilePathMapFrom(oldSmap)
                
                for k, v in diff.iteritems():
                    if k == MetsManifestComparator.STRUCT_MAP_CHANGES:
                        structMapDiff = v
                        for key, val in structMapDiff.iteritems():
                            #TODO: rename case of the collection
                            collectionName = key.replace(MetsManifestComparator.LOGICAL_COLLECTION_CHANGE,"")
                            
                            if MetsManifestComparator.LOGICAL_COLLECTION_CHANGE in str(key):
                                addedDivs = val[MetsManifestComparator.ADDED_DIVS]
                                gdbc.updateGraphAddingNodes(addedDivs, collectionName, newFilePathMap)
                                
                                deletedDivs = val[MetsManifestComparator.DELETED_DIVS]
                                gdbc.updateGraphDeletingNodes(deletedDivs, oldSmap, oldFilePathMap)
                            elif MetsManifestComparator.ADDED_DEFAULT_DIV in str(key):
                                gdbc.updateGraphAddingDefaultDiv(val, smap, newFilePathMap)
                            elif MetsManifestComparator.DELETED_DEFAULT_DIV in str(key):
                                gdbc.updateGraphDeletingDefaultDiv(val, oldSmap, oldFilePathMap)
                            else:
                                logger.info('ERROR: should not happen, unknown key in the diff map') 
                    else:
                        print("fileSec Changes not needed for now")
                        
                logger.info('Manifest comparison completed')
            else:
                logger.error('Manifest comparison not done, because one of manifest files was not found')            
        else: 
            gdbc.push(smap)
    
    logger.info("Sync completed")
    
    if args.user:
        irodsu.unsetUser()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='B2SAFE graphDB client')
    parser.add_argument("-confpath", help="path to the configuration file")
    parser.add_argument("-dbg", "--debug", action="store_true", 
                        help="enable debug")
    parser.add_argument("-d", "--dryrun", action="store_true",
                        help="run without performing any real change")
    parser.add_argument("-path", help="irods path to the data")
    
    parser.add_argument("-u", "--user", nargs=1, help="irods user")
    
    parser.set_defaults(func=sync) 
    
    args = parser.parse_args()
    args.func(args)