#!/usr/bin/env python 
# -*- coding: utf-8 -*- 

import argparse
import base64
import hashlib
import json
import logging.handlers
import pyxb
import traceback
import os

from xml import sax
from py2neo import Graph, Node, Relationship, authenticate, GraphError
import requests

import ConfigParser
from validate_mets_manifest import MetsManifestValidator
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
    
    def __init__(self, conf, rootPath, irodsu):
        """
        Graph initialization
        """
        self.conf = conf
        self.root = rootPath.rsplit('/',1)[0]
        self.collPath = rootPath
        # initializing the iRODS commands
        self.irodsu = irodsu
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
            if self.conf.dryrun:
                print("create node for ZONE: "+self.conf.irods_zone_name)
            else: 
                self.graph.create(self.zone)
            resources = self.conf.irods_res.split(',')
            for res in resources:
                res_name,res_path = res.split(':')
                self.resNode = Node("Resource", name=res_name, path=res_path)
                res_is_located_in_zone = Relationship(self.resNode, 
                                                      "IS_AVAILABLE_IN", 
                                                      self.zone)
                if self.conf.dryrun:
                    print("create node and relation for Resource: "+
                          res_name+" under path "+res_path)
                else: 
                    try:
                        self.graph.create_unique(res_is_located_in_zone)
                    except GraphError as ge:
                        logger.warning('Graph error: %s', ge)
        else:
            logger.info('Node "Zone" found')
        
        coll_names = {}
        while True:
            coll_meta = self.pullMessage()
            if len(coll_meta) == 0: break
            coll_names.update(coll_meta)
        self.metadata = {}
        for coll in coll_names:
            while True:
                metadata = self.pullMessage(coll_names[coll]['md5hash'] + '_NOTIFY')
                if len(metadata) == 0: break
                self.metadata.update(metadata)
            self.deleteTopic(coll_names[coll]['md5hash'])
            self.deleteSub(coll_names[coll]['md5hash'] + '_NOTIFY')


    def pullMessage(self, subscription = None, buffer_length = None):
        """Pulls a set of messages from a message queue, according to a 
           predefined buffer length, and removes them from the queue.
           Return a dictionary of the messages' key-value pairs.
        """
        metadata = {}
        if subscription is None:
            sub = self.conf.msg_subscription
        else:
            sub = subscription
        if buffer_length is None:
            num = self.conf.msg_buffer
        else:
            num = buffer_length
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
                logger.debug('Content decoded: ' + content)
                if subscription is None:
                    hashed_content = hashlib.md5(content).hexdigest()
                    metadata.update({content:{'md5hash':hashed_content}})
                else:
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
        # { /cinecaDMPZone2/home/claudio/testSuite:{ owner:claudio, objects: [] } }
        pair = message.split(":", 1)
        collection = pair[0].strip(" {")
        msg_dict = {collection: {}}
        second_pair = pair[1].split("objects:", 1)
        coll_attrs = second_pair[0].strip(" ,{").split(",")
        for attr in coll_attrs:
            if len(attr) > 0 and ":" in attr:
                key,val = attr.split(":")
                msg_dict[collection][key] = val
        objs_attrs = second_pair[1].strip(" []}").split(",")
        for attr in objs_attrs:
            if len(attr) > 0 and "=:=" in attr:
                name,key,val = attr.split("=:=")
                path = collection + "/" + name
                if path in msg_dict.keys():
                    msg_dict[path][key] = val
                else:
                    msg_dict[path] = {key:val}
        return msg_dict


    def deleteTopic(self, name):
        """Delete the queue/topic"""

        logger.info('Deleting the topic {}'.format(name))
        session = requests.Session()
        payload = {'key': self.conf.msg_token}
        endpoint = self.conf.msg_endpoint
        headers = {'Accept': 'application/json'}
        res = session.delete(endpoint + '/topics/' + name.strip(),
                             headers=headers, params=payload)
        logger.debug('Status code: {}'.format(str(res.status_code)))
        logger.debug('Response: {}'.format(res.text))
        return res.text


    def deleteSub(self, name):
        """Delete a subscription"""

        logger.info('Deleting the subscription {}'.format(name))
        session = requests.Session()
        payload = {'key': self.conf.msg_token}
        endpoint = self.conf.msg_endpoint        
        headers = {'Content-Type': 'application/json'}
        res = session.delete(endpoint + '/subscriptions/' + name.strip(),
                             headers=headers, params=payload)
        logger.debug('Status code: {}'.format(str(res.status_code)))
        logger.debug('Response: {}'.format(res.text))
        return res.text


# dynamic data ###################################

    def push(self, structuralMap, collPath):
        """It uploads new data to the graphDB"""
        logger.info('Start to upload the new metadata to the Graph DB')
        self._structRecursion(structuralMap, collPath)

    def _structRecursion(self, d, collPath):
        """This is the main function responsible to read the manifest 
           dictionary in input and create the graph as result.
        """
        # check the difference between collPath and self.root
        parentColl = ""
        if self.root in collPath:
            diffPath = collPath.replace(self.root, "")
            logger.debug('path difference: ' + diffPath)
            diffs = diffPath.rsplit('/',1)
            if (diffs is not None) and (len(diffs) > 1):
                parentColl = diffs[0].lstrip('/')
        logger.debug('parent collection: ' + parentColl)
        # start to look if the node has nested objects
        if len(d['nestedObjects']) > 0:

            # add the root path of the collection. 
            # It should happen just once, for the root collection,
            # the only one of the type 'digitalCollection'
            if (d['type'] == 'digitalCollection'):
                path = collPath
            else:
                path = ''
                d['name'] = collPath + ':' + d['name']
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
                    if len(parentColl) > 0:
                        aggPath = parentColl + '/' + path[7:]
                    else:
                        aggPath = path[7:]
                    if self.conf.dryrun: 
                        print "get the checksum based on path: " + str(aggPath)
                    else:
                        agg.properties['location'] = aggPath
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
                nodes = self._structRecursion(elem, collPath)
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
                        if len(parentColl) > 0:
                            dePath = parentColl + '/' + fp[7:]
                        else:
                            dePath = fp[7:]
                        de = self._defineDigitalEntity(d['name'], dePath, 
                                                       d['type'], fid)
                        leafs.append(de)
            # if there is not a path, the leaf is an aggregation, even if an empty one.
            else:
                if len(d['linkedMets']) == 0: 
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
        if (absolutePath in self.metadata.keys() 
            and 'PID' in self.metadata[absolutePath].keys()):
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
        if (absolutePath in self.metadata.keys()
            and 'EUDAT/ROR' in self.metadata[absolutePath].keys()):
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
 
        owners = None
        if absolutePath in self.metadata.keys():
            logger.debug('found the path: ' + absolutePath)
            owner = self.metadata[absolutePath]['owner']
            owners = [owner]
        logger.debug('Got the list of owners for path [' + absolutePath + ']: ' + str(owners))
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
            # basing the search on the property which is unique
            if eudat_type == 'DigitalEntity':
                entity = self.graph.find_one(eudat_type, "location", path)
            else:
                entity = self.graph.find_one(eudat_type, "name", name)
            if entity is None:
                entity = entityNew
                self.graph.create(entity)
            logger.debug('Entity created: ' + str(entity))
            return entity
    
    def findNodeByProperty(self, eudat_type, property_key, property_value):
        return self.graph.find_one(eudat_type, property_key, property_value)

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
                newCollectionNode = Node("Aggregation", location = '', 
                                         name = collectionNodeName, 
                                         checksum = '', 
                                         nodetype = 'entityRelation')
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
            agg = self._createUniqueNode("Aggregation", smap['name'], 
                                         self.collPath, 
                                         '', 
                                         smap['type'])
            
            for fptr_elem in defaultDiv.fptr: 
                fileId = fptr_elem.FILEID
                path = filePaths[fileId]
                fileName = (path[0].replace("file://","")).replace(smap['name'],"")
                
                de = self._defineDigitalEntity(defaultDiv.LABEL,
                                                fileName,
                                                defaultDiv.TYPE,
                                                fileId)
                
                de_belongs_to_agg = Relationship(de, "BELONGS_TO", agg)
                self.graph.create_unique(de_belongs_to_agg)
                logger.debug('Created relation: ' + str(de_belongs_to_agg))
        
    def updateGraphDeletingDefaultDiv(self, defaultDiv):
        if self.conf.dryrun:
            print("detach, delete the node from aggregation node,"+
                  " check if there are loose nodes after detach")
        else:
            for fptr_elem in defaultDiv.fptr: 
                nodeName = fptr_elem.FILEID
                cypher = self.graph.cypher
                #get all nodes with only one relation to the given node
                #and detach delete them
                cypher.execute("MATCH (n{name:{nName}})-[r]-(m)-[t]-(x) "+
                               "WITH n, m, count(t)+1 AS mrel "+
                               "WHERE mrel = 1 DETACH DELETE m", 
                               {"nName":str(nodeName)})
                #than detach delete the given node
                cypher.execute("MATCH (n{name:{nName}}) DETACH DELETE n",
                                {"nName":str(nodeName)})
            
#           #delete disconnected nodes
#           MATCH (n) WHERE NOT (n)--() DELETE n
#             
#           #get all nodes with only one relation to the given node 
#           #and detach delete them
#           MATCH (n)-[r]-(m) WITH n, m count((m)--())
#           AS mrel WHERE mrel = 1 DETACH DELETE m
#             
#           #then detach delete the given node
#           MATCH (n) DETACH DELETE n
    
    def updateGraphDeletingNodes(self, deletedDivs):
        if self.conf.dryrun: 
            print("detach, delete the node from aggregation node,"+
                  " check if there are loose nodes after detach")
        else: 
            for nodeName in deletedDivs.keys(): 
                cypher = self.graph.cypher
                cypher.execute("MATCH (n{name:{nName}})-[r]-(m)-[t]-(x) "+
                               "WITH n, m, count(t)+1 AS mrel "+
                               "WHERE mrel = 1 DETACH DELETE m", 
                               {"nName":str(nodeName)})
                cypher.execute("MATCH (n{name:{nName}}) "+
                               "DETACH DELETE n", {"nName":str(nodeName)})
    
    def getLinkedNodes(self, nodeName):
        if self.conf.dryrun: 
            print("get nodes with IS_LINKED_TO rel to node  " +nodeName)
            return []
        else:
            cypher = self.graph.cypher
            return cypher.execute("MATCH (n) - [rel:IS_LINKED_TO]-(m) "+
                                  "WHERE m.name = {mName} return n", 
                                  {"mName":str(nodeName)})
    
    def deleteLinkedToRelationBetween(self, smapName, linkedSmapName):
        if self.conf.dryrun: 
            print("delete relation between rootSmap: " 
                  +smapName+ " and linkedSmap: " +linkedSmapName)
        else:
            cypher = self.graph.cypher
            tx = cypher.begin()
            cypher.execute("MATCH (a)-[r:IS_LINKED_TO]-(b) " +
                           "WHERE a.name = {nName} AND b.name = {mName} DELETE r",
                           {"nName":str(smapName), "mName":str(linkedSmapName)})
            tx.commit()
            
    def deleteSubgraph(self, linkedNodeName):
        if self.conf.dryrun: 
            print("detach, delete the subgraph "+
                  "starting from the given root node "+linkedNodeName)
        else:
            print("detach, delete the subgraph "+
                  "starting from the given root node "+linkedNodeName)
            cypher = self.graph.cypher
            tx = cypher.begin()
            cypher.execute("MATCH (n{name:{nName}})<-[r:BELONGS_TO*]-(m) "+
                           " DETACH DELETE n, m", {"nName":str(linkedNodeName)})
            tx.commit()
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

############################################################################################
# B2SAFE GraphDB synchronizer                                                              #
# holds configuration information, a GraphDBClient, MetsParser and IRODSUtils instance     # 
# for all operations needed to be done to synchronize the descriptions of iRODS collection #
# with the neo4j graph.                                                                    #
############################################################################################

class GraphDBSynchronizer():
    
    def __init__(self, debug, dryrun, configuration, gdbc, mp, irodsu):
        """
        Synchronizer initialization
        """
        self.configuration = configuration
        self.debug = debug
        self.dryrun = dryrun
        self.gdbc = gdbc
        self.mp = mp
        self.irodsu = irodsu
        logger.info("GraphDBSynchronizer created")
        
        self.sychronizedPaths = []
        
        self.newManifestPath = ""
        self.oldManifestPath = ""
        
        self.mets_from_new_manifest = None
        self.mets_from_old_manifest = None
    
    def createOrUpdateGraph(self, path):
        #get the 'old' manifest from the collection already injected into iRODS
        manifests = self.getManifestFilesFrom(path)
        xmltext = manifests["newManifestFile"]
        oldXMLtext = manifests["oldManifestFile"]
        
        if xmltext is None:
            logger.error('manifest.xml file not found')
        else:
            #lookup if the manifest are validated and if it is if valid or not, if not validate
            validationResult = self.isManifestValid(path, self.newManifestPath, xmltext)
            self.mets_from_new_manifest = validationResult["mets"]
            
            #only the internal consistency can be checked 
            #validationResult2 = self.isManifestValid(path, self.oldManifestPath, oldXMLtext)
            #self.mets_from_old_manifest = validationResult2["mets"]
            
            if validationResult["isValid"] is False:
                logger.error("The new manifest is valid = " 
                + str(validationResult["isValid"]) + " so no further execution!")
                return -1
            
            try:
                logger.info("Reading METS manifest ...")
                structuralMaps = self.mp.parse(xmltext)
                for smapName, smap in structuralMaps.iteritems():
                    smapNode = self.gdbc.findNodeByProperty("Aggregation", "name", smapName)
                    if (oldXMLtext is not None) & (smapNode is not None):
                        #the manifest was changed and the subgraph for the smap exists, 
                        #so update of the subgraph is needed
                        if not self.mets_from_old_manifest:
                            self.mets_from_old_manifest = CreateFromDocument(oldXMLtext) 
                        if not self.mets_from_new_manifest:
                            self.mets_from_new_manifest = CreateFromDocument(xmltext)
                        
                        oldStructuralMaps = self.mp.parse(oldXMLtext)
                        if not oldStructuralMaps:
                            logger.error('no strucktMap in old manifest.xml found')
                            break
                        
                        if (self.mets_from_old_manifest is not None) & (self.mets_from_new_manifest is not None):
                            metsComparator = MetsManifestComparator(self.configuration, logger)
                            diff = metsComparator.compareMetsManifestFiles(self.mets_from_old_manifest,
                                                                            self.mets_from_new_manifest)
                            
                            oldSmap = oldStructuralMaps[smapName]
                            self.updateGraphWith(diff, smap)
                            
                            linksInOldMets = self.extractLinksFrom(oldSmap)
                            linksInNewMets = self.extractLinksFrom(smap)
                            linksDiff = metsComparator.compareLinks(linksInOldMets, linksInNewMets)
                            
                            self.checkForDeletedSubgraphs(linksDiff, metsComparator)
                            
                            self.updateLinksWith(linksDiff, smapName)
                                   
                            logger.info('Graph update completed')
                        else:
                            logger.error('Graph update not done, because one of manifest files was not found')            
                    else: 
                        #create subgraph for the smap
                        if smapNode is None:
                            logger.debug("create subgraph for the smap: "+ smapName)
                            self.gdbc.push(smap, path)
                            #create, connect or update all links to other manifests/subgraphs and the linked subgraphs
                            linkedMets = self.extractLinksFrom(smap)
                            rootSmapNode = self.gdbc.findNodeByProperty("Aggregation", "name", smapName)
                            for pathToLinkedMets in linkedMets:
                                irodsPathToLinkedMets = self.gdbc.root + pathToLinkedMets.replace("file:/","").replace("___", ":")
                                returnVal = self.syncLinkedMets(rootSmapNode, irodsPathToLinkedMets)
                                if returnVal is not None:
                                    if returnVal < 0:
                                        return -1
                            
                logger.info("Sync completed")
            except pyxb.exceptions_.UnrecognizedContentError:
                logger.error("UnrecognizedContentError: "+traceback.format_exc())
            except sax._exceptions.SAXParseException:
                logger.error("SAXParseException: "+traceback.format_exc())
            except:
                logger.error(traceback.format_exc())
    
    def checkForDeletedSubgraphs(self, linksDiff, metsComparator):
        metsComparator.sortOutDeletedLinks(linksDiff, self.mets_from_old_manifest.fileSec,
                                           self.mets_from_new_manifest.fileSec, self.gdbc.root)
        #check with ils in irods if the sub collection still exists
        disconnectedLinks = linksDiff["disconnectedLinks"]
        deletedLinks = linksDiff["deletedLinks"]
        for pathToLinkedMets in linksDiff["disconnectedLinks"]:
            irodsPathToLinkedMets = self.gdbc.root + pathToLinkedMets.replace("file:/","")
            pathParts = irodsPathToLinkedMets.rsplit(os.sep, 1)
            fileName = pathParts[1]
            collPath = pathParts[0]
            
            #check if the linked collection exist
            out = self.irodsu.listDir(collPath)
            if out is not None:
                irodsFilesMap = out[1]
                fileNames = []
                for key, val in irodsFilesMap.iteritems():
                    for key, val in val.iteritems():
                        if "__files__" in key:
                            for fileName in val:
                                fileNames.append(fileName)
                #check if the linked manifest exist     
                if fileName not in fileNames:
                    disconnectedLinks.remove(pathToLinkedMets)
                    deletedLinks.append(pathToLinkedMets)
            else:
                disconnectedLinks.remove(pathToLinkedMets)
                deletedLinks.append(pathToLinkedMets)
        logger.debug("deletedLinks: " + str(deletedLinks))
        logger.debug("disconnectedLinks: " + str(disconnectedLinks))
        linksDiff["deletedLinks"] = deletedLinks
        linksDiff["disconnectedLinks"] = disconnectedLinks
                   
    def syncLinkedMets(self, rootSmapNode, pathToLinkedMets):
        #before follow the link, check if not already synchronized, to avoid loops
        if pathToLinkedMets not in self.sychronizedPaths:
            self.sychronizedPaths.append(pathToLinkedMets)
            logger.debug("syncLinkedMets " + pathToLinkedMets)
            
            #follow the link, get the smap
            linkedMetsXMLtext = self.irodsu.getFile(pathToLinkedMets)
            linkedSmaps = self.mp.parse(linkedMetsXMLtext)
            for linkedSmapName in linkedSmaps.keys():      
                #should be only one in
                if linkedSmapName is None:
                    logger.error("no smap LABEL to find the root node in the graph "+
                                 " found in: "+pathToLinkedMets)
                
                #entry in the recursion if linked smap has further links
                returnVal = self.createOrUpdateGraph(pathToLinkedMets.rsplit('/',1)[0])
                
                if returnVal is not None:
                    if returnVal < 0:
                        return -1
                
                #connect the root node (of the smap) to the linked one 
                #with the IS_LINKED_TO relation
                linkedSmapNode = self.gdbc.findNodeByProperty("Aggregation", "name",
                                                               linkedSmapName)
                if linkedSmapNode is not None:
                    if self.configuration.dryrun: 
                        print("create relation between rootSmap: " 
                              + rootSmapNode.name + 
                              " and linkedSmap: " + linkedSmapName)
                    else:
                        smap_is_linked_to = Relationship(linkedSmapNode, "IS_LINKED_TO",
                                                          rootSmapNode)
                        #if already connected will do nothing
                        self.gdbc.graph.create_unique(smap_is_linked_to)
                        logger.debug('Created relation: ' + str(smap_is_linked_to))
                else:
                    logger.error("no smap node with name: "+linkedSmapName)
        else:
            logger.info("path already synchronized: "+pathToLinkedMets)
    
    def getManifestFilesFrom(self, path):
        #get all file names that are directly in the collection ()
        out = self.irodsu.listDir(path)
        irodsFilesMap = out[1]
        fileNames = []
        for key, val in irodsFilesMap.iteritems():
            for key, val in val.iteritems():
                if "__files__" in key:
                    for fileName in val:
                        fileNames.append(fileName)
        
        newManifestPath = ""
        oldManifestPath = ""
        manifests = []   
        for fName in fileNames:
            if "manifest" in fName:
                manifests.append(fName)  
        if manifests:
            manifests.sort()
            orderedManifests = manifests
            lastIndex = len(orderedManifests) - 1
            if lastIndex == 1:
                newManifestPath = orderedManifests[lastIndex]
                oldManifestPath = orderedManifests[lastIndex-1]
            elif lastIndex == 0:
                newManifestPath = orderedManifests[lastIndex]
            else:
                logger.error("unexpected number of manifests")
        
        newManifestFile = None
        oldManifestFile = None
        if newManifestPath != "":
            logger.debug("new METS manifest path: " + path +"/"+ newManifestPath)
            self.newManifestPath = path +"/"+ newManifestPath
            newManifestFile = self.irodsu.getFile(path +"/"+ newManifestPath)
        if oldManifestPath != "":
            logger.debug("old METS manifest path: " + path +"/"+ oldManifestPath)
            self.oldManifestPath = path +"/"+ oldManifestPath
            oldManifestFile = self.irodsu.getFile(path +"/"+ oldManifestPath)
        self.sychronizedPaths.append(path +"/"+ newManifestPath)
        
        result = {}
        result["newManifestFile"] = newManifestFile
        result["oldManifestFile"] = oldManifestFile
        return result
        
    def updateLinksWith(self, linksDiff, smapName):
        disconnectedLinks = linksDiff["disconnectedLinks"]
        #disconnected link -> delete relation
        for disconnectedLink in disconnectedLinks:
            linkedCollectionName = disconnectedLink.rsplit('/',2)[1]
            print("disconnectedLink linkedCollectionName: "+linkedCollectionName)
            result = self.gdbc.getLinkedNodes(smapName)
            nodesList = result.to_subgraph().nodes
            print(str(nodesList))
            for linkedNode in nodesList:
                linkedNodeName = linkedNode["name"]
                if linkedCollectionName in linkedNodeName:
                    self.gdbc.deleteLinkedToRelationBetween(smapName, linkedNodeName)
        
        #deleted link -> delete subgraph
        deletedLinks = linksDiff["deletedLinks"]
        for deletedLink in deletedLinks:
            linkedCollectionName = deletedLink.rsplit('/',2)[1]
            result = self.gdbc.getLinkedNodes(smapName)
            nodesList = result.to_subgraph().nodes
            for linkedNode in nodesList:
                linkedNodeName = linkedNode["name"]
                print("XXXXX "+ linkedCollectionName + " : " +linkedNodeName)
                if linkedCollectionName in linkedNodeName:
                    self.gdbc.deleteSubgraph(linkedNodeName)
        
        #added -> synchLink: createOrUpdate subgraph and create a relation between given smap and the linked one
        addedLinks = linksDiff["addedLinks"]
        rootSmapNode = self.gdbc.findNodeByProperty("Aggregation", "name", smapName)
        for addedLink in addedLinks:
            irodsPathToLinkedMets = self.gdbc.root + addedLink.replace("file:/","").replace("___", ":")
            self.syncLinkedMets(rootSmapNode, irodsPathToLinkedMets)
            
    def updateGraphWith(self, diff, smap):
        newFilePathMap = self.getFilePathMapFrom(smap)
        for k, v in diff.iteritems():
            if k == MetsManifestComparator.STRUCT_MAP_CHANGES:
                structMapDiff = v
                for key, val in structMapDiff.iteritems():
                    collectionName = key.replace(MetsManifestComparator.LOGICAL_COLLECTION_CHANGE,"")
                    
                    if MetsManifestComparator.LOGICAL_COLLECTION_CHANGE in str(key):
                        addedDivs = val[MetsManifestComparator.ADDED_DIVS]
                        self.gdbc.updateGraphAddingNodes(addedDivs, collectionName, newFilePathMap)
                        
                        deletedDivs = val[MetsManifestComparator.DELETED_DIVS]
                        self.gdbc.updateGraphDeletingNodes(deletedDivs)
                    elif MetsManifestComparator.ADDED_DEFAULT_DIV in str(key):
                        self.gdbc.updateGraphAddingDefaultDiv(val, smap, newFilePathMap)
                    elif MetsManifestComparator.DELETED_DEFAULT_DIV in str(key):
                        self.gdbc.updateGraphDeletingDefaultDiv(val)
                    else:
                        logger.info('ERROR: should not happen, unknown key in the diff map') 
            else:
                print("fileSec Changes not needed for now")
    
    def extractLinksFrom(self, smap):
        #mp.parse(xmltext) collects all mptr path's in list smap['linkedMets']
        links = []
        for obj in smap['nestedObjects']:
            if obj['linkedMets']:
                linkArr = obj['linkedMets']
                link = linkArr[0]
                links.append(link)
        
        return links
    
    def getFilePathMapFrom(self, smap):
        filePaths = {}
        for obj in smap['nestedObjects']:
            if len(obj['nestedObjects']) > 0 :
                self.getFilePathMapRecursively(obj['nestedObjects'], filePaths)
            else:
                for fileId, path in obj['filePaths'].iteritems():
                    filePaths[fileId] = path
        
        return filePaths
        
    def getFilePathMapRecursively(self, nestedObjectsList, filePaths):
        for obj in nestedObjectsList:
            if len(obj['nestedObjects']) > 0 :
                self.getFilePathMapRecursively(obj['nestedObjects'], filePaths)
            else:
                for fileId, path in obj['filePaths'].iteritems():
                    filePaths[fileId] = path
    
    def isManifestValid(self, path, manifestPath, xmltext):
        #imeta auf manifest, getting VALIDATION_STATUS COMPLETED if not existent, not validated
        metadata = self.irodsu.getAllMetadata(manifestPath)
        if metadata is not None:
            if len(metadata) > 0:
                print("meta data found: "+str(metadata))
                if metadata["VALIDATION_STATUS"] is not None:
                    validationResult = {}
                    validationResult["mets"] = None
                    if metadata["IS_CONSISTENT"] is not None & metadata["ALL_FILES_EXISTING"] is not None:
                        validationResult["isValid"] = metadata["IS_CONSISTENT"] & metadata["ALL_FILES_EXISTING"]
                        return validationResult
                    else:
                        return self.validateManifest(path, xmltext)
        #if the manifest is not validated before, validate manifest
        return self.validateManifest(path, xmltext)
    
    def validateManifest(self, path, xmltext):
        mets_from_manifest = CreateFromDocument(xmltext)
        irodsFilesMap = self.irodsu.deepListDir(path)
        validator = MetsManifestValidator(path, self.debug, self.dryrun, logger)
        missingFilesIDs, notExistingFiles = validator.validateMetsManifestFile(mets_from_manifest, irodsFilesMap[1])
               
        is_consistent_flag = True
        all_files_existing_flag = True
        if len(missingFilesIDs) > 0:
            is_consistent_flag = False
        if len(notExistingFiles) > 0:
            all_files_existing_flag = False
        
        validationResult = {}
        validationResult["mets"] = mets_from_manifest
        validationResult["isValid"] = is_consistent_flag & all_files_existing_flag
        return validationResult
        
################################################################################
# B2SAFE GraphDB client Command Line Interface                                 #
################################################################################
def sync(args):
    logger.info("Sync starting ...")
    configuration = Configuration(args.confpath, args.debug, args.dryrun, logger)
    configuration.parseConf();
    
    irodsu = manifest.IRODSUtils(configuration.irods_home_dir, logger, configuration.irods_debug)
    path = args.path
    gdbc = GraphDBClient(configuration, path, irodsu)
    mp = manifest.MetsParser(configuration, logger)
    
    if args.user:
        logger.info('Working as iRODS user ' + args.user[0])
        irodsu.setUser(args.user[0])
    
    synchronizer = GraphDBSynchronizer(args.debug, args.dryrun, configuration, gdbc, mp, irodsu)
    synchronizer.createOrUpdateGraph(path)
        
    if args.user:
        irodsu.unsetUser()
    
    logger.info("Sync END")
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='B2SAFE graphDB client')
    parser.add_argument("confpath", help="path to the configuration file")
    parser.add_argument("-dbg", "--debug", action="store_true", 
                        help="enable debug")
    parser.add_argument("-d", "--dryrun", action="store_true",
                        help="run without performing any real change")
    parser.add_argument("-u", "--user", nargs=1, help="irods user")
    parser.add_argument("path", help="irods path to the data")
    
    parser.set_defaults(func=sync) 
    
    args = parser.parse_args()
    args.func(args)
