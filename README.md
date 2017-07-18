B2SAFE Metadata Management
===========

This set of scripts aims to get metadata from the B2SAFE service and upload them to a GraphDB (Neo4J).
They rely on the py2neo library v2.0.8.  

There are three main scripts:
 * mets_factory.py: it takes the path of a collection and a document describing the data-metadata relations, as inputs, and builds an XML document, which represents the manifest of that collection. The collection path can be file system based or part of the irods namespace. The data-metadata relation document has to be compliant with the json-ld format and the EUDAT controlled vocabulary, while the manifest is a METS formatted document.
 If the collection specified contains a subcollection that aleady has a METS formated manifest, then a link will be created from the manifest of the top collection to the manifest of the subcollection.
 
 * b2safe_neo4j_client.py: it take in iput the manifest file stored under the iRODS path specified and translate its content together with some metadata got from the B2SAFE service into a graph, which is stored in a neo4j DB. If there is already a graph in the neo4j DB for the collection under the specified path, then the b2safe_neo4j_client will compare the "old" and "new" manifests (that assumed both to be under the collection path), extract the changes and update the graph accordingly.
 If the top collection contains a subcollection also with a manifest file discribing it and the manifest file of the top collection has a link element (mptr) in the structural map pointing to it, than the b2safe_neo4j_client will create a graph for the subcollection and connect this subgraph with the the graph of the top collection with the IS_LINKED_TO relation.
 
 * validate_mets_manifest.py: it takes the path to the manifest file, that needs to be validated, analyse this of inconsistencies inside the mets dokument and if all files referenced are existence in iRODS collection under the same path as the manifest. At the end it writes the validaiton results in the log file and validaiton status in iRODS metadata and as attribute RECORDSTATUS of the metsHdr element inside the manifest.

## mets_factory
In the conf directory there are examples of the two file s required by this script: the metadata.json and the configuration file.
```
$ ./mets_factory.py -h
usage: mets_factory.py [-h] [-dbg] [-d] (-i IRODS | -f FILESYSTEM) confpath

METS factory

positional arguments:
  confpath              path to the configuration file

optional arguments:
  -h, --help            show this help message and exit
  -dbg, --debug         enable debug
  -d, --dryrun          run without performing any real change
  -i IRODS, --irods IRODS
                        irods path
  -f FILESYSTEM, --filesystem FILESYSTEM
                        fs path
```

## b2safe_neo4j_client

In order to get the list of options, just type:
```
$ ./b2safe_neo4j_client.py -h
usage: b2safe_neo4j_client.py [-h] [-dbg] [-d] confpath path

B2SAFE graphDB client

positional arguments:
  confpath       path to the configuration file
  path           irods path to the data

optional arguments:
  -h, --help     show this help message and exit
  -dbg, --debug  enable debug
  -d, --dryrun   run without performing any real change
```
For example, to execute it:
```
$ ./b2safe_neo4j_client.py conf/b2safe_neo4j.conf /cinecaDMPZone/home/rods/test1
```
Where the configuation file should look like this:
```
$ cat conf/b2safe_neo4j.conf 
# section containing the logging options
[Logging]
# possible values: INFO, DEBUG, ERROR, WARNING, CRITICAL
log_level=DEBUG
log_file=log/b2safe_neo4j.log

# section containing the sources for users and projects/groups'information
[GraphDB]
address=localhost:8888
username=
password=
path=/db/data/

[iRODS]
zone_name=cinecaDMPZone
zone_ep=dmp1.novalocal:1247
# list of resources separated by comma: res_name1:res_path1,res_name2:res_path2
resources=cinecaRes1:/mnt/data/irods/Vault
irods_home_dir=/cinecaDMPZone/home
irods_debug=True
```

##validate_mets_manifest
In order to get the list of options, just type:
```
$ validate_mets_manifest.py -h
usage: validate_mets_manifest.py [-h] [-confpath CONFPATH] [-dbg] [-d]
                                 [-path PATH] [-u USER]

B2SAFE manifest validation

optional arguments:
  -h, --help            show this help message and exit
  -confpath CONFPATH    path to the configuration file
  -dbg, --debug         enable debug
  -d, --dryrun          run without performing any real change
  -path PATH            irods path to the manifest to validate
  -u USER, --user USER  irods user
```
The main assumption is that a file called *EUDAT_manifest_METS.xml* is available under the irods path provided as input, which should be the root of the main collection described by the metadata.  
Therefore in this case we expect to find: /cinecaDMPZone/home/rods/test1/EUDAT_manifest_METS.xml.  
The manifest file contains the structural metadata of the collection /cinecaDMPZone/home/rods/test1 and must be compliant to the METS format (http://www.loc.gov/standards/mets/).
Some examples are available in the test directory.

If an update done to the collection the assumption is that there are at least 3 manifest files EUDAT_manifest_METS.xml one copy of this file with a timestamp (EUDAT_manifest_METS.2017-05-23.13:18:30.xml) and one changed manifest with the latest timestamp as sufix e.g. EUDAT_manifest_METS.2017-05-23.13:19:32.xml.
