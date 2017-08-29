#!/bin/bash

set -x

SCRIPT_HOME=/home/irods/B2SAFE-core/scripts/metadata
B2SAFE_NEO4J_CLIENT=${SCRIPT_HOME}/b2safe_neo4j_client.py
METS_FACTORY=${SCRIPT_HOME}/mets_factory.py
IRODS_HOME_PATH=/JULK_ZONE/home/irods/julia

#clear iRODS / create new folder in iRODS
irm -r ${IRODS_HOME_PATH}/collection_A/

rm -r collection_A 

#switch metadata.json and collection
cp conf/metadata_old.json conf/metadata.json
cp -r collection_A_old collection_A

#create collection in iRODS
iput -frb collection_A ${IRODS_HOME_PATH}/

#create manifest.xml
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A/subCollection_B $(pwd)/conf/mets_factory.conf
sleep 1
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A $(pwd)/conf/mets_factory.conf

#create graph
python ${B2SAFE_NEO4J_CLIENT} -dbg -u irods $(pwd)/conf/b2safe_neo4j.conf ${IRODS_HOME_PATH}/collection_A
