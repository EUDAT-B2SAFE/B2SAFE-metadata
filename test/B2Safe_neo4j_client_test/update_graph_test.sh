#!/bin/bash

set -x

IRODS_USER=irods
SCRIPT_HOME=/home/irods/B2SAFE-core/scripts/metadata
B2SAFE_NEO4J_CLIENT=${SCRIPT_HOME}/b2safe_neo4j_client.py
METS_FACTORY=${SCRIPT_HOME}/mets_factory.py
IRODS_HOME_PATH=/JULK_ZONE/home/irods/julia
OLD_MANIFEST_NAME=$(ils ${IRODS_HOME_PATH}/collection_A/ | grep EUDAT_manifest_METS | sed 's/^[ \t]*//;s/[ \t]*$//')

iget -r ${IRODS_HOME_PATH}/collection_A/${OLD_MANIFEST_NAME}

rm -r collection_A

#switch metadata.json and collection
cp conf/metadata_new.json conf/metadata.json
cp -r collection_A_new collection_A

#clear iRODS
irm -r ${IRODS_HOME_PATH}/collection_A/

#put new collection into iRODS
iput -frb collection_A ${IRODS_HOME_PATH}/

#create manifest.xml
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A/subCollection_C $(pwd)/conf/mets_factory.conf
sleep 1
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A $(pwd)/conf/mets_factory.conf

#put the old manifest into new collection in iRODS
iput -f ${OLD_MANIFEST_NAME} ${IRODS_HOME_PATH}/collection_A/

#update graph
python ${B2SAFE_NEO4J_CLIENT} -dbg -u ${IRODS_USER} $(pwd)/conf/b2safe_neo4j.conf ${IRODS_HOME_PATH}/collection_A 

#clean up
rm -r collection_A $(pwd)/conf/metadata.json ${OLD_MANIFEST_NAME}
