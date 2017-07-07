#!/bin/bash

set -x

source test_config.sh

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
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A $(pwd)/conf/mets_factory.conf

#copy xml in temp file
iget ${IRODS_HOME_PATH}/collection_A/manifest.xml EUDAT_manifest_METS.2017-05-23.13:19:32.xml

#save manifest under UEDAT name and copy to a file with timestamp
iput -f EUDAT_manifest_METS.2017-05-23.13:16:38.xml ${IRODS_HOME_PATH}/collection_A/
iput -f EUDAT_manifest_METS.xml ${IRODS_HOME_PATH}/collection_A/
iput -f EUDAT_manifest_METS.2017-05-23.13:19:32.xml ${IRODS_HOME_PATH}/collection_A/
irm -r ${IRODS_HOME_PATH}/collection_A/manifest.xml

#update graph
python ${B2SAFE_NEO4J_CLIENT} -dbg -confpath $(pwd)/conf/b2safe_neo4j.conf -path ${IRODS_HOME_PATH}/collection_A -u irods

#clean up
rm -r collection_A $(pwd)/conf/metadata.json EUDAT_manifest_METS.2017-05-23.13:19:32.xml
