#!/bin/bash

set -x

source test_config.sh

#clear iRODS / create new folder in iRODS
irm -r ${IRODS_HOME_PATH}/collection_A/

rm -r collection_A EUDAT_manifest_METS.2017-05-23.13:16:38.xml EUDAT_manifest_METS.xml

#switch metadata.json and collection
cp conf/metadata_old.json conf/metadata.json
cp -r collection_A_old collection_A

#create collection in iRODS
iput -frb collection_A ${IRODS_HOME_PATH}/

#create manifest.xml
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A/subCollection_B $(pwd)/conf/mets_factory.conf
python ${METS_FACTORY} -dbg -i ${IRODS_HOME_PATH}/collection_A $(pwd)/conf/mets_factory.conf

#save manifest under UEDAT name and copy to a file with timestamp
iget -f ${IRODS_HOME_PATH}/collection_A/manifest.xml EUDAT_manifest_METS.2017-05-23.13:16:38.xml
iget -f ${IRODS_HOME_PATH}/collection_A/manifest.xml EUDAT_manifest_METS.xml

irm -r ${IRODS_HOME_PATH}/collection_A/manifest.xml
iput -f EUDAT_manifest_METS.2017-05-23.13:16:38.xml ${IRODS_HOME_PATH}/collection_A/
iput -f EUDAT_manifest_METS.xml ${IRODS_HOME_PATH}/collection_A/

#create graph
python ${B2SAFE_NEO4J_CLIENT} -dbg -confpath $(pwd)/conf/b2safe_neo4j.conf -path ${IRODS_HOME_PATH}/collection_A -u irods
