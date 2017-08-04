################################################################################
#                                                                              #
# EUDAT B2SAFE metadata management rule set                                    #
#                                                                              #
################################################################################


EUDATCreateVersion(*path) {

    logInfo("[EUDATCreateVersion] creating version for *path");
    msiSplitPath(*path, *Coll, *objectName);
    # construct version name
    msiGetSystemTime(*Tim, "human");
    # check whether there is a file extension on the name
    EUDATObjectNameSplit(*objectName, *main, *extension)
    *Vers = *main ++ "." ++ "*Tim" ++ *extension;
    *pathver = *Coll ++ "/" ++ *Vers;
    logDebug("[EUDATCreateVersion] executing msiDataObjCopy (*path, *pathver, forceFlag=)");
    msiDataObjCopy(*path, *pathver, "forceFlag=", *status);
    msiSetACL("default", "own", $userNameClient, *pathver);
    logInfo("[EUDATCreateVersion] *path written as version *pathver");
}


EUDATObjectNameSplit(*objectName, *main, *extension) {

  logDebug("[EUDATObjectNameSplit] splitting *objectName");
  msiStrlen(*objectName, *Lfile);
  *Lsub = int(*Lfile);
  *Iloc = *Lsub -1;
  while (*Iloc >= 0) {
    msiSubstr(*objectName,"*Iloc","1",*Char);
    if (*Char == ".") {
      *Lsub = *Iloc;
      break;
    }
    else {
      *Iloc = *Iloc -1;
    }
  }
  msiSubstr(*objectName,"0","*Lsub",*main);
  *extension = "";
  if(*Iloc != 0) {
    *Iloc = int(*Lfile) - *Lsub;
    msiSubstr(*objectName,"*Lsub","*Iloc",*extension);
  }
  logDebug("[EUDATObjectNameSplit] splitted in *main + *extension");
}


EUDATValidateManifest(*path, *user) {

    getMetadataConfParameters(*mdConfPath)

    # check if the user param is passed as input
    # otherwise use the session variable
    if (*user == "" || *user == "None") {
        *user = $userNameClient
    }
    # check if the input path is a collection
    msiGetObjType(*path,*path_type);
    if (*path_type == '-c') {
        *mpath = "*path/EUDAT_manifest_METS.xml"
    }
    # or the manifest
    else if (*path like "*/EUDAT_manifest_METS.xml") {
        *mpath = *path
    }
    else {
        writeLine("serverLog","ERROR: the path *path is not valid");
        fail;
    }

    msiExecCmd("validate_mets_manifest.py","*mdConfPath -u *user -path '*mpath'",
               "null", "null", "null", *outValMet);
    msiGetStdoutInExecCmdOut(*outValMet, *resp);
    getConfParameters(*msiFreeEnabled, *msiCurlEnabled, *authzEnabled);
    if (*msiFreeEnabled) {
        msifree_microservice_out(*outValMet);
    }

    *validation = bool("false");
    EUDATgetLastAVU(*mpath, "VALIDATION_STATUS", *valid_status);
    if (*valid_status == "COMPLETED") {
        EUDATgetLastAVU(*mpath, "ALL_FILES_EXISTING", *file_exist);
        EUDATgetLastAVU(*mpath, "IS_CONSISTENT", *is_consistent);
        if (*file_exist == "True" && *is_consistent == "True") {
            *validation = bool("true");
        }
    }

    *validation;
}


EUDATStoreMetadata(*collPath, *user) {

    getMetadataConfParameters(*mdConfPath)

    # check if the user param is passed as input
    # otherwise use the session variable
    if (*user == "" || *user == "None") {
        *user = $userNameClient
    }
    # check if the input path is a collection
    msiGetObjType(*collPath,*path_type);
    if (*path_type != '-c') {
        writeLine("serverLog","ERROR: the path *collPath is not valid");
        fail;
    }

    msiExecCmd("b2safe_neo4j_client.py"," -u *user '*mdConfPath' '*collPath'",
               "null", "null", "null", *outMdStore);
    msiGetStdoutInExecCmdOut(*outMdStore, *resp);
    getConfParameters(*msiFreeEnabled, *msiCurlEnabled, *authzEnabled);
    if (*msiFreeEnabled) {
        msifree_microservice_out(*outMdStore);
    }
}


EUDATCheckAndUpdateMetadata(*collPath, *user) {

# list the manifest versions in a collection
# choose the two most recent ones
#
# if there are at least two
#   trigger the neo4client to get the differences
# else
#   trigger the neo4client to validate the manifest
    if (EUDATValidateManifest(*collPath, *user)) {
    # if it is valid create a new graph
        writeLine("serverLog","DEBUG: the collection *collPath has a valid manifest");
        EUDATStoreMetadata(*collPath, *user);
    }
    else {
    # mark as not valid
        writeLine("serverLog","DEBUG: the collection *collPath has not a valid manifest");
    }
}


getMetadataConfParameters(*mdConfPath) {
    *mdConfPath="/opt/eudat/b2safe-metadata/conf/b2safe_neo4j.conf";
}

