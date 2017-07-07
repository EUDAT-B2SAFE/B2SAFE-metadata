
 - Before running the tests:
	- please create a b2safe_neo4j.conf from conf/b2safe_neo4j.conf_template with you connection information to neo4j and iRODS
	- then change the paths in test.sh
	
 - first run the create_graph_test.sh. This will inject a colleciton into iRODS and run first mets_factory script and then the b2safe_neo4j_client. If it runns without error you can check in neo4j browser if the graph was created. There schould be one top collection collection_A including a logical collection and some default nodes / files and a subcollection subCollection_B connected over IS_RELATED_TO relation to the top collection.
 
 - then you can run the update_graph_test.sh, that will change the configs and the collection and runn again the mets_factory script and then the b2safe_neo4j_client. The b2safe_neo4j_client will detact then that there is a update needed and will update the graph deleting and adding some nodes in the logical collection and default nodes and deleting the link to subCollection_B and creating a new one from subCollection_C.