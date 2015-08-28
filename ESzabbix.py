#!/usr/bin/env python2.7

# Created by Aaron Mildenstein on 19 SEP 2012

from pyes import *
import sys
import os

# Define the fail message
def zbx_fail():
    print "ZBX_NOTSUPPORTED"
    sys.exit(2)
    
searchkeys = ['query_total', 'fetch_time_in_millis', 'fetch_total', 'fetch_time', 'query_current', 'fetch_current', 'query_time_in_millis', 'open_contexts']
getkeys = ['missing_total', 'exists_total', 'current', 'time_in_millis', 'missing_time_in_millis', 'exists_time_in_millis', 'total']
docskeys = ['count', 'deleted']
indexingkeys = ['delete_time_in_millis', 'index_total', 'index_current', 'delete_total', 'index_time_in_millis', 'delete_current']
storekeys = ['size_in_bytes', 'throttle_time_in_millis']
cachekeys = ['filter_size_in_bytes', 'field_size_in_bytes', 'field_evictions']
warmerkeys = ['total', 'total_time_in_millis']
refreshkeys = ['total', 'total_time_in_millis']
mergeskeys = ['total', 'total_time_in_millis', 'total_docs','total_size_in_bytes']
filterkeys = ['memory_size_in_bytes', 'evictions']
segmentkeys = ['count', 'memory_in_bytes', 'index_writer_memory_in_bytes', 'index_writer_max_memory_in_bytes', 'version_map_memory_in_bytes', 'fixed_bit_set_memory_in_bytes']
jvmkeys = ['heap_committed_in_bytes', 'heap_used_in_bytes', 'heap_used_percent', 'heap_max_in_bytes', 'non_heap_used_in_bytes', 'non_heap_committed_in_bytes']
clusterkeys = searchkeys + getkeys + docskeys + indexingkeys + storekeys + warmerkeys + refreshkeys + filterkeys + segmentkeys + jvmkeys
allowed_keys = ['indexing', 'search', 'get', 'docs', 'cache', 'warmer', 'refresh', 'merges', 'filter', 'segments', 'mem', 'store']
returnval = None

# __main__

# We need to have two command-line args: 
# sys.argv[1]: The node name or "cluster"
# sys.argv[2]: The subcategory
# sys.argv[3]: The "key" (status, filter_size_in_bytes, etc)

# if len(sys.argv) < 4:
    # zbx_fail()

# Try to establish a connection to elasticsearch
try:
    conn = ES('localhost:9200',timeout=25,default_indices=[''])
except Exception, e:
    zbx_fail()

def es_stats(conn, nodes=None):
    """
    The cluster :ref:`nodes info <es-guide-reference-api-admin-cluster-nodes-stats>` API allows to retrieve one or more (or all) of
    the cluster nodes information.
    """
    parts = ["_nodes", "stats"]
    if nodes:
        parts = ["_nodes", ",".join(nodes), "stats"]
    
    path = make_path(*parts)
    return conn._send_request('GET', path)

if sys.argv[1] == 'cluster':
    if sys.argv[3] in clusterkeys:
        nodestats = es_stats(conn)
        subtotal = 0
        for nodename in nodestats['nodes']:
            if sys.argv[2] in allowed_keys:
		if sys.argv[2] == 'mem':
                    indexstats = nodestats['nodes'][nodename]['jvm'][sys.argv[2]]
		else:
                    indexstats = nodestats['nodes'][nodename]['indices'][sys.argv[2]]
            else:
                zbx_fail()
            try:
                subtotal += indexstats[sys.argv[3]]
            except Exception, e:
                pass
        returnval = subtotal


    else:
        # Try to pull the managers object data
        try:
            escluster = managers.Cluster(conn)
        except Exception, e:
            zbx_fail()
        # Try to get a value to match the key provided
        try:
            returnval = escluster.health()[sys.argv[2]]
        except Exception, e:
            zbx_fail()
        # If the key is "status" then we need to map that to an integer
        if sys.argv[2] == 'status':
            if returnval == 'green':
                returnval = 0
            elif returnval == 'yellow':
                returnval = 1
            elif returnval == 'red':
                returnval = 2
            else:
                zbx_fail()

# Mod to check if ES service is up
elif sys.argv[1] == 'service':
    if sys.argv[2] == 'status':
        try:
            conn.status()
            returnval = 1
        except Exception, e:
            returnval = 0

else: # Not clusterwide, check the next arg

    # ZABBIX has the FQDN, and ES wants the short name.  SIGH

    localname = os.uname()[1]

    nodestats = es_stats(conn)
    # print nodestats
    for nodename in nodestats['nodes']:
        if localname in nodestats['nodes'][nodename]['name']:
            if sys.argv[2] in allowed_keys:
	        if sys.argv[2] == 'mem':
	            stats = nodestats['nodes'][nodename]['jvm'][sys.argv[2]]
	        else:
	            stats = nodestats['nodes'][nodename]['indices'][sys.argv[2]]
            else:
	        zbx_fail()
            try:
	        returnval = stats[sys.argv[3]]
            except Exception, e:
	        pass


# If we somehow did not get a value here, that's a problem.  Send back the standard 
# ZBX_NOTSUPPORTED
if returnval is None:
    zbx_fail()
else:
    print returnval

# End

