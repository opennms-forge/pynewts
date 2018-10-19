#!/usr/bin/env python
import datetime, time
from utils import df, round_down, ValueType,decompose
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Range of time which we wish to query
end = datetime.datetime.now() # now
start = end + datetime.timedelta(-30) # 30 days ago

# The foreign source and foreign id of the node we wish to query (assumes your using OpenNMS w/ storeByForeignSource = true)
foreign_source = 'NODES'
foreign_id = 'localhost'

# Now let's grab a session to our Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('newts')

# First, let's find the resource ids for metrics related to the given node
# select resource from terms where context='G' and field='_idx2' and value = '(snmp:fs:NODES,5)';
MIN_RESOURCE_DEPTH = 4
MAX_RESOURCE_DEPTH = 8
CONTEXT = 'G'

class Resource:
    def __init__(self, key):
        self.key = key
        self.attributes = []

resources = []
query = SimpleStatement('SELECT resource FROM terms WHERE context = %s AND field = %s and value = %s')
for depth in range(MIN_RESOURCE_DEPTH,MAX_RESOURCE_DEPTH):
    field = '_idx3'
    value = '(snmp:fs:%s:%s,%d)' % (foreign_source, foreign_id, depth)
    for row in session.execute(query, (CONTEXT, field, value)):
        resources.append(Resource(row.resource))

# Now gather the resource level attributes for every resource (i.e. strings.property values)
query = SimpleStatement('SELECT attribute,value FROM resource_attributes WHERE context = %s AND resource = %s')
print("Resources on %s:%s :" % (foreign_source,foreign_id))
for resource in resources:
    resource.attributes = []
    for row in session.execute(query, (CONTEXT, resource.key)):
        resource.attributes.append((row.attribute,row.value))
    print("%s: %s" % (resource.key, resource.attributes))

# Convert the times to timestamps expressed in seconds
start_ts = time.mktime(start.timetuple())
end_ts = time.mktime(end.timetuple())
shard = 604800 # Match the value of the resource shard set in your install - see org.opennms.newts.config.resource_shard

# Compute the partition keys
first_partition = int(round_down(start_ts, shard))
last_partition = int(round_down(end_ts, shard)) + shard

partitions = []
for partition in range(first_partition, last_partition, shard):
    print("Partition: %d includes data from: %s, to %s" % (partition, df(partition), df(partition+shard)))
    partitions.append(partition)

# Gather the samples for every resource in every partition
query = SimpleStatement('SELECT * from samples WHERE context = %s AND partition = %s and resource = %s')
for resource in resources:
    # Execute the query for each partition
    for partition in partitions:
        for row in session.execute(query, (CONTEXT, partition, resource.key)):
            print(row.context, row.partition, row.resource, row.collected_at, row.metric_name, row.attributes, decompose(row.value))
