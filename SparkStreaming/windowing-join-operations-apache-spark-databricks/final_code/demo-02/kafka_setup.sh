Steps to connect with Kafka

# Create resource group
#   Login to azure portal
#   Click on resource groups -> Click create 
#   		resource group name: "loony_hdinsight_rg"
#   		region : East US


# Now go to the resource group
# Now we have to create virtual network
# Click on create -> search 'virtual network'
# Click on create ->
# Choose our resource group
# 		name : loony_hdinsight_kafka_vnet
#       region : East US
# Click on create

# Now go back to the resource group
# Now we can see the virtual network is there

# At this point we have to create azure hdinsights
# So for that click on create from resource groups
# search 'Azure HDInsight'
# Click on create -> 
# 		cluster name : loony-hdinsight-kafka-cluster
# 		region : east us 2
# Then click on 'select cluster type'
# There will be different types of cluster
# I am going to choose 'Kafka' from there
# Next we have to give login user name and password
# 		username : loony_admin
# 		password : Password@123
# 		ssh user : sshuser (same password)

# Click on next -> It will lead to storage
# 		Storage-account name : loonyhdinsightstorage

# Click on next -> It will lead to 'Security + networking'
# choose our virtual network from here

# Click on next -> It will be go to 'Configuration +  pricing'
# We have to choose the cheapest from all nodes
#       Head node: Change to the one that costs 21.25 INR/hour
#       Zookeeper node: Change to the one that costs 4.32 INR/hour
# 		All we have to change standard disks : 1
# 		Worker node : 3
#       Zookeeper node: Change to the one that costs 4.32 INR/hour

# Click on next : Click Review + Create
# It will take 15-20 min to create the cluster

Configuring kafka for IP address

# Go to our resources -> click on the cluster(loony-hdinsight-kafka-cluster)
# Then click on the url https://loony-hdinsight-kafka-cluster.azurehdinsight.net
# It will ask for the username and password
# 		username : loony_admin
# 		password : Password@123
# Ths will lead to Ambari and we are going to use the kafka from here.

# On the page click on the Config History tab to show what has been set up

# Next click on kafka from left side
# Click on config and search - 'kafka-env ' in the filter
# Go to end of the code and paste following code in that

	IP_ADDRESS=$(hostname -i)
    echo advertised.listeners=$IP_ADDRESS
    sed -i.bak -e '/advertised/{/advertised@/!d;}' /usr/hdp/current/kafka-broker/conf/server.properties
    echo "advertised.listeners=PLAINTEXT://$IP_ADDRESS:9092" >> /usr/hdp/current/kafka-broker/conf/server.properties


# Search for "listener" in the Filter box
# Set to:

PLAINTEXT://0.0.0.0:9092 - to access all ports

# Then click on save -> updates

# Next go to the drop down of the 'Actions' on top right
# Choose 'Turn on maintenance mode' -> Click Okay

# Next go and click on 'Restart' -> Restart all affected 
# Now we will be able to seeall the background operations are restarting

# Once everything has started -> Click on Okay
# Now go to summary we can all the components has started

# IMPORTANT: Please click through and copy the new values for this, they will not always be the same

Kafka : IP Address: 10.1.0.7
	Hostname: wn0-loony.u2wcvf4dyteenp3u0ebclefmcd.bx.internal.cloudapp.net
Kafka : IP Address: 10.1.0.5
	Hostname: wn1-loony.u2wcvf4dyteenp3u0ebclefmcd.bx.internal.cloudapp.net
Kafka : IP Address: 10.1.0.6
	Hostname: wn2-loony.u2wcvf4dyteenp3u0ebclefmcd.bx.internal.cloudapp.net


ZooKeeper Server : IP Address: 10.1.0.11
ZooKeeper Server : IP Address: 10.1.0.12
ZooKeeper Server : IP Address: 10.1.0.13


Creating Kafka topic

# Go to resource group -> Click on hdinsight cluster -> Click on ssh + cluster login 
# Choose the host name from host name
# And copy the ssh command. This is needed for the connection
	ssh sshuser@loony-hdinsight-kafka-cluster-ssh.azurehdinsight.net

# Next click on the cloud shell icon

# Click on the Full Screen icon on the top right oc Cloud Shell so you have Cloud Shell fill almost the entire page

# Paste the ssh command there in the command shell - ssh sshuser@loony-hdinsight-kafka-cluster-ssh.azurehdinsight.net
# Enter the password : Password@123

# Now we have connected to the hdinsight kafka cluster

export TOPIC="advertising"

export ZOOKEEPERS="10.1.0.11,10.1.0.12,10.1.0.13"

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh \
--create --replication-factor 3 --partitions 4 --topic $TOPIC --zookeeper $ZOOKEEPERS

# run the producer using the below command:
export BROKERS="wn0-loony.u2wcvf4dyteenp3u0ebclefmcd.bx.internal.cloudapp.net:9092,wn1-loony.u2wcvf4dyteenp3u0ebclefmcd.bx.internal.cloudapp.net:9092,wn2-loony.u2wcvf4dyteenp3u0ebclefmcd.bx.internal.cloudapp.net:9092"

/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \
--broker-list $BROKERS --topic $TOPIC



# Tab 2
# Show that Hello! is received here
# Kill this consumer, we will no longer use it
# Close the tab


# Go to loony_databricks_rg and click on loony_databricks_workspace
# Under Setting, click on Virtual Network Peerings > Add Peering
# Give the following details:
	Name: loony_databricks_peer
	Virtual Network : loony_hdinsight_kafka_vnet (choose this one)
	Tick on Allow forwarded traffic

	Copy the Databricks Virtual Network Resource ID : /subscriptions/50ee833b-7a1c-449f-ad1f-ab670f962831/resourceGroups/databricks-rg-loony_databricks_workspace-23xmbnbhpg73u/providers/Microsoft.Network/virtualNetworks/workers-vnet

	Click on Add

# Now the status will be Initiated

# Go to Vnet> Peerings
# Click on +Add
# Give the following details
	This virtual network
	Peering link name: loony_databricks_vnet_peer

	Remote virtual network
	Peering link name: loony_remote_databricks_vnet_peer

	Click on I know my resource ID and paste the previously copied resource id 
	Create

# Go to loony_databricks_workspace > Virtual Network Peerings now we see the status is connected
# Launch the workspace and create a new notebook











