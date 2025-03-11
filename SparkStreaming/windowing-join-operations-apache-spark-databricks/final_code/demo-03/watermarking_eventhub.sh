
Create namespace

# Go to resource group which is already created 'loony_databricks_rg'
# Then search 'Event hubs' -> And Choose event hub
# Click on create 
# Choose resource group
# And give following details while creating the namespace

	name : loony-eventhub-namespace
	region : East US
	pricing tier : Basic

# Click review + create
# Create the namespace
# once it is ready navigate to the namespace

Create eventhub

# Click on +Event hub
# Add following setails
	name : loony-sales

# Click on create
# Event hub has created in the namespace
# We can see the eventhub at the bottom in the namespace page

# We need two details from this page now
	name : loony-sales
	primary key

# To get the primary key go to 'Shared access policies' which is there in the left side of the namespace page
# Then click on the 'RootManageSharedAccessKey' 
# Then the right side keys will be active
# Copy the connection string primary key

	Connection string–primary key:
	Endpoint=sb://loony-eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=eKnj1tqnC6UJdhA9I2IVN1rvh2oVcc6Ubd2Cnvr4PQ0=


# Go to databricks
# Click on Workspace > Create > Library
# Select Maven
# Click on search packages: Give - azure-eventhubs-spark_2.12
# Choose com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21
# Click on Create
# Install the library on the current cluster


# Go to the Databricks workspace
# Have Watermarking open on one tab
# Have SalesSource open on another tab

# Go to the demo_03_Watermarking and run the first few cells there
# Remaining recording notes will be in the demo_03_Watermarking notebook

























