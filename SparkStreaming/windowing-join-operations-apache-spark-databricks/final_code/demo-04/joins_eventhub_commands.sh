
Create namespace
# Go to resource group which has already created 'loony_databricks_rg'
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

# Go to the event hub namespace that we have already created
# loony-eventhub-namespace

Create eventhub

# Click on +Event hub
# Add following setails

	name : loony-ratings

# Click on create
# Event hub has created in the namespace
# We can see the eventhub at the bottom in the namespace page

# We need two details from this page now
	name : loony-ratings
	primary key

# To get the primary key go to 'Shared access policies' which is there in the left side of the namespace page
# Then click on the 'RootManageSharedAccessKey' 
# Then the right side keys will be active
# Copy the connection string primary key

	Connection string–primary key:
	Endpoint=sb://loony-eventhub-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=eKnj1tqnC6UJdhA9I2IVN1rvh2oVcc6Ubd2Cnvr4PQ0=

# Go to the Databricks workspace
# Have StreamingStaticJoins open on one tab
# Have RatingsSource open on another tab
# Start with the code in StreamingStaticJoins























