#!/usr/bin/env python
# coding: utf-8

# In[2]:
get_ipython().system('pip install boto3')

# In[48]:
import boto3
# In[49]:
s3 = boto3.resource('s3',
aws_access_key_id='[Access Key Id]',
aws_secret_access_key='[Secret Access Key]' )

# In[50]:
try:
    s3.create_bucket(Bucket='datacont-exps', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'}) 
except Exception as e:
    print (e)
# In[51]:
bucket = s3.Bucket("datacont-exps")
# In[52]:
bucket.Acl().put(ACL='public-read')

# In[75]:
#upload a new object into the bucket
body = open('experiments.csv', 'rb')

# In[76]:
o = s3.Object('datacont-exps', 'test').put(Body=body)

# In[77]:
s3.Object('datacont-exps', 'test').Acl().put(ACL='public-read')
# In[78]:
dyndb = boto3.resource('dynamodb',
    region_name='us-west-2',
    aws_access_key_id='[Access Key Id]',
    aws_secret_access_key='[Secret Access Key]'
)
# In[79]:
try:
    table = dyndb.create_table(
            TableName='DataTable',
            KeySchema=[
                {
                    'AttributeName': 'PartitionKey',
                    'KeyType': 'HASH'
                }, {
                    'AttributeName': 'RowKey',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'PartitionKey',
                    'AttributeType': 'S'
    }, {
                    'AttributeName': 'RowKey',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
    )
except Exception as e:
    print (e)
    #if there is an exception, the table may already exist.
    table = dyndb.Table("DataTable")

# In[80]:
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable')
# In[81]:
print(table.item_count)

# In[82]:
import csv

# In[85]:
with open('experiments.csv', 'r') as csvfile: 
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    for item in csvf:
        print(item)
        body = open(item[5], 'rb') 
        s3.Object('datacont-exps', item[5]).put(Body=body)
        md = s3.Object('datacont-exps', item[5]).Acl().put(ACL='public-read')
        url = " https://s3-us-west-2.amazonaws.com/datacont-exps/" + item[5] 
        metadata_item = {
            'PartitionKey': item[0], 
            'RowKey': item[1],
            'Temp' : item[2], 
            'Conductivity' : item[3], 
            'Concentration': item[4],
            'url':url
        }
        try: 
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")


# In[86]:
try:
    response = table.get_item(Key={
            'PartitionKey': 'experiment1',
            'RowKey': '1'})
    item = response['Item'] 
    print(item)
except e:
    print(e)

# In[87]:
print(response)
