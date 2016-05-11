import boto3
import boto.dynamodb2
import math
import decimal
import csv
import time
from datetime import datetime
from collections import OrderedDict
from apns import APNs, Frame, Payload

dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id="",
                          aws_secret_access_key="",
                          region_name="us-east-1")
                        )
table_store = dynamodb.Table('order_storage')
table_information = dynamodb.Table('Order')
snsclient=boto3.client('sns')
snsTopicArn = ''

apns = APNs(use_sandbox=True, cert_file=, key_file=)

notelist={}
client_token = ''

def apns_sns_send(token, mes):
    token_hex = token
    apns.gateway_server.send_notification(token_hex, payload)

try:
    while True:
    	t=time.time()
    	nowtime=int(((t-14400)%86400)/60)
    	#scan table to get all existing order time information
        result_all=table_store.scan()
		result_all=list(result_all['Items'])
		if (len(result_all)!=0):
			for result in result_all:
				#new information
				if (result['RestaurantOrderTime']>=nowtime):
					cor_info_raw=table_information.get_item(Key={'ID':result['ID']})
					cor_info=cor_info_raw['Item']
					info={'RestaurantOrderTime':result['RestaurantOrderTime'], 'Restaurant':cor_info['Restaurant'], 'Order':cor_info['Order']}
					notelist[result['ID']]=info
		print notelist
		newnote={}
		for key in notelist:		
			if (notelist[key]['RestaurantOrderTime']<=nowtime):
				#time to send notification to restaurant	
				output=notelist[key]['Order']
				response=snsclient.publish(TopicArn=snsTopicArn,Message=output)
				payload = Payload(alert='You have a new order!', badge=1, sound='default', category=None, custom={'food':notelist[key]['Order'],'uuid':key})
				apns_sns_send(client_token, payload)
			else:
				#not the time to send notification
				newnote[key]=notelist[key]
		notelist=newnote
		print notelist	
		#wait for 60s			
		time.sleep(60)

except KeyboardInterrupt:
    exit
