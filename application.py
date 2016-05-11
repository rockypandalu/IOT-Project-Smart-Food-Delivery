from flask import render_template, jsonify, Flask, request
import time
from grovepi import *
import grovepi
import json
import urllib
from collections import OrderedDict
import time
import boto3
import botocore.session
import boto.sns
import logging
import math
import requests
from threading import Thread
from apns import APNs, Frame, Payload
apns = APNs(use_sandbox=True, cert_file='--Marked--.pem', key_file='--Marked--.pem')

dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id="--Marked--",
                          aws_secret_access_key="--Marked--",
                          region_name="us-east-1")

application = app = Flask(__name__)

order_dict_1 = {'ID':'0', 'box':2 ,'relay':4, 'status':0}
order_dict_2 = {'ID':'0', 'box':7 ,'relay':5, 'status':0}

dht_sensor_port_1 = 8   #outside

#Send push notifications
def apns_sns_send():
    payload = Payload(alert='Your food has arrived!', badge=1, sound='default', category=None)
    print payload
    apns.gateway_server.send_notification('--Marked--', payload)

#Measure humidity to decide if calling cleaning servecies
def humexp(box):
    global order_dict_1
    global order_dict_2
    table = dynamodb.Table('HumidityExperiment')
    data1 = {}
    tem_list_8 = []
    time_count = 0
    dht_sensor_port_2 = int(box['box'])
    while(time_count<=300):
	try:
	    [ temp1,hum1 ] = dht(dht_sensor_port_1,0)
            [ temp2,hum2 ] = dht(dht_sensor_port_2,0)
	    hum1=float(hum1)
            hum2=float(hum2)
	    if str(hum1)!='nan' and str(hum2)!='nan':
                time_count+=1
	        data1['t'+str(time_count)] = str(hum2-hum1)
	except:
            pass
	time.sleep(1)
    if box['box']==2:
        data1['ID']='1_'+str(time.time())
    else:
	data1['ID']='2_'+str(time.time())
    table.put_item(Item=data1)
    if order_dict_1 == box:
    	order_dict_1['status'] = 3
    else:
	order_dict_2['status'] = 3
    print 'hum det done'

#Open box for delivery guys
@app.route('/deliver', methods = ['GET', 'POST', 'PUT'])
def deliver():
    # Get the current uuid
    dic = dict(request.form)
    for each in dic:
        uuid = each
        break
    print uuid
    #Make sure it is in the table in DynamoDB
    orders=dynamodb.Table('Order')
    orders = orders.scan()
    for order in orders["Items"]:
        if order['ID']==uuid:
	    if order_dict_1['status']==0:
		order_dict_1['status']=1
		order_dict_1['ID']=uuid
		relay = order_dict_1['relay']
        	grovepi.pinMode(relay,"OUTPUT")
        	grovepi.digitalWrite(relay,1)
        	time.sleep(5)
        	grovepi.digitalWrite(relay,0)
		apns_sns_send()
		print 'box 1 put in food'
	    elif order_dict_2['status']==0:
                order_dict_2['status']=1
                order_dict_2['ID']=uuid
                relay = order_dict_2['relay']
                grovepi.pinMode(relay,"OUTPUT")
                grovepi.digitalWrite(relay,1)
                time.sleep(5)
                grovepi.digitalWrite(relay,0)
		print 'box 2 put in food'
		apns_sns_send()
    return 'delivery'

#Open box for users
@app.route('/openbox', methods = ['GET', 'POST', 'PUT'])
def openbox():
    # Get the current uuid
    dic = dict(request.form)
    for each in dic:
        uuid = each
        break
    print uuid
    # Make sure the order number is valid
    if uuid == order_dict_1['ID'] and order_dict_1['status']==1:
        relay = order_dict_1['relay']
	grovepi.pinMode(relay,"OUTPUT")
        grovepi.digitalWrite(relay,1)
        time.sleep(5)
        grovepi.digitalWrite(relay,0)
	order_dict_1['ID'] = '0'
	order_dict_1['status'] = 2
        t = Thread(target=humexp, args=(order_dict_1,))
        t.daemon = True
        t.start()
	print 'food in box 1 took out'
        return 'opened'
    elif uuid == order_dict_2['ID'] and order_dict_2['status']==1:
        relay = order_dict_2['relay']
        grovepi.pinMode(relay,"OUTPUT")
        grovepi.digitalWrite(relay,1)
        time.sleep(5)
        grovepi.digitalWrite(relay,0)
	order_dict_2['ID'] = '0'
	order_dict_2['status'] = 2
        t = Thread(target=humexp, args=(order_dict_2,))
        t.daemon = True
        t.start()
	print 'food in box 2 took out'
        return 'opened'
    else:
	return 'no match'


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
