from boto.ec2.connection import EC2Connection
from time import sleep
import subprocess
import argparse
from argparse import RawTextHelpFormatter
import calendar
import json
import sys
import datetime
import time
from subprocess import Popen, PIPE
import ConfigParser, os, socket
import sys
import statsmodels.api as sm
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from matplotlib.pylab import rcParams
from dateutil import parser

# Call AWS CLI and obtain JSON output, we could've also used tabular or text, but json is easier to parse.
def make_call(cmdline):#, profile):
    cmd_args = ['aws', '--output', 'json'] + cmdline
    p = Popen(cmd_args, stdout=PIPE)
    res, _ = p.communicate()
    if p.wait() != 0:
        sys.stderr.write("Failed to execute: " + " ".join(cmd_args))
        sys.exit(1)
    if not res:
        return {}
    return json.loads(res)

def iso_to_unix_time(iso):
    return calendar.timegm(time.strptime(iso, '%Y-%m-%dT%H:%M:%S.%fZ'))

def get_price_history(region, hours, inst_type):
	f = open("results" + str(hours) + ".txt","w")
	now = datetime.datetime.utcfromtimestamp(time.time())
	start_time = now - datetime.timedelta(hours)
	#inst_type = "m4.xlarge"
	res = make_call(["ec2", "--region", region,
	                     "describe-spot-price-history",
	                     "--start-time", start_time.isoformat(),
	                     "--end-time", now.isoformat(),
	                     "--instance-types", inst_type,
	                     "--product-descriptions", "Linux/UNIX"])
	for p in res['SpotPriceHistory']:
		cur_time = p['Timestamp']
		cur_az = p['AvailabilityZone']
		cur_price = p['SpotPrice']
		f.write(str(cur_az) + ' ' + str(cur_time) + ' ' + str(cur_price) + '\n')
	f.close()

def parse_regions(file_name, inst_type, region):
	f = open(file_name,"r")
	f1 = open(region + "a.csv","w")
	f2 = open(region + "b.csv","w")
	#f3 = open(region + "c.csv","w")
	for line in f:
		region, timestamp, price = line.split()
		if region == "ap-southeast-1a":
			f1.write(str(timestamp) + "," + str(price) + '\n')
		elif region == "ap-southeast-1b":
			f2.write(str(timestamp) + "," +  str(price) + '\n')
		#elif region == "us-west-2c":
		#	f3.write(str(timestamp) + " " +  str(price) + '\n')
	f.close()
	f1.close()
	f2.close()
	#f2.close()

def pad_region(file_name):
	f = open(file_name + '.csv',"r")
	f1 = open(file_name + '-pad.csv',"w")
	data = {}
	x = ""
	y = ""

	for line in f:
		arr = line.split(',')
		temp = arr[0].split("T")
		#print temp
		temp2 = temp[1].split(":")
		data[(temp[0],temp2[0])] = arr[1]
		#print data[(temp[0],temp2[0])]

	flag = True
	ans = 0.0
	prev_hour = 0
	data2 = {}
	for (d,t) in sorted(data):
		
		t2 = t[0:2]
		
		if(flag):
			prev_hour = t2
			flag = False	
			ans = data[(d,t)]
		elif(t != prev_hour):
			data2[(d,prev_hour)] = ans
			ans = data[(d,t)]
			prev_hour = t2
		else:
			ans = max(ans, data[(d,t)])

	its = 0
	for (d,t) in sorted(data2):
		#print d,t
		if(its > 0):
			if(int(t) != (int(prev)+1)%24):
				length = (int(t) - int(prev) + 24)%24
				for x in range(length - 1):
					f1.write(d + ',' + str((x + 1 + int(prev))%24) + ',' + data2[(prev_d,prev)])
		if(its == len(data2) - 1):
			f1.write(str(d) + ',' + str(t) + ',' + str(data2[(d,t)]))
		else:
			f1.write(d + ',' + t + ',' + data2[(d,t)])
		its += 1
		prev = t
		prev_d = d
	f.close()
	f1.close()

def simplify_data(file_name):
	f = open(file_name + ".csv","r")
	f1 = open(file_name + "-final.csv","w")
	flag = True
	i = 1 
	f1.write("index,price\n")
	for line in f:
		d,t,p = line.split(',')
		#print str(i) + ',' + str(p)
		f1.write(str(i)+','+str(p))
		i += 1 
	f.close()
	f1.close()

def stats(on_demand_price, option, time1):
	
	BidPrices = []
	Avg_Uptime = []
	Avg_Downtime = []
	Availability = []
	Prices = []
	Uptimes = []
	f = open("ap-southeast-1"+ option + ".csv","r")
	flag = True
	for line in f:
		dt,p = line.split(',')
		t = int(dt.split('T')[1].split('.')[0].split(':')[0])
		if(flag == True):
			flag = False
			ending = t - time1
		if t < ending:
			break
		Prices.append(float(p))
		Uptimes.append(parser.parse(dt))
	uptime = 0.0
	downtime = 0.0
	availability = 0.0
	max_price = on_demand_price
	bid = 0.0
	failures = 0
	repairs = 0
	#f1.write(str(on_demand_price) + '\n')
	#f1.write("Bid price,Avg. Uptime(in hrs),Avg. Downtime(in hrs),Availability\n")
	l = len(Prices)
	while bid <= max_price + 0.0001:
		uptime = downtime = failures = repairs = 0
		total = 0
		for i in xrange(l):
			if i > 0:
				 
				if Prices[i] >= bid:
					downtime += (Uptimes[i-1] - Uptimes[i]).seconds
				else:
					uptime += (Uptimes[i-1] - Uptimes[i]).seconds 	
				if Prices[i-1] < bid and bid <= Prices[i]:
					repairs += 1
				elif Prices[i-1] >= bid and bid > Prices[i]:
					failures += 1
		#print bid, failures, repairs
		#print uptime,(Uptimes[0] - Uptimes[-1]).seconds/3600.0
		if uptime == (Uptimes[0] - Uptimes[-1]).seconds:
			BidPrices.append(bid)
			Avg_Uptime.append((Uptimes[0] - Uptimes[-1]).seconds/3600.0)
			Avg_Downtime.append(0.0)
			Availability.append(1.0)
		if failures == 0 or repairs == 0:
			bid += 0.0001
			continue
		mtbf = (uptime*1.0/3600)/failures*1.0
		mttr = (downtime*1.0/3600)/repairs*1.0
		BidPrices.append(bid)
		Avg_Uptime.append(mtbf)
		Avg_Downtime.append(mttr)
		Availability.append(float(mtbf)/float(mtbf+mttr))
		#s = str(float(bid)) + ',' + str(uptime/failures*1.0) + ',' + str(downtime/repairs*1.0) + ',' +  str(float(mtbf)/float(mtbf+mttr))
		#f1.write(s + '\n')
		bid += 0.0001			
	return Prices,BidPrices,Avg_Uptime,Avg_Downtime,Availability

def get_recent_history(price_zone):
	mean_2a = 0.0
	std_2a = 0.0
	sum_2a = 0.0
	sq_2a = 0.0
	for p in price_zone:
		sum_2a += p.price
	mean_2a = sum_2a*1.0/len(price_zone)
	for p in price_zone:
		sq_2a += (p.price - mean_2a)**2	
	
	std_2a = sq_2a*1.0/len(price_zone)
	
	return mean_2a,std_2a

def auto_corr(file_name, lag):
	f = open(file_name + ".csv","r")
	P = []
	for line in f:
		i,p = line.split(',')
		if i == 'index':
			continue
		i = int(i)
		p = float(p)
		P.append(p)
	l = len(P)
	mean_2a = 0.0
	var_2a = 0.0
	sum_2a = 0.0
	sq_2a = 0.0
	for p in P:
		sum_2a += p
	mean_2a = sum_2a*1.0/len(P)
	for p in P:
		sq_2a += (p - mean_2a)**2	
	#print "sum of squares = ", sq_2a
	var_2a = sq_2a*1.0/len(P)
	#print "mean = ",mean_2a
	#print "std_dev = ",var_2a**0.5
	T = 60
	R = []
	R.append(1)
	for t in range(1,T):
		sum_r = 0.0
		for i in xrange(lag):
			sum_r += (P[i] - mean_2a)*(P[i+t] - mean_2a)
		r = sum_r/sq_2a
		R.append(r)
	return R,mean_2a,var_2a**0.5

def get_stl(file1,file2):
	data1 = pd.read_csv(file1 + '.csv',names=['DateTime', 'Price'],
 						index_col=['DateTime'],
 						parse_dates=True)
	data2 = pd.read_csv(file2 + '.csv',names=['DateTime', 'Price'],
 						index_col=['DateTime'],
 						parse_dates=True)
	
	

	decompfreq = 24*60/15*1
	res1 = sm.tsa.seasonal_decompose(data1.Price.interpolate(),
 										freq=decompfreq,
 										model='additive')
	
	res1.resid.dropna(inplace=True)

	res2 = sm.tsa.seasonal_decompose(data2.Price.interpolate(),
 										freq=decompfreq,
 										model='additive')
	res2.resid.dropna(inplace=True)
	#res2.plot()
	#plt.show()
	if np.std(res1.resid) > np.std(res2.resid):
		return 'b'
	else:
		return 'a' 
	
def read_user_data_from_local_config():
        user_data = config.get('EC2', 'user_data')
        if config.get('EC2', 'user_data') is None or user_data == '':
                try:
                        user_data = (open(config.get('EC2', 'user_data_file'), 'r')).read()
                except:
                        user_data = ''
        return user_data

def create_client():
        client = EC2Connection(config.get('IAM', 'access'), config.get('IAM', 'secret'))
        regions = client.get_all_regions()
        for r in regions:
                if r.name == config.get('EC2', 'region'):
                        client = EC2Connection(config.get('IAM', 'access'), config.get('IAM', 'secret'), region = r)
                        return client
        return None

def get_existing_instance(client):
        instances = client.get_all_instances(filters = { 'tag:Name': config.get('EC2', 'tag') })
        if len(instances) > 0:
                return instances[0].instances[0]
        else:
                return None
def list_all_existing_instances(client):
        reservations = client.get_all_instances(filters = { 'tag:Name': config.get('EC2', 'tag') })
        if len(reservations) > 0:
                r_instances = [inst for resv in reservations for inst in resv.instances]
                for inst in r_instances:
                        print "Instance Id: %s (%s)" % (inst.id, inst.state)


def get_spot_price(client, av_zone, type_inst):
        price_history = client.get_spot_price_history(instance_type = type_inst, product_description = 'Linux/UNIX', availability_zone = av_zone)
        return price_history

def provision_instance(client, user_data, bid_price, option, type_inst):

        req = client.request_spot_instances(price = bid_price,
                image_id = config.get('EC2', 'ami'), 
                instance_type = type_inst, 
                key_name = config.get('EC2', 'key_pair'), 
                user_data = user_data,
                placement = config.get('EC2','availability_zone') + option,
                type = 'one-time',
                security_groups = [config.get('EC2', 'security_group')])
        print 'Spot request created'
        print 'Waiting for spot provisioning',

        while True:
                current_req = client.get_all_spot_instance_requests()[-1]
                if current_req.state == 'active':
                        print 'done.'
                        instance = client.get_all_instances([current_req.instance_id])[0].instances[0]
                        instance.add_tag('Name', config.get('EC2', 'tag'))
                        return instance
                print '.',
                sleep(10)

def destroy_instance(client, inst):
        try:
                print 'Terminating', str(inst.id), '...',
                client.terminate_instances(instance_ids = [inst.id])
                print 'done.'
                inst.remove_tag('Name', config.get('EC2', 'tag'))
        except:
                print 'Failed to terminate:', sys.exc_info()[0]

def wait_for_up(client, inst):
        print 'Waiting for instance to come up'
        while True:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if inst.ip_address is None:
                        inst = get_existing_instance(client)
                try:
                        if inst.ip_address is None:
                                print 'IP not assigned yet ...',
                        else:
                                s.connect((inst.ip_address, 22))
                                s.shutdown(2)
                                print 'Server is up!'
                                print 'Server Public IP - %s' % inst.ip_address
                                break
                except:
                        print '.',
                sleep(10)

def main():
        # Entry

		#get_price_history(config.get('EC2', 'region'), 4, config.get('EC2', 'type')) #Get spot pricing history to work on
		#sys.exit(0)
		action = 'start' if len(sys.argv) == 1 else sys.argv[1]
		client = create_client()
		if client is None:
				print 'Unable to create EC2 client'	
				sys.exit(0)
		inst = get_existing_instance(client)
		user_data = read_user_data_from_local_config()
		type_inst = sys.argv[3]
		parse_regions(sys.argv[2], type_inst, config.get('EC2', 'region')) # Parse data according to regions, coz each zone is a separate market

		pad_region("ap-southeast-1a") #Pad the data to convert it into an hourly time series data
		pad_region("ap-southeast-1b")
		#pad_region("us-west-2c")

		simplify_data("ap-southeast-1a-pad") #Simplify to calculate statistics
		simplify_data("ap-southeast-1b-pad")
		#simplify_data("us-west-2c-pad")

        #Gather stats about failures us-west-2a
        #BidPrices_2a = []
        #Avg_Uptime_2a = []
        #Avg_Downtime_2a = []
        #Availability_2a = []
        #BidPrices_2a,Avg_Uptime_2a,Avg_Downtime_2a,Availability_2a = stats("ap-southeast-1a-pad-final", 0.289)

        #Gather stats about failures us-west-2b
        #BidPrices_2b = []
        #Avg_Uptime_2b = []
        #Avg_Downtime_2b = []
        #Availability_2b = []
        #BidPrices_2b,Avg_Uptime_2b,Avg_Downtime_2b,Availability_2b = stats("ap-southeast-1b-pad-final", 0.289)

        #Gather stats about failures us-west-2c
        #BidPrices_2c = []
        #Avg_Uptime_2c = []
        #Avg_Downtime_2c = []
        #Availability_2c = []
        #BidPrices_2c,Avg_Uptime_2c,Avg_Downtime_2c,Availability_2c = stats("us-west-2c-pad-final", 0.239)

        #price_2a = get_spot_price(client, 'us-west-2a')
        #price_2b = get_spot_price(client, 'us-west-2b')
        #price_2c = get_spot_price(client, 'us-west-2c')

		#uptime = 12
		uptime = float(config.get('EC2', 'uptime'))

        #mean_2a, std_2a = get_recent_history(price_2a)
        #mean_2b, std_2b = get_recent_history(price_2b)
        #mean_2c, std_2c = get_recent_history(price_2c)
        
        #print mean_2a, std_2a
        #print mean_2b, std_2b
        #print mean_2c, std_2c

		Values_2a = []
		Values_2a, mean_2a, std_2a = auto_corr("ap-southeast-1a-pad-final", 60)
		#for i in xrange(len(Values_2a)):
		#	print str(i) + ',' + str(Values_2a[i])

		Values_2b = []
		Values_2b, mean_2b, std_2b = auto_corr("ap-southeast-1b-pad-final", 60)
		#for i in xrange(len(Values_2b)):
		#	print str(i) + ',' + str(Values_2b[i])

		#Values_2c = []
		#Values_2c = auto_corr("us-west-2c-pad-final", 60)
		#for i in xrange(len(Values_2c)):
		#print str(i) + ',' + str(Values_2c[i])
		
		l = len(Values_2a)
		i = 0
		threshold = 0.3
		
		option = get_stl("ap-southeast-1a","ap-southeast-1b")		
		if option == 'a':
			Values = Values_2a
		else:
			Values = Values_2b
		#option = 'b'
		#if std_2a < std_2b:
		#	Values = Values_2a
		#	option = 'a'
		#else:
		#	Values = Values_2b
		while Values[i] >= threshold:
			#print Values_2a[i], threshold
			i += 1
		#print "hours = ",i
		now = datetime.datetime.utcfromtimestamp(time.time())

		st_time = now - datetime.timedelta(i)
		#Price = client.get_spot_price_history(instance_type = type_inst, product_description = 'Linux/UNIX',
        #										availability_zone = config.get('EC2', 'region') + option,
        #										start_time = st_time.isoformat(), end_time = now.isoformat())
		

		#Price.reverse()
		#print "length = ",len(Price)
		Price = []
		BidPrices_1a = []
		Avg_Uptime_1a = []
		Avg_Downtime_1a = []
		Availability_1a = []
		Price, BidPrices_1a,Avg_Uptime_1a,Avg_Downtime_1a,Availability_1a = stats(0.355, option, 2)

		#for m in xrange(len(BidPrices_1a)):
		#	print str(BidPrices_1a[m]/0.355) + ',' + str(Avg_Uptime_1a[m]) + ',' + str(Avg_Downtime_1a[m]) + ',' + str(Availability_1a[m])

		i = 0

		l = len(BidPrices_1a)
		while i < l:
			if Price[0] <= BidPrices_1a[i]:
				break
			i += 1
		
		if i == l:
			print "Place a persistent bid with the reqd uptime. Market price is too high"
			i = 0
		#print uptime, Avg_Uptime_1a[i]
		while i < l:
			if Availability_1a[i] >= 0.992:
				break
			i += 1
		bid_price = BidPrices_1a[i]
		print "Estimated bid:",bid_price,"Zone:",option
		sys.exit(0)
		if action == 'start':
        
			#if inst is None or inst.state == 'terminated':
			spot_price = get_spot_price(client, config.get('EC2', 'availability_zone') + option, type_inst)
			print 'Spot price is ' + str(spot_price[0].price) + ' ...',
			if spot_price[0].price >= bid_price:
				print 'too high!'
				print spot_price, bid_price
				sys.exit(0)
			else:
				print 'below maximum bid, continuing'
				provision_instance(client, user_data, bid_price, option, type_inst)
				inst = get_existing_instance(client)
			print inst
			wait_for_up(client, inst)
		elif action == 'stop' and inst is not None:
			destroy_instance(client, inst)
		elif action == 'list':
			print 'Active Spot Instnaces (AMI: %s)' % config.get('EC2', 'ami')
		else:
			spot_price = get_spot_price(client)

if __name__ == "__main__":
        config = ConfigParser.ConfigParser()
        config.read('details.cfg')
        main()
