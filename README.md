Spot bid broker
==================
This is a python script that spawns EC2 spot instances for minimum bid and maximum uptime(specific instance type, region and bid).

Go through the following steps to place the spot bids:

1. Using pip, install boto, mock, nose and wsgiref

2. Add your aws keys, region name, ami, key pair name, security group name to the config file

3. First, retrieve the 2-month spot history by executing the following command:
$ python spot_history_data.py <instance_type> <region name> > results.txt

4. Run main.py to place the spot bid with max uptime:
$ python main.py start <file name with history data> <instance type>

--------------------------------------------------------------------------------------------------------
Details about functions:

parse_regions():
The spot history data contains data pertaining to all availability zones. This functions splits data based on availability zones.

pad_region():
Used to pad data to make an hourly data(For easier analysis)

simplify_data():
Display data as a tuple - <hour,price>

stats():
To compute Avg uptime, Avg. Downtime, Availability of a persistent request for bids varying from 0 to on-demand cost.

get_recent_history():
Get mean and standard deviation of the data.

auto_corr():
Compute the autocorrelation of the spot pricing history with a lag factor varying from 0hrs to the input provided(mostly 50-60hrs)

get_stl():
Makes decision on which availability zone to choose by comparing standard deviations of residual graphs(by removing seasonality and trend)

Credits
-------------
Some part of the code comes from [spot-ec2-proxifier](https://github.com/alexzorin/spot-ec2-proxifier) project
read_user_data_from_local_config():
Read keys from the local config file

get_spot_price_history():
Get spot history data from AWS.

create_client():
Create an EC2 client session using boto

provision_instance():
Place a spot request with given parameters.

wait_for_up():
Used to connect to the spot instance and get its public IP
