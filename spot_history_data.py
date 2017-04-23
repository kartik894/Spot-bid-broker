import json
import sys
import datetime
import calendar
import time
from subprocess import Popen, PIPE

#target = open("history.json","w")
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

region = sys.argv[2]
now = datetime.datetime.utcfromtimestamp(time.time())
start_time = now - datetime.timedelta(hours=24*60)
inst_type = sys.argv[1]
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
	print cur_az, cur_time, cur_price
