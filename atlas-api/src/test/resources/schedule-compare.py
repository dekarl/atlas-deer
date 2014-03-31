#!/usr/bin/python

import argparse
import dateutil.parser
import datetime
import httplib
import sys
import json

class Struct:
    def __init__(self, **entries): 
        self.__dict__.update(entries)

class Atlas:

	def __init__(self, host, port, version, key):
		self.host = host
		self.port = port
		self.version = version
		self.key = key

	def get_schedule(self, source, channel, start, end):
		resource = self.get_resource(source, channel, start, end)
		response = self.get(resource)
		return self.simplify(response)

	def get(self, resource):
		conn = httplib.HTTPConnection(self.host, self.port)
		conn.request('GET', resource)
		print "GET http://%s:%s%s" % (self.host, self.port, resource)
		resp = conn.getresponse()
		if not resp.status == 200:
			if resp.status == 400:
				print "request failed for %s: %s" % (resource, resp.reason)
			if resp.status == 404:
				print "resource %s doesn't appear to exist" % (resource)
			if resp.status >= 500:
				print "problem with %s? %s %s" % (self.host, resp.status, resp.reason)
			resp.read()
			conn.close()
			sys.exit()
		return Struct(**json.loads(resp.read()))

	def simplify(self,response):
		return response

	def v4_channel_id(self, channel):
		prefix = 'http://atlas.metabroadcast.com/4.0/channels/'
		for a in channel.aliases:
			if a.startswith(prefix):
				return a[len(prefix):]
		raise Exception("couldn't find v4 channel id for %s" % channel.id)

	def join(self, params):
		return "&".join(["%s=%s"%(k,v) for k,v in params.iteritems() if not v == None])

	def get_resource(self, source, channel, start, end):
		if (self.version == 3):
			params = {
				'publisher':source, 
				'channel_id' : channel.id, 
				'from':start.isoformat(),
				'to':end.isoformat(),
				'apiKey':self.key
			}
			return "/3.0/schedule.json?%s" % self.join(params)
		if (self.version == 4):
			params = {'source':source,'from':start.isoformat(),'to':end.isoformat(),'apiKey':self.key}
			channel_id = self.v4_channel_id(channel)
			return "/4.0/schedules/%s.json?%s" %(channel_id, self.join(params))
		raise Exception("unexpected version %s" % self.version)


def compare(left, right):
	print "comparing\n%s\n%s" % (left, right)

arg_parser = argparse.ArgumentParser(description='Compare Atlas schedules.')
arg_parser.add_argument('-h1', dest='host1', default='atlas.metabroadcast.com', metavar='host1', help='Remote host for 1st schedule')
arg_parser.add_argument('-h2', dest='host2', default='atlas.metabroadcast.com', metavar='host2', help='Remote host for 2nd schedule')
arg_parser.add_argument('-p1', dest='port1', default=80, type=int, metavar='port1', help='Port for 1st schedule')
arg_parser.add_argument('-p2', dest='port2', default=80, type=int, metavar='port2', help='Port for 2nd schedule')
arg_parser.add_argument('-v1', dest='version1', default=3, type=int, choices=xrange(3, 5), help='API version for 1st schedule')
arg_parser.add_argument('-v2', dest='version2', default=4, type=int, choices=xrange(3, 5), help='API version for 2nd schedule')
arg_parser.add_argument('-k', dest='key', help='API key to use')

arg_parser.add_argument('source', metavar='source', help='Source of the schedules to compare')
arg_parser.add_argument('channel', metavar='channel', help='v3 ID of channel for which schedules should be compared')
arg_parser.add_argument('start', metavar='start', help='Start time of schedules to compare')
arg_parser.add_argument('end', metavar='end', nargs='?', help='End time of schedules to compare')

args = arg_parser.parse_args();

args.start = dateutil.parser.parse(args.start)
args.end = dateutil.parser.parse(args.end) if not args.end==None else (args.start + datetime.timedelta(days=1))

print "identifying channel %s" % args.channel

##
# Currently there's only a v3 /channels resource so we may need to find the v4 id and we may as well 
# check that the channel actually exists while were at it. 
# There's no guarantee that the hosts specified in the args have a 3.0/channels resource so this is
# hard-coded.
##
channelHost = 'atlas.metabroadcast.com'
channelResource = '/3.0/channels/%s.json' % args.channel
channelConn = httplib.HTTPConnection(channelHost)
channelConn.request('GET', channelResource)
print "GET http://%s%s" % (channelHost, channelResource)
channelResp = channelConn.getresponse()
if not channelResp.status == 200:
	if channelResp.status == 400:
		print "request failed for %s: %s" % (args.channel, channelResp.reason)
	if channelResp.status == 404:
		print "channel %s doesn't appear to exist" % (args.channel)
	if channelResp.status >= 500:
		print "problem with %s? %s %s" % (channelHost, channelResp.status, channelResp.reason)
	channelResp.read()
	channelConn.close()
	sys.exit()

channel = Struct(**json.loads(channelResp.read())['channels'][0])

print "comparing schedules on '%s' between %s and %s" % (channel.title, args.start, args.end)

atlas1 = Atlas(args.host1, args.port1, args.version1, args.key)
atlas2 = Atlas(args.host2, args.port2, args.version2, args.key)

schedule1 = atlas1.get_schedule(args.source, channel, args.start, args.end)
schedule2 = atlas2.get_schedule(args.source, channel, args.start, args.end)

compare(schedule1, schedule2)
