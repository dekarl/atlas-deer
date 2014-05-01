#!/usr/bin/python

import argparse
import dateutil.parser
import datetime
import httplib
import sys
import json
from tabulate import tabulate

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

def color(c, val):
  if sys.stdin.isatty():
    return "\x1b[%sm%s\x1b[0m" % (c, val)
  return val

class Struct:
  def __init__(self, **entries):
    self.__dict__.update(entries)

class SimpleEntry:
  def __init__(self, id, title, bid, start, end):
    self.id = id
    self.title = title
    self.bid = bid
    self.start = start
    self.end = end

  def __str__(self):
    return "%s %s %s %s %s" % (self.id,self.title,self.bid,self.start,self.end)

  def __repr__(self):
    return self.__str__()

  def as_list(self):
    fmt = "%H:%M:%S"
    start = self.start.strftime(fmt)
    end = self.end.strftime(fmt)
    return [start, end, self.bid, self.id, self.title[:15]]

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
    col = "32" if self.version == 3 else "33"
    print color(col, "GET http://%s:%s%s" % (self.host, self.port, resource))
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
      return "/4.0/schedules/%s.json?annotations=content.description&%s" %(channel_id, self.join(params))
    raise Exception("unexpected version %s" % self.version)

  def simplify(self, schedule):
    if self.version == 4:
      return self.simplify_v4(schedule)
    return self.simplify_v3(schedule)

  def simplify_v4(self, schedule):
    entries = []
    sched_entries = schedule.schedule['entries'];
    for sched_entry in sched_entries:
      i = sched_entry['item']
      b = sched_entry['broadcast']
      start = dateutil.parser.parse(b['transmission_time'])
      end = dateutil.parser.parse(b['transmission_end_time'])
      entries.append(SimpleEntry(i['id'],i['title'],b['id'],start, end))
    return entries

  def simplify_v3(self, schedule):
    entries = []
    sched_entries = schedule.schedule[0]['items']
    for se in sched_entries:
      b = se['broadcasts'][0]
      start = dateutil.parser.parse(b['transmission_time'])
      end = dateutil.parser.parse(b['transmission_end_time'])
      entries.append(SimpleEntry(se['id'],se['title'],b['id'],start,end))
    return entries

headers = ['Title', 'ID', 'BID', 'End', 'Start']

def compare(left, right):
  table = [headers + ["|"] + headers[::-1]]
  longer = max(len(left),len(right))
  l = None
  r = None
  while len(left) > 0 and len(right) > 0:
    l = l if l != None else left.pop(0)
    r = r if r != None else right.pop(0)
    if (l.start > r.start):
      table.append(mismatch(None, r))
      r = None
    if (l.start < r.start):
      table.append(mismatch(l, None))
      l = None
    else:
      table.append(matching_start(l, r))
      l = None
      r = None
  if len(left) > 0:
    for l in left:
      table.append(mismatch(l, None))
  else :
    for r in right:
      table.append(mismatch(None, r))
  print tabulate(table, headers="firstrow")

highlight = lambda li: [color("31", v) if i in range(2,4) else v for (i,v) in enumerate(li)]

def matching_start(l, r):
  left = l.as_list()
  right = r.as_list()
  if (l.id != r.id or l.bid != r.bid):
    left = highlight(left)
    right = highlight(right)
  return left[::-1] +["|"]+ right

red = lambda x : color("41",' ' * x)
missing_row = [red(8),red(8), red(11), red(6), red(15)]

def mismatch(l, r):
  vals_or_missing = lambda e: missing_row if e == None else e.as_list()
  return vals_or_missing(l)[::-1] +["|"]+ vals_or_missing(r)

print "identifying channel '%s'" % args.channel

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
print color("35","GET http://%s%s" % (channelHost, channelResource))
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
