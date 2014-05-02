# Schedule Scripts

simple scripts relating to schedules

## schedule-bootstrap.py

Makes HTTP requests to bootstrap schedules for a range of days for a set of
channels or all the channels on a platform.

e.g.
```
./schedule-bootstrap.py -source pressassociation.com -start 2014-05-05 hkqs
./schedule-bootstrap.py -source pressassociation.com -start 2014-05-05 -end 2014-05-06 hkqs
./schedule-bootstrap.py -source pressassociation.com -start 2014-05-05 -end 2014-05-06 -platform cbhN
```

The target host can be overridden with the `-host` parameter.

## schedule-compare.py

Fetches schedules from two sources and produces a table comparing them. Output
versions 3 and 4 are supported.

__N.B__ only v3 channel IDs are supported because only a v3 `/channels` resource exists.

Parameters are described with `./schedule-compare.py -h`

e.g.
```
./schedule-compare.py -h2 stage.atlas.metabroadcast.com -v2 4 -k <key> pressassociation.com cbbh 2014-05-05
```
