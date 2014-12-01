from parser import parse_sync_output
import time
from pymongo import MongoClient
import datetime
import uuid
import quasar
from twisted.internet import defer, reactor
import sys
from configobj import ConfigObj

_client = MongoClient()
upmudb = _client.upmu_database
syncdb = _client.sync_databaseq

streaminfo = ConfigObj('manager/upmuconfig.ini')

def parse(string):
    """ Parses data (in the form of STRING) into a series of sync_output
    objects. Returns a list of sync_output objects. If STRING is not of a
    suitable length (i.e., if the number of bytes is not some multiple of
    the length of a sync_output struct) a ParseException is raised. """
    lst = []
    while string:
        obj, string = parse_sync_output(string)
        lst.append(obj)
    return lst

def termerror(args):
	print "error: ", args

def doinsert(q, uid, data, desc, id):
    df = q.insertValues(str(uid), data)
    def ondone((stat, args)):
        if stat == 'ok':
            pass
            #print "insert ok",desc
            #syncdb.finserts.insert({"fid":id,"ok":True})
        else:
            print "insert error",desc
            #syncdb.finserts.insert({"fid":id,"ok":False})
    df.addCallback(ondone)
    df.addErrback(termerror)
    return df

@defer.inlineCallbacks
def process_loop(q):
    while True:
       print "looping"
       yield process(q)
       print "sleeping"
       time.sleep(60)

@defer.inlineCallbacks
def process(q):
    ytagbase = 50
    sernum = sys.argv[1]
    allfiles = upmudb.received_files.find({"serial_number":sernum, "xtag":{"$exists":False}, 
      "$or":[{"ytag":{"$lt":ytagbase}},{"ytag":{"$exists":False}}]},timeout=False).sort("name")
    print "There are %d files matching ser=%s" % (allfiles.count(), sernum)
    #print "There are %d files matching ser=%s" % (allfiles.count(), serialnumber)
    epoch = datetime.datetime.utcfromtimestamp(0)
    file_idx = 0
    for fl in allfiles:
        try:
            print ".",
            sys.stdout.flush()
            for synco in parse(fl["data"]):
                #Time
                try:
                    if (not (2012 < synco.sync_data.times[0] < 2016)):
                        print "rejecting bad date record"
                        continue
                    t = datetime.datetime(*synco.sync_data.times)
                    ts = int((t - epoch).total_seconds()*1000000)*1000
                    dfields = [synco.sync_data.L1MagAng, synco.sync_data.L2MagAng, synco.sync_data.L3MagAng,
                               synco.sync_data.C1MagAng, synco.sync_data.C2MagAng, synco.sync_data.C3MagAng]
                    maps = ["L1MAG","L1ANG","L2MAG","L2ANG","L3MAG","L3ANG",
                            "C1MAG","C1ANG","C2MAG","C2ANG","C3MAG","C3ANG"]
                    ifields = [[[], []] for _ in dfields]
                    lsfields = []
                    dfidx = 0
                    for df in dfields:
                        idx = 0
                        for sp in df:
                            t_sp = ts + 8333333*idx
                            ifields[dfidx][0].append([t_sp, sp.mag])
                            ifields[dfidx][1].append([t_sp, sp.angle])
                            idx += 1
                        dfidx += 1
                    idx = 0
                    for sp in synco.sync_data.lockstate:
                        t_sp = ts + 8333333*idx
                        lsfields.append([t_sp, sp])

                    for dfidx in xrange(len(dfields)):
                        magu = uuid.UUID(streaminfo[sernum][maps[dfidx*2]]['uuid'])
                        angu = uuid.UUID(streaminfo[sernum][maps[dfidx*2+1]]['uuid'])
                        yield doinsert(q, magu, ifields[dfidx][0], fl["name"]+"::"+maps[dfidx*2], fl["_id"])
                        yield doinsert(q, angu, ifields[dfidx][1], fl["name"]+"::"+maps[dfidx*2+1], fl["_id"])
                    yield doinsert(q, uuid.UUID(streaminfo[sernum]["LSTATE"]['uuid']), lsfields,fl["name"]+"::LSTATE",fl["_id"])
                except Exception as e:
                    print "FAILi (error)",fl["_id"]
                    print e
        except Exception as e:
            print "FAILo (error)",fl["_id"]
            print e
        print "inserted ", fl["name"]
        upmudb.received_files.update({"_id":fl["_id"]}, {"$set":{"ytag":ytagbase}})
    print "Inserted all files"


d = quasar.connectToArchiver("asylum.cs.berkeley.edu")
d.addCallback(process_loop)
d.addErrback(termerror)
reactor.run()
