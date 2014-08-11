from parser import parse_sync_output
from pymongo import MongoClient
import datetime
import uuid
import quasar
from sync2_uuids import umap
from twisted.internet import defer, reactor
import sys

_client = MongoClient()
upmudb = _client.upmu_database
syncdb = _client.sync_database

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
            #print "insert ok",desc
            syncdb.finserts.insert({"fid":id,"ok":True})
        else:
            print "insert error",desc
            syncdb.finserts.insert({"fid":id,"ok":False})
    df.addCallback(ondone)
    df.addErrback(termerror)
    return df

@defer.inlineCallbacks
def process(q):
    sernum = sys.argv[1]
    allfiles = upmudb.received_files.find({"serial_number":sernum},timeout=False).sort("name")
    #print "There are %d files matching ser=%s" % (allfiles.count(), serialnumber)
    epoch = datetime.datetime.utcfromtimestamp(0)
    file_idx = 0
    for fl in allfiles:
        if syncdb.finserts.find({"fid":fl["_id"],"ok":True}).count() != 0:
            print "skipping (flok)",fl["name"]
            continue
        try:
            for synco in parse(fl["data"]):
                #Time
                try:
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
                        magu = umap[sernum][maps[dfidx*2]]
                        angu = umap[sernum][maps[dfidx*2+1]]
                        yield doinsert(q, magu, ifields[dfidx][0], fl["name"]+"::"+maps[dfidx*2], fl["_id"])
                        yield doinsert(q, angu, ifields[dfidx][1], fl["name"]+"::"+maps[dfidx*2+1], fl["_id"])
                    yield doinsert(q, umap[sernum]["LSTATE"], lsfields,fl["name"]+"::LSTATE",fl["_id"])
                except Exception as e:
                    print "FAILi (error)",fl["_id"]
                    print e
        except Exception as e:
            print "FAILo (error)",fl["_id"]
            print e
        print "inserted ", fl["name"]


d = quasar.connectToArchiver("asylum.cs.berkeley.edu")
d.addCallback(process)
d.addErrback(termerror)
reactor.run()
