from parser import parse_sync_output
from pymongo import MongoClient
import datetime
import uuid
import requests
import json
import cProfile

_client = MongoClient()
db = _client.upmu_database

persistent_uuids = [
uuid.UUID('3af77b3f-3c04-4c34-9b1d-20481e1fb89f'), #L1MAG
uuid.UUID('189d722c-44d4-4db7-9799-88f063aa2242'), #L1ANG
uuid.UUID('ff57040c-6b04-49ea-acd5-dd16d4ee1b73'), #L2MAG
uuid.UUID('6ffbcb5b-d833-4b50-bd58-c26d9637bbee'), #L2ANG
uuid.UUID('eae664e8-5059-4be8-b08b-bbd5b8890f09'), #L3MAG
uuid.UUID('92d95eef-a2f4-4880-8250-69e23a4dce4c'), #L3ANG
uuid.UUID('67d0588a-e3f4-44c7-95cb-ef9a98da0700'), #C1MAG
uuid.UUID('4320a8cf-b444-47b9-8e29-ac117c97ebe7'), #C1ANG
uuid.UUID('ce48da92-52a0-4552-b44f-40d61e1aa160'), #C2MAG
uuid.UUID('c055e297-e900-4da9-b1cf-3d3658a2ca15'), #C2ANG
uuid.UUID('1cc2b394-a1bf-42b6-a3c5-6862c9fa7a44'), #C3MAG
uuid.UUID('d5642fe6-a052-4ef6-9374-302c1110425a'), #C3ANG
uuid.UUID('28919507-4f75-4f9f-8e74-1763c30c229b'), 
uuid.UUID('ce52413f-04be-4bf1-b147-1ad537a50d02'),
uuid.UUID('42cd595f-d844-43c8-851b-fcb4aaa40183'),
uuid.UUID('c62e6d57-aca1-4377-b1a8-480d479b5592'),
uuid.UUID('1225cfe4-89b1-4b9a-b2ec-12500c458c06'),
uuid.UUID('cb9fefed-11fc-4ca7-822e-8dc2500b6884'),
uuid.UUID('016d9e0d-1995-4610-b3b1-2eb031dc648a'),
uuid.UUID('137b982b-1a4b-483b-b4c5-d7bb3202f0ac'),
uuid.UUID('dec4108f-b4a2-4910-9b9b-a1c185709ca4'),
uuid.UUID('12619f48-24e6-4f8d-9b68-2fadfc1d90e4')
]
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

ses = requests.Session()

def doinsert(uid, data):
    doc = {"uuid":str(uid), "Readings":data}
    #url = "http://asylum.cs.berkeley.edu:9000/data/add/fookey"
    url = "http://localhost:9000/data/add/fookey"
    r = ses.post(url, data=json.dumps(doc))
    _ = r.content

def process(serialnumber):
    allfiles = db.received_files.find({"serial_number":serialnumber}).sort("name")
    #print "There are %d files matching ser=%s" % (allfiles.count(), serialnumber)
    epoch = datetime.datetime.utcfromtimestamp(0)
    file_idx = 0
    for fl in allfiles:
        file_idx += 1
        num_gps = []
        for synco in parse(fl["data"]):
            #Time
            t = datetime.datetime(*synco.sync_data.times)
            ts = int((t - epoch).total_seconds()*1000000)*1000
            dfields = [synco.sync_data.L1MagAng, synco.sync_data.L2MagAng, synco.sync_data.L3MagAng,
                       synco.sync_data.C1MagAng, synco.sync_data.C2MagAng, synco.sync_data.C3MagAng]
            ifields = [[[], []] for _ in dfields]
            dfidx = 0
            for df in dfields:
                idx = 0
                for sp in df:
                    t_sp = ts + 8333333*idx
                    ifields[dfidx][0].append([t_sp, sp.mag])
                    ifields[dfidx][1].append([t_sp, sp.angle])
                    idx += 1
                dfidx += 1
            for dfidx in xrange(len(dfields)):
                doinsert(persistent_uuids[dfidx*2],ifields[dfidx][0])        
                doinsert(persistent_uuids[dfidx*2+1],ifields[dfidx][1])        
            #GPS num
            #num_gps = synco.gps_stats.satellites
#        if file_idx == 200:
#            break
        print "Inserted file %d" % file_idx

if __name__ == "__main__":
    #cProfile.run('process("P3001071")')
    process("P3001071")
