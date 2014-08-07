from parser import parse_sync_output
from pymongo import MongoClient
import datetime
import uuid
import requests
import json

_client = MongoClient()
db = _client.upmu_database

persistent_uuids = [
uuid.UUID('3af77b3f-3c04-4c34-9b1d-20481e1fb89f'),
uuid.UUID('189d722c-44d4-4db7-9799-88f063aa2242'),
uuid.UUID('ff57040c-6b04-49ea-acd5-dd16d4ee1b73'),
uuid.UUID('6ffbcb5b-d833-4b50-bd58-c26d9637bbee'),
uuid.UUID('eae664e8-5059-4be8-b08b-bbd5b8890f09'),
uuid.UUID('92d95eef-a2f4-4880-8250-69e23a4dce4c'),
uuid.UUID('67d0588a-e3f4-44c7-95cb-ef9a98da0700'),
uuid.UUID('4320a8cf-b444-47b9-8e29-ac117c97ebe7'),
uuid.UUID('ce48da92-52a0-4552-b44f-40d61e1aa160'),
uuid.UUID('c055e297-e900-4da9-b1cf-3d3658a2ca15')
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

def doinsert(uid, data):
    doc = {"uuid":str(uid), "Readings":data}
    url = "http://localhost:9000/data/add/fookey"
    r = requests.post(url, data=json.dumps(doc))
    print r  

def process(serialnumber):
    allfiles = db.received_files.find({"serial_number":serialnumber})
    #print "There are %d files matching ser=%s" % (allfiles.count(), serialnumber)
    epoch = datetime.datetime.utcfromtimestamp(0)
    for fl in allfiles:
        num_gps = []
        for synco in parse(fl["data"]):
            #Time
            t = datetime.datetime(*synco.sync_data.times)
            ts = int((t - epoch).total_seconds()*1000000)*1000
            dfields = [synco.sync_data.L1MagAng]
            ifields = [[[], []] for _ in dfields]
            dfidx = 0
            for df in dfields:
                idx = 0
                for sp in df:
                    t_sp = ts + 8333333*idx
                    ifields[dfidx][0].append([t_sp, sp.mag])
                    ifields[dfidx][1].append([t_sp, sp.angle])
                    idx += 1
                doinsert(persistent_uuids[dfidx*2],ifields[dfidx][0])        
                doinsert(persistent_uuids[dfidx*2+1],ifields[dfidx][1])        
                dfidx += 1
            #GPS num
            #num_gps = synco.gps_stats.satellites
            
        break

if __name__ == "__main__":
    process("P3001071")
