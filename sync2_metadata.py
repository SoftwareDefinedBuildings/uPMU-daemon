__author__ = 'immesys'

from pymongo import MongoClient
import copy
from sync2_uuids import umap

_client = MongoClient()
mdb = _client.sync_database

upmus = [
    ("P3001035","soda_b"),
    ("P3001071","soda_a"),
    ("P3001097","grizzly_old"),
    ("P3001082","grizzly_new"),
    ("P3001293","switch_a6_new"),
    ("P3001319","emma_old"),
    ("P3001065","emma_new")
]

mr = {
    "Path" : "t",
    "Metadata" :
    {
        "SourceName" : "uPMU",
        "Instrument" :
        {
            "ModelName" : "PQube3"
        }
    },
    "uuid" : "",
    "Properties" :
    {
        "UnitofTime" : "ns",
        "Timezone" : "UTC",
        "UnitofMeasure" : "deg",
        "ReadingType" : "double"
    }
}

for sernum, name in upmus:
    for label, unit in [("L1MAG", "V"), ("L2MAG","V") , ("L3MAG","V"),
                        ("L1ANG","deg"),("L2ANG","deg"),("L3ANG","deg"),
                        ("C1MAG","A"),("C2MAG","A"), ("C3MAG","A"),
                        ("C1ANG","deg"),("C2ANG","deg"),("C3ANG","deg"),
                        ("LSTATE","bitmap")]:
        m = copy.deepycopy(mr)
        m["Path"] = "/upmu/%s/%s" % (name, label)
        m["uuid"] = umap["sernum"][label]
        m["Properties"]["UnitofMeasure"] = unit
        bdb.metadata.insert(m)