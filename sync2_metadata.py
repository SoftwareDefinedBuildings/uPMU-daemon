__author__ = 'immesys'

from pymongo import MongoClient
import copy
from sync2_uuids import umap

_client = MongoClient()
mdb = _client.qdf

upmus = [
#    ("P3001035","soda_b"),
#    ("P3001071","soda_a"),
#    ("P3001097","grizzly_old"),
#    ("P3001082","switch_a6_old"),
#    ("P3001059","grizzly_old"),
    ("P3001293","switch_a6"),
#    ("P3001319","emma_old"),
#    ("P3001065","emma_new"),
    ("P3001244","soda_a"),
    ("P3001039","soda_b"),
#    ("P3001192","culler_a"),
#    ("P3001297","culler_b"),
    ("P3001190","building_71"),
    ("P3001352","grizzly_new"),
]

mr = {
    "Path" : "t",
    "Metadata" :
    {
        "SourceName" : "uPMU",
        "Instrument" :
        {
            "ModelName" : "PQube3",
            "SerialNumber": "serial"
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
        m = copy.deepcopy(mr)
        m["Path"] = "/upmu/%s/%s" % (name, label)
        m["uuid"] = str(umap[sernum][label])
        m["Properties"]["UnitofMeasure"] = unit
        m["Metadata"]["Instrument"]["SerialNumber"] = sernum
        mdb.metadata.insert(m)
