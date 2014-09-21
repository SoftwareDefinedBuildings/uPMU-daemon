__author__ = 'immesys'
import uuid

import sys

template="""
    "{}": {{ #{}
        "L1MAG" : uuid.UUID('{}'),
        "L1ANG" : uuid.UUID('{}'),
        "L2MAG" : uuid.UUID('{}'),
        "L2ANG" : uuid.UUID('{}'),
        "L3MAG" : uuid.UUID('{}'),
        "L3ANG" : uuid.UUID('{}'),
        "C1MAG" : uuid.UUID('{}'),
        "C1ANG" : uuid.UUID('{}'),
        "C2MAG" : uuid.UUID('{}'),
        "C2ANG" : uuid.UUID('{}'),
        "C3MAG" : uuid.UUID('{}'),
        "C3ANG" : uuid.UUID('{}'),
        "LSTATE": uuid.UUID('{}'),
        }},
"""

uuids = [str(uuid.uuid4()) for i in xrange(13)]
args = [sys.argv[1], sys.argv[2]] + uuids

print template.format(*args)