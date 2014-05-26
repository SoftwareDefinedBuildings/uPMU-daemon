import struct

class sync_point(object):
    LENGTH = 8
    def __init__(self, *data):
        assert len(data) == 2, 'wrong number of fields (expected 2, got {0})'.format(len(data))
        assert [float, float] == map(type, data), 'wrong types of arguments'
        self.angle, self.mag = data
    
class sync_output_msgq(object):
    LENGTH = 508 + (720 * sync_point.LENGTH)
    def __init__(self, *data):
        assert len(data) == 9, 'wrong number of fields (expected 9, got {0})'.format(len(data))
        assert float == type(data[0]), 'wrong types of arguments'
        assert [tuple for _ in xrange(8)] == map(type, data[1:]), 'wrong types of arguments'
        assert len(data[1]) == 6, 'argument 1 (times) has wrong length (expected 6, got {0})'.format(len(data[1]))
        assert [120 for _ in xrange(7)] == [len(item) for item in data[2:]], 'argument list has wrong length'
        self.sampleRate, self.times, self.lockstate, self.L1MagAng, self.L2MagAng,\
        self.L3MagAng, self.C1MagAng, self.C2MagAng, self.C3MagAng = data
    
class sync_pll_stats_msgq(object):
    LENGTH = 16
    def __init__(self, *data):
        assert len(data) == 4, 'wrong number of fields (expected 4, got {0})'.format(len(data))
        assert [int, int, int, int] == map(type, data), 'wrong types of arguments'
        self.ppl_state, self.pps_prd, self.curr_err, self.center_frq_offset = data
    
class sync_gps_stats(object):
    LENGTH = 28
    def __init__(self, *data):
        assert len(data) == 7, 'wrong number of fields (expected 7, got {0})'.format(len(data))
        assert [float for _ in xrange(7)] == map(type, data), 'wrong types of arguments'
        self.alt, self.lat, self.hdop, self.lon, self.satellites, self.state, self.hasFix = data
        
class sync_output(object):
    LENGTH = sync_output_msgq.LENGTH + sync_pll_stats_msgq.LENGTH + sync_gps_stats.LENGTH
    def __init__(self, *data):
        assert len(data) == 3, 'wrong number of fields (expected 3, got {0})'.format(len(data))
        assert [sync_output_msgq, sync_pll_stats_msgq, sync_gps_stats] == map(type, data), 'wrong types of arguments'
        self.sync_data, self.pll_stats, self.gps_stats = data
        
# Functions to parse strings into structs

def parse_sync_point(string):
    """ Parses the beginning of STRING as a sync_point. Returns the
    corresponding sync_point object and the remainder of the string. """
    return sync_point(*struct.unpack('<ff', string[:8])), string[8:]
    
def parse_sync_output_msgq(string):
    """ Parses the beginning of STRING as a sync_output_msgq. Returns the
    corresponding sync_output_msgq object and the remainder of the string. """
    f1 = struct.unpack('<f', string[:4])[0]
    f2 = struct.unpack('<iiiiii', string[4:28])
    f3 = struct.unpack(''.join(('<', ''.join('i' for _ in xrange(120)))), string[28:508])
    string = string[508:]
    frest = [] # a list of 6 lists, each corresponding to the remaining arguments
    for _ in xrange(6):
        sync_point_lst = []
        for _ in xrange(120):
            point, string = parse_sync_point(string)
            sync_point_lst.append(point)
        frest.append(tuple(sync_point_lst))
    return sync_output_msgq(f1, f2, f3, *frest), string
    
def parse_sync_pll_stats_msgq(string):
    """ Parses the beginning of STRING as a sync_pll_stats_msgq. Returns the
    corresponding sync_pll_stats_msgq object and the remainder of the string.
    """
    return sync_pll_stats_msgq(*struct.unpack('<IIii', string[:16])), string[16:]
    
def parse_sync_gps_stats(string):
    """ Parses the beginning of STRING as a sync_gps_stats. Returns the
    corresponding sync_gps_stats object and the remainder of the string. """
    return sync_gps_stats(*struct.unpack('<fffffff', string[:28])), string[28:]
    
def parse_sync_output(string):
    """ Parses the beginning of STRING as a sync_output. Returns the
    corresponding sync_output object and the remainder of the string. """
    f1, string = parse_sync_output_msgq(string)
    f2, string = parse_sync_pll_stats_msgq(string)
    f3, string = parse_sync_gps_stats(string)
    return sync_output(f1, f2, f3), string

