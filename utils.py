# Functions used by multiple files
import calendar
import datetime

def check_duplicates(sorted_struct_list):
    # Check if the structs have duplicates or missing items, print warnings if so
    dates = tuple(datetime.datetime(*s.sync_data.times) for s in sorted_struct_list)
    i = 1
    while i < len(dates):
        date1 = dates[i-1]
        date2 = dates[i]
        delta = int((date2 - date1).total_seconds() + 0.5) # round difference to nearest second
        if delta == 0:
            print 'WARNING: duplicate record for {0}'.format(str(date2))
        elif delta != 1:
            print 'WARNING: missing record(s) (skips from {0} to {1})'.format(str(date1), str(date2))
        i += 1
        
def time_to_nanos(lst):
    """ Converts the time as given in a time[] array into nanoseconds since
the epoch. """
    return 1000000000 * calendar.timegm(datetime.datetime(*lst).utctimetuple())
    
def lst_to_rows(parsed):
    rows = []
    for s in parsed:
        basetime = time_to_nanos(s.sync_data.times)
        # it seems s.sync_data.sampleRate is the number of milliseconds between samples
        timedelta = 1000000 * s.sync_data.sampleRate # nanoseconds between samples
        i = 0
        while i < 120:
            row = []
            row.append(basetime + int((i * timedelta) + 0.5))
            row.append(s.sync_data.lockstate[i])
            for start in ('L', 'C'):
                for num in xrange(1, 4):
                    attribute = getattr(s.sync_data, '{0}{1}MagAng'.format(start, num))
                    row.append(attribute[i].angle)
                    row.append(attribute[i].mag)
            row.append(s.gps_stats.satellites)
            row.append(s.gps_stats.hasFix)
            i += 1
            rows.append(row)
    return rows
