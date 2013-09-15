import pandas as pd
import sys
from datetime import timedelta
from pdb import set_trace

def service_type(timestamp):
    '''get BART service_id'''
    dayofweek = pd.Timestamp(timestamp).dayofweek # monday is 0, ..., sat is 5, sun is 6
    if dayofweek == 5:
        return 'SAT'
    elif dayofweek == 6:
        return 'SUN'
    else:
        return 'WKDY'
        
def prints(*args):
    # heroku print statement
    print(args)
    sys.stdout.flush()
    
def mod24hrs(s):
    hrs, mmss = s.split(':', 1)
    return "{:02d}:{}".format(int(hrs) - 24, mmss)

def parse_date_times(date, tseries):
    '''
    convert transit agency's strange datetimes that wrap over 24hrs
    tseries is a series of time strings
    '''
    daystr = date.strftime('%Y-%m-%d ')
    try: 
        return pd.to_datetime(daystr + tseries, errors='raise')
    except:
        roll_over_midnight = tseries.str.findall('^2[4-9].+').apply(len) > 0
        
        tseries.ix[roll_over_midnight] = \
            tseries.ix[roll_over_midnight].apply(mod24hrs)
        datetimes = pd.to_datetime(daystr + tseries, coerce=True)
        datetimes.ix[roll_over_midnight] = \
            datetimes.ix[roll_over_midnight] + timedelta(days=1)
        return datetimes
    