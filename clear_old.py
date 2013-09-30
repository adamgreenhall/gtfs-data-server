from utils import pd, set_trace
from config import con
from datetime import datetime

t = (pd.Timestamp(datetime.now()) - pd.DateOffset(days=7)).strftime('%Y-%m-%d')

query_stop = """
delete from
    stop_time_updates ST
    using trip_updates T
where ST.trip_update_id = T.oid
and T.timestamp < '{}'""".format(t)

query_trip = """
delete from trip_updates where timestamp < '{}'
""".format(t)

cur = con.cursor()
try:
    cur.execute(query_stop)
    print('deleted stop updates: {}'.format(cur.rowcount))
    cur.execute(query_trip)
    print('deleted trip updates: {}'.format(cur.rowcount))
    con.commit()
finally:
    con.close()