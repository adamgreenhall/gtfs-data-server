import pandas as pd

# http://64.111.127.166/ridership/Ridership_August2013.xlsx
num_to_strs = {12.0: '12', 19.0: '19', 16.0: '16', 24.0: '24'}

stop_names = pd.read_csv('stop_names.csv', 
    delimiter=';').set_index('abbrev').stop_id

hourly_guestimate = pd.Series({
    0: 0, 1: 0, 2: 0, 3: 0, 
    4: 1, 
    5: 2,
    6: 5,
    7: 10,
    8: 30, 
    9: 40, #morning peak?
    10: 20, 
    11: 15,
    12: 15,
    13: 16,
    14: 18, 
    15: 19, 
    16: 20,
    17: 25,
    18: 35, # evening peak?
    19: 30,
    20: 25,
    21: 20,
    22: 10,
    23: 5
})

hourly_guestimate = hourly_guestimate.astype(float) / hourly_guestimate.sum()


def get_hourly(hourly_guestimate, sheetname='Weekday OD'):
    df = pd.read_excel('Ridership_August2013.xlsx', sheetname,
        skiprows=1, parse_cols=45, index_col=0)\
        .rename(columns=num_to_strs).rename(num_to_strs)

    daily_counts = pd.DataFrame(dict(
        entries=df.ix['Entries'].rename(stop_names),
        exits=df.T.ix['Exits'].rename(stop_names)
    )).drop(['Entries', 'Exits'])


    hourly_entries = pd.DataFrame({
        station: counts['entries'] * hourly_guestimate
         for station, counts in daily_counts.iterrows()})

    hourly_exits = pd.DataFrame({
        station: counts['exits'] * hourly_guestimate
         for station, counts in daily_counts.iterrows()})
    
    hourly_entries['MCAR_S'] = hourly_entries['MCAR']
    hourly_exits['MCAR_S'] = hourly_exits['MCAR']
    
    hourly_entries['19TH_N'] = hourly_entries['19TH']
    hourly_exits['19TH_N'] = hourly_exits['19TH']
    
    hourly_exits.index.name = 'hour'
    hourly_entries.index.name = 'hour'

    return hourly_exits.astype(int), hourly_entries.astype(int)

services = dict(Weekday='WKDY', Saturday='SAT', Sunday='SUN')
for sheet_name, service in services.iteritems():
    entries, exits = get_hourly(hourly_guestimate, '{} OD'.format(sheet_name))
    entries.to_csv('entries-{}.csv'.format(service))
    exits.to_csv('exits-{}.csv'.format(service))