"""
script to load spreadsheet of tampa bicycle citations into panda
"""

import os, sys
import json
import requests
import datetime
from dateutil import parser
from django.template.defaultfilters import slugify
from csvkit import CSVKitDictReader as cdr

today = datetime.datetime.today().date()
def parse_dob(value):
    turndate = datetime.date(2004, 1, 1)# allow for any citees as young as 10
    try:
        DOB = parser.parse(value.strip()).date()
    except:
        print "couldn't parse dob %s" % value
        return None
    else:
        if DOB == today:
            return None
        elif DOB > turndate:
            return DOB.replace(year=DOB.year-100)
        else:
            return DOB

class RunVars:
    """class to track run variables"""
    def __init__(self):
        self.starter = datetime.datetime.now()
        self.processed = 0
        self.updated = 0
        self.created = 0
        self.passed = 0

# panda params
PANDA_BASE = os.getenv('PANDA_BASE')
PANDA_AUTH_PARAMS = {
    'email': os.getenv('PANDA_USER'),
    'api_key': os.getenv('PANDA_API_KEY')
}
QUERY_LIMIT = 1200
PANDA_BULK_UPDATE_SIZE = 1000
PANDA_API = '%s/api/1.0' % PANDA_BASE
PANDA_ALL_DATA_URL = "%s/data/" % PANDA_API
PANDA_DATASET_BASE = "%s/dataset" % PANDA_API
# PANDA_VOTERS_SUFFIX = '&category=voters'
# PANDA_NEWSMAKERS_BASE = '%s/arrest-watchlist' % PANDA_DATASET_BASE
# PANDA_NEWSMAKERS_DATA_URL = '%s/data/' % PANDA_NEWSMAKERS_BASE

def panda_get(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.get(url, params=params)

def panda_put(url, data, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.put(url, data, params=params, headers={ 'Content-Type': 'application/json' })

def panda_delete(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)

def get_exid(query):
    dat = json.loads(panda_get(PANDA_NEWSMAKERS_DATA_URL, params={'q': query}).text)
    return dat['objects'][0]['external_id']

def get_uri(query):
    dat = json.loads(panda_get(PANDA_NEWSMAKERS_DATA_URL, params={'q': query}).text)
    return dat['objects'][0]['resource_uri']

def update_panda(put_data):
    itemcount = len(put_data['objects'])
    if itemcount <= PANDA_BULK_UPDATE_SIZE:
        panda_put(PANDA_NEWSMAKERS_DATA_URL, json.dumps(put_data))
        print "sent %s items to panda" % itemcount
        return itemcount
    else:
        print "too many objects to send at once -- found %s" % itemcount
        return None

# Panda loading vessel
put_data = {'objects': []}

"""
RAW HEADINGS
  1: ID
  2: Uniform Case Number
  3: Citaion Number
  4: Case Number
  5: Law Enf Agency Name
  6: Law Enf Officer Name
  7: Offense Date
  8: Received Date
  9: Last Name
 10: First Name
 11: Middle Name
 12: Suffix
 13: Address Line 1
 14: Address Line 2               ## add to 13
 15: City                                ## add to 13
 16: State                              ## add to 13
 17: Zip Code                        ## add to 13
 18: Date Of Birth
 19: Race
 20: Gender
 21: Driver License Number
 22: Driver License State       ## add to 21
 23: Commercial Vehicle
 24: Statute
 25: Statute Description
 26: Posted Speed
 27: Actual Speed
 28: Disposition
 29: Disposition Date
 30: Amount Paid
 31: Date Paid
 32: Defensive Driving School (DDS)
 33: DDS Court Ordered
 34: DDS Elected (Regular)
 35: DDS Elected (Advanced)
 36: Tag Number
 37: Tag State                  ## add to 36
 38: Case Filed Date
 39: Case Closed Date
 40: Offense Year
 41: Age

NOTE: can clean up the BOM (byte order mark) character, and many other encoding poblems with ftfy's fix_text function, like so:
import ftfy
bad_col = reader.fieldnames[0]
bad_col
>> u'\ufeffUniform Case Number'

ftfy.fix_text(bad_col)
>> u'Uniform Case Number'

desired-columns = '1,9,10,11,12,18,19,20,21,22,13,14,15,16,17,7,25,5,6'
"""

# initiate Panda dataset -- only  run once
headings = [
    u'lname',
    u'fname',
    u'mname',
    u'suffix',
    u'DOB',
    u'race',
    u'gender',
    u'DL',
    u'address',
    u'date',
    u'statute',
    u'agency',
    u'officer'
]
dataset_name = u"Tampa bicycle citations 2003 to 2014"
dataset_slug = slugify(dataset_name)
dataset_url = '%s/%s/' % (PANDA_DATASET_BASE, dataset_slug)
data_url = '%sdata/' % dataset_url
dataset_dict = {
    'name': dataset_name,
    'description': 'A list of bicycle citations in Hillsborough County from 2003 to 2014.',
    'categories': [u'/api/1.0/category/all-dob/', u'/api/1.0/category/crime/', u'/api/1.0/category/traffic-tickets/']
}
def initialize_dataset():
    rtest = panda_get(dataset_url)
    if rtest.status_code ==  404: # dataset doesn't exist, so we can create it
        response = panda_put(
                dataset_url, 
                json.dumps(dataset_dict), 
                params={ 'columns': ','.join(headings), }
                )
        if response.status_code==201:
            print "created panda dataset %s" % dataset_name
            return True
        else:
            print "failed with status code %s and reason %s" % (response.status_code, response.reason)
            return False
    elif rtest.reason == 'OK':
        testd = json.loads(rtest.text)
        print "dataset already created and has %s rows; can proceed with upload" % testd['row_count']
        return True

infile = "/data/AllBikeViolations.csv"
runv = RunVars()
def load_tickets():
    with open(infile, 'r') as f:
        reader = cdr(f)
        for row in reader:
            runv.processed += 1
            PK = row['ID']
            ADDR = ", ".join([ row[ky] for ky in ['Address Line 1', 'Address Line 2', 'City', 'State', 'Zip Code'] if row[ky] ])
            DL = "%s (%s)" % (row['Driver License Number'], row['Driver License State'])
            # TAG = "%s (%s)" % (row['Tag Number'], row['Tag State'])
            put_data['objects'].append({
                'external_id': unicode(PK),
                'data': [
                    row['Last Name'],
                    row['First Name'],
                    row['Middle Name'],
                    row['Suffix'],
                    row['Date Of Birth'],
                    row['Race'].replace('White', 'Wh').replace('Black', 'Bl'),
                    row['Gender'],
                    DL,
                    ADDR,
                    row['Offense Date'],
                    row['Statute Description'],
                    row['Law Enf Agency Name'],
                    row['Law Enf Officer Name']
                    ]
            })
            if len(put_data['objects']) == 1000:
                runv.created += 1000
                print "shipped %s rows" % runv.created
                panda_put(data_url, json.dumps(put_data))
                put_data['objects'] = []
    if put_data['objects']:
        print 'shipping final %i rows' % len(put_data['objects'])
        panda_put(data_url, json.dumps(put_data))
        runv.created += len(put_data['objects'])
        put_data['objects'] = []
    print "pushed %s rows to panda dataset %s; process took %s" % (
                                                                    runv.created, 
                                                                    dataset_name, 
                                                                    (datetime.datetime.now()-runv.starter)
                                                                    )

# if you want the script to run automatically,
# uncomment the lines starting with 'if __name__ ...'
# you can optionally put a csv file in /data 
# and pass its name to the script like so:
#        python load_tampa_bike_citations.py FILENAME
#
#
# if __name__ == "__main__":
#     try:
#         param = sys.argv[1]
#     except:
#         pass
#     else:
#         infile = "/data/%s" % param
#     if initialize_dataset():
#         load_tickets()
