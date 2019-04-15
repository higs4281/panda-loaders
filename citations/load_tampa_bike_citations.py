from __future__ import unicode_literals

import csv
import datetime
import json
import os
import sys

import requests
from dateutil import parser
from django.template.defaultfilters import slugify


class RunVars:
    """Track runtime variables."""

    def __init__(self):
        self.starter = None
        self.processed = 0
        self.updated = 0
        self.created = 0
        self.passed = 0

# panda params
RUNVARS = RunVars()
RUNVARS.starter = datetime.datetime.now()
PANDA_BASE_URL = os.getenv('PANDA_BASE_URL')
PANDA_AUTH_PARAMS = {
    'email': os.getenv('PANDA_USER'),
    'api_key': os.getenv('PANDA_API_KEY')
}
QUERY_LIMIT = 1200
PANDA_BULK_UPDATE_SIZE = 1000
PANDA_API = '%s/api/1.0' % PANDA_BASE_URL
PANDA_ALL_DATA_URL = "%s/data/" % PANDA_API
PANDA_DATASET_BASE = "%s/dataset" % PANDA_API
DATASET_NAME = "Tampa bicycle citations 2003 to 2014"
DATASET_SLUG = slugify(DATASET_NAME)
DATASET_URL = '{}/{}/'.format(PANDA_DATASET_BASE, DATASET_SLUG)
DATA_URL = '{}data/'.format(DATASET_URL)


def parse_dob(value):
    turndate = datetime.date(2004, 1, 1)  # allow for any citees as young as 10
    try:
        dob = parser.parse(value.strip()).date()
    except Exception as e:
        print("Couldn't parse dob {} ({})".format(value, e))
        return None
    else:
        if dob == datetime.date.today():
            return None
        elif dob > turndate:
            return dob.replace(year=dob.year - 100)
        else:
            return dob


def panda_get(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.get(url, params=params)


def panda_put(url, data, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.put(
        url, data,
        params=params,
        headers={'Content-Type': 'application/json'})


def panda_delete(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)


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

NOTE: can clean up the BOM (byte order mark) character,
and many other encoding poblems, with ftfy's fix_text function, like so:

import ftfy
bad_col = reader.fieldnames[0]
bad_col
>> '\ufeffUniform Case Number'

ftfy.fix_text(bad_col)
>> 'Uniform Case Number'

desired-columns = '1,9,10,11,12,18,19,20,21,22,13,14,15,16,17,7,25,5,6'
"""


def initialize_dataset():
    """Initiate a PANDA dataset for bicycle citations."""
    output_headings = [
        'lname',
        'fname',
        'mname',
        'suffix',
        'DOB',
        'race',
        'gender',
        'DL',
        'address',
        'date',
        'statute',
        'agency',
        'officer',
    ]
    dataset_dict = {
        'name': DATASET_NAME,
        'description': (
            'A list of bicycle citations in Hillsborough County '
            'from 2003 to 2014.'),
        'categories': [
            '/api/1.0/category/all-dob/',
            '/api/1.0/category/crime/',
            '/api/1.0/category/traffic-tickets/'
        ]
    }
    rtest = panda_get(DATASET_URL)
    if rtest.status_code == 404:  # Dataset doesn't exist, so we can create it
        response = panda_put(
            DATASET_URL,
            json.dumps(dataset_dict),
            params={'columns': ','.join(output_headings)}
        )
        if response.status_code == 201:
            print("Created PANDA dataset {}".format(DATASET_NAME))
            return True
        else:
            print(
                "Failed with status code {} and reason {}".format(
                    response.status_code, response.reason))
            return False
    elif rtest.reason == 'OK':
        testd = json.loads(rtest.text)
        print(
            "Dataset is already created and has {} rows; "
            "We can proceed with upload.".format(testd['row_count']))
        return True


def load_tickets(infile):
    if not os.path.isfile(infile):
        print("Couldn't find the source file '{}'".format(infile))
        return
    address_rows = [
        'Address Line 1', 'Address Line 2', 'City', 'State', 'Zip Code']
    put_data = {'objects': []}
    with open(infile, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            RUNVARS.processed += 1
            pk = row['ID']
            addr = ", ".join([row[key] for key in address_rows if row[key]])
            dl = "{} ({})".format(
                row['Driver License Number'], row['Driver License State'])
            put_data['objects'].append({
                'external_id': str(pk),
                'data': [
                    row['Last Name'],
                    row['First Name'],
                    row['Middle Name'],
                    row['Suffix'],
                    row['Date Of Birth'],
                    row['Race'].replace('White', 'Wh').replace(
                        'Black', 'Bl'),
                    row['Gender'],
                    dl,
                    addr,
                    row['Offense Date'],
                    row['Statute Description'],
                    row['Law Enf Agency Name'],
                    row['Law Enf Officer Name']
                ]
            })
            if len(put_data['objects']) == 1000:
                RUNVARS.created += 1000
                print("Shipped {} rows".format(RUNVARS.created))
                panda_put(DATA_URL, json.dumps(put_data))
                put_data['objects'] = []
    if put_data['objects']:
        print('Shipping final {} rows.'.format(len(put_data['objects'])))
        panda_put(DATA_URL, json.dumps(put_data))
        RUNVARS.created += len(put_data['objects'])
        put_data['objects'] = []
    print("pushed {} rows to panda dataset {}; process took {}".format(
        RUNVARS.created,
        DATASET_NAME,
        (datetime.datetime.now() - RUNVARS.starter)))


if __name__ == "__main__":
    """
    Load a spreadsheet of Tampa bicycle citations into PANDA.

    If no source file path is passed,
    the default will be /data/AllBikeViolations.csv
    """
    if not initialize_dataset():
        print("Couldn't initialize the PANDA dataset.")
        sys.exit(1)
    if len(sys.argv) > 1:
        source = sys.argv[1]
    else:
        source = "/data/AllBikeViolations.csv"
    load_tickets(source)
