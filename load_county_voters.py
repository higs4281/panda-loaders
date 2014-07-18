import datetime
import os, sys, json
from subprocess import call

import requests
from csvkit import CSVKitDictReader as cdr
from csvkit import CSVKitWriter as ckw

from django.template.defaultfilters import slugify

newsweb = 'http://web_news/voters/'
data_month = 'July'
today = datetime.datetime.today().date()
year = today.year
base = "/opt/django-projects/standalones/panda/voter_reg"
yearbase = "%s/%s" % (base, year)
rawbase = "%s/VoterExtract" % yearbase
temp = "%s/temp" % yearbase
prepbase = "%s/prep" % yearbase
loadbase = "%s/load" % yearbase
rawheader = "%s/HEADER.txt" % base

# vars stored in the env
PANDA_AUTH_PARAMS = {
    'email': os.getenv('PANDA_USER'),
    'api_key': os.getenv('PANDA_API_KEY')
}
PANDA_BASE = os.getenv('PANDA_BASE')

#panda api vars
QUERY_LIMIT = 1200
PANDA_BULK_UPDATE_SIZE = 1000
PANDA_API = '%s/api/1.0' % PANDA_BASE
PANDA_ALL_DATA_URL = "%s/data/" % PANDA_API
PANDA_DATASET_BASE = "%s/dataset" % PANDA_API
PANDA_VOTERS_SUFFIX = '&category=voters'

def panda_get(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.get(url, params=params)

def panda_put(url, data, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.put(url, data, params=params, headers={ 'Content-Type': 'application/json' })

def panda_delete(url, params={}):
    params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)

VOTER_COLUMNS = [
    u'lname', 
    u'fname', 
    u'mname', 
    u'suffix', 
    u'addr1', 
    u'addr2', 
    u'city', 
    u'zip', 
    u'gender', 
    u'race', 
    u'birthdate', 
    u'party', 
    u'areacode', 
    u'phone',
    u'email'
    ]
raced ={
    '1': 'American Indian',
    '2': 'Asian',
    '3': 'BL',# using BL so searches for people named 'Black' will work
    '4': 'Hispanic',
    '5': 'WH',# using WH so searches for people named 'White' will work
    '6': 'Other',
    '7': 'Multiracial',
    '9': 'Unknown'#yes, there is no 8
    }
partyd = {#updated for 2014
    'DEM': 'Democratic', 
    'REP': 'Republican', 
    'TPF': 'Tea Party', 
    'GRE': 'Green Party', 
    'INT': 'Independent Party',
    'IDP': 'Independence Party', 
    'LPF': 'Libertarian', 
    'NO PARTY': 'no party',
    'NP': 'no party',
    'AIP': "American's",
    'CPF': "Constitution",
    'ECO': "Ecology",
    'FPP': "Pirate",
    'PFP': "Peace & Freedom",
    'PEACE & FREEDOM': "Peace & Freedom",
    'REF': "Reform",
    'FSW': "Socialist Workers",
    'JPF': "Justice",
    'PSL': "Socialism and Liberation",
    }    
to_harvest = {
    # 'CIT': 'Citrus',
    # 'HER': 'Hernando',
    # 'HIL': 'Hillsborough',
    # 'LAK': 'Lake',
    # 'MAN': 'Manatee',
    # 'ORA': 'Orange',
    # 'OSC': 'Osceola',
    # 'PAS': 'Pasco',
    # 'PIN': 'Pinellas',
    'POL': 'Polk',
    'SAR': 'Sarasota',
    'SUM': 'Sumter'
    }
columns = "3,5,6,4,8,9,10,12,20,21,22,24,35,36,38,2"

def prep(filename):
    prepstart = datetime.datetime.now()
    slug = filename[:3]
    rawfile = "%s/%s" % (rawbase, filename)
    tempfile = "%s/%s_temp.csv" % (temp, slug)
    prepfile = "%s/%s_prep.csv" % (prepbase, slug)
    loadfile = "%s/%s.csv" % (loadbase, slug)
    call("cp %s %s" % (rawheader, tempfile), shell=True)
    call('cat %s/%s >> %s' % (rawbase, filename, tempfile), shell=True)
    call("csvcut -t -c %s %s > %s" % (columns, tempfile, prepfile), shell=True)
    with open(prepfile, 'r') as f:
        biglist = []
        reader = cdr(f)
        header = reader.fieldnames
        for row in reader:
            if row['race'] in raced.keys():
                RACE = raced[row['race']]
            else:
                RACE = ''
            if row['party'] in partyd.keys():
                PARTY = partyd[row['party']] 
            else:
                PARTY = ''
            biglist.append([
                    row['lname'].strip(), 
                    row['fname'].strip(), 
                    row['mname'].strip(), 
                    row['suffix'].strip(), 
                    ' '.join(row['addr1'].split()),#strips extra interior white space
                    row['addr2'].strip(), 
                    row['city'].strip(), 
                    row['zip'].strip(), 
                    row['gender'].strip(), 
                    RACE, 
                    row['birthdate'].strip(), 
                    PARTY, 
                    row['areacode'].strip(), 
                    row['phone'].strip(),
                    row['email'].strip(),
                    row['voter_ID'].strip()
                    ])
        biglist=sorted(biglist)#sorts list of lists based on first field, which is last name
        with open(loadfile, 'w') as lf:
            writer = ckw(lf)
            writer.writerow(header)
            for entry in biglist:
                writer.writerow(entry)
    print "%s ready for loading; prepping took %s" % (loadfile (datetime.datetime.now-prepstart))

def no_dotfiles(path):
    for f in os.listdir(path):
        if not f.startswith('.'):
            yield f

def prep_files():
    for each in no_dotfiles(rawbase):
        if each[:3] in to_harvest.keys():
            prep(each)

put_data = {'objects': []}
def export_county(countyfile):
    putstart = datetime.datetime.now()
    putcount = 0
    slug = countyfile[:3]
    if slug not in to_harvest.keys():
        print "county isn't in the to_harvest list"
        return putcount
    else:
        county = to_harvest[slug]
        name = "%s voter registration %s" % (county, year)
        dataset_slug = slugify(name)
        dataset_url = '%s/%s/' % (PANDA_DATASET_BASE, dataset_slug)
        data_url = '%sdata/' % dataset_url
        #initialize new dataset
        dataset = {
            'name': name,
            'description': 'Data from %s %s; to search statewide, visit %s' % (data_month, year, newsweb),
            'categories': [u'/api/1.0/category/all-dob/', u'/api/1.0/category/voters/']
        }
        response = panda_put(dataset_url, json.dumps(dataset), params={
            'columns': ','.join(VOTER_COLUMNS),
        })
        with open("%s/%s" % (loadbase, countyfile), 'r') as cf:
            reader = cdr(cf)
            for row in reader:
                put_data['objects'].append({
                    'external_id': unicode(row['voter_ID']),
                    'data': [row[key] for key in VOTER_COLUMNS]
                })
                if len(put_data['objects']) % 500 == 0:
                    print "500 processed"
                if len(put_data['objects']) == 1000:
                    putcount += 1000
                    print 'Updating %i rows' % len(put_data['objects'])
                    panda_put(data_url, json.dumps(put_data))
                    put_data['objects'] = []
        if put_data['objects']:
            print 'Updating %i rows' % len(put_data['objects'])
            panda_put(data_url, json.dumps(put_data))
            putcount += len(put_data['objects'])
            put_data['objects'] = []
        print "pushed %s rows to panda dataset %s; process took %s" % (putcount, 
                                                                        name, 
                                                                        (datetime.datetime.now()-putstart)
                                                                        )
        return putcount

def export_all():
    for countyfile in no_dotfiles(loadbase):
        slug = countyfile[:3]
        if not slug in to_harvest.keys():
            print "county %s isn't in the to_harvest list" % to_harvest[slug]
            continue
        else:
            export_county(countyfile)




