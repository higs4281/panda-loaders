import csv
import datetime
import json
import os
import subprocess
import sys

import requests
from dateutil import parser
from django.template.defaultfilters import slugify

# SET VOTER_DATA_DATE to the date on the voter disk
VOTER_DATA_DATE = datetime.datetime(2019, 5, 12).date()
if os.getenv('VOTER_DATA_DATE'):
    VOTER_DATA_DATE = parser.parse(os.getenv('VOTER_DATA_DATE')).date()

YEAR = VOTER_DATA_DATE.year

# PANDA VARS
PANDA_AUTH_PARAMS = {
    'email': os.getenv('PANDA_USER'),
    'api_key': os.getenv('PANDA_API_KEY')
}
PANDA_BASE = os.getenv('PANDA_BASE')
QUERY_LIMIT = 1200
PANDA_BULK_UPDATE_SIZE = 1000
PANDA_API = '{}/api/1.0'.format(PANDA_BASE)
PANDA_ALL_DATA_URL = "{}/data/".format(PANDA_API)
PANDA_DATASET_BASE = "{}/dataset".format(PANDA_API)
PANDA_VOTERS_SUFFIX = '&category=voters'

# FILE SYSTEM VARS
BASE = os.getenv('PANDA_LOADERS_BASE_DIR', "/tmp")
YEARBASE = "{}/{}".format(BASE, YEAR)
RAWBASE = "{}/VoterDetail".format(YEARBASE)
TEMP = "{}/temp".format(YEARBASE)
PREPBASE = "{}/prep".format(YEARBASE)
LOADBASE = "{}/load".format(YEARBASE)
LOADED = "{}/loaded".format(YEARBASE)
RAWHEADER = "{}/HEADER.txt".format(BASE)  # tab-delimited original header


def get_postgres_db_name():
    return "voter_data_{}".format(VOTER_DATA_DATE).replace('-', '_')


def prep_directories():
    """Make sure directories exist and are empty."""
    for each in [YEARBASE, RAWBASE, TEMP, PREPBASE, LOADBASE, LOADED]:
        if not os.path.isdir(each):
            os.mkdir(each)
    for _dir in [TEMP, PREPBASE]:
        for _file in os.listdir(_dir):
            subprocess.run(['rm', '{}/{}'.format(_dir, _file)])
        # for _file in os.listdir(LOADBASE):
        #     orig = "{}/{}".format(LOADBASE, _file)
        #     subprocess.run(['mv', '{} {}'.format(orig, LOADED)])


def panda_get(url, params):
    """GET call to Panda API."""
    if not params:
        params = PANDA_AUTH_PARAMS
    else:
        params.update(PANDA_AUTH_PARAMS)
    return requests.get(url, params=params)


def panda_put(url, data, params):
    """PUT call to Panda API."""
    if not params:
        params = PANDA_AUTH_PARAMS
    else:
        params.update(PANDA_AUTH_PARAMS)
    return requests.put(
        url, data, params=params,
        headers={'Content-Type': 'application/json'})


def panda_delete(url, params):
    """DELETE call to Panda API."""
    if not params:
        params = PANDA_AUTH_PARAMS
    else:
        params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)


VOTER_COLUMNS = [
    'lname',
    'fname',
    'mname',
    'suffix',
    'addr1',
    'addr2',
    'city',
    'zip',
    'gender',
    'race',
    'birthdate',
    'party',
    'areacode',
    'phone',
    'email',
    'voter_ID',
]

_RACE = {
    '1': 'American Indian',
    '2': 'Asian',
    '3': 'BL',  # using BL so searches for people named 'Black' will work
    '4': 'Hispanic',
    '5': 'WH',  # using WH so searches for people named 'White' will work
    '6': 'Other',
    '7': 'Multiracial',
    '9': 'Unknown'  # yes, there is no 8
}
_PARTY = {  # Codes for parties registered in Florida as of October 2018
    'CPF': "Constitution",
    'DEM': 'Democratic',
    'ECO': "Ecology",
    'GRE': 'Green Party',
    'IND': 'Independent',
    'LPF': 'Libertarian',
    'NPA': 'no party',
    'PSL': "Socialism and Liberation",
    'REF': "Reform",
    'REP': 'Republican',
    # deprecated abbreviations, kept for parsing older files
    'AIP': "American's",
    'FPP': "Pirate",
    'FSW': "Socialist Workers",
    'IDP': 'Independence Party',
    'INT': 'Independent Party',
    'JPF': "Justice",
    'NO PARTY': 'no party',
    'NP': 'no party',
    'PEACE & FREEDOM': "Peace & Freedom",
    'PFP': "Peace & Freedom",
    'TPF': 'Tea Party',
}

ALL_FL_COUNTIES = {
    "ALA": "Alachua",
    "BAK": "Baker",
    "BAY": "Bay",
    "BRA": "Bradford",
    "BRE": "Brevard",
    "BRO": "Broward",
    "CAL": "Calhoun",
    "CHA": "Charlotte",
    "CIT": "Citrus",
    "CLA": "Clay",
    "CLL": "Collier",
    "CLM": "Columbia",
    "DAD": "Miami-Dade",
    "DES": "Desoto",
    "DIX": "Dixie",
    "DUV": "Duval",
    "ESC": "Escambia",
    "FLA": "Flagler",
    "FRA": "Franklin",
    "GAD": "Gadsden",
    "GIL": "Gilchrist",
    "GLA": "Glades",
    "GUL": "Gulf",
    "HAM": "Hamilton",
    "HAR": "Hardee",
    "HEN": "Hendry",
    "HER": "Hernando",
    "HIG": "Highlands",
    "HIL": "Hillsborough",
    "HOL": "Holmes",
    "IND": "Indian River",
    "JAC": "Jackson",
    "JEF": "Jefferson",
    "LAF": "Lafayette",
    "LAK": "Lake",
    "LEE": "Lee",
    "LEO": "Leon",
    "LEV": "Levy",
    "LIB": "Liberty",
    "MAD": "Madison",
    "MAN": "Manatee",
    "MON": "Monroe",
    "MRN": "Marion",
    "MRT": "Martin",
    "NAS": "Nassau",
    "OKA": "Okaloosa",
    "OKE": "Okeechobee",
    "ORA": "Orange",
    "OSC": "Osceola",
    "PAL": "PalmBeach",
    "PAS": "Pasco",
    "PIN": "Pinellas",
    "POL": "Polk",
    "PUT": "Putnam",
    "SAN": "SantaRosa",
    "SAR": "Sarasota",
    "SEM": "Seminole",
    "STJ": "St.Johns",
    "STL": "St.Lucie",
    "SUM": "Sumter",
    "SUW": "Suwannee",
    "TAY": "Taylor",
    "UNI": "Union",
    "VOL": "Volusia",
    "WAK": "Wakulla",
    "WAL": "Walton",
    "WAS": "Washington"
}

MID_FLORIDA = {  # Counties used for Tampa Bay regional reference db
    "ALA": "Alachua",
    "BRE": "Brevard",
    "CHA": "Charlotte",
    "CIT": "Citrus",
    "DES": "Desoto",
    "DUV": "Duval",
    "HAR": "Hardee",
    "HER": "Hernando",
    "HIL": "Hillsborough",
    "LAK": "Lake",
    "LEE": "Lee",
    "LEO": "Leon",
    "MAN": "Manatee",
    "MRN": "Marion",
    "ORA": "Orange",
    "PAS": "Pasco",
    "PIN": "Pinellas",
    "POL": "Polk",
    "SAR": "Sarasota",
    "SEM": "Seminole",
}

SOUTH_FLORIDA = {  # southern counties not covered by mid_Florida list
    'BRO': 'Broward',
    'CLL': 'Collier',
    'DAD': "Miami-Dade",
    'GLA': "Glades",
    'HEN': "Hendry",
    'MON': "Monroe",
    'MRT': "Martin",
    'PAL': "PalmBeach",
}

COLUMNS = "3,5,6,4,8,9,10,12,20,21,22,24,35,36,38,2"


def stage_local_files(filename, slug):
    """Handle local file-copying operations."""
    tempfile = "{}/{}_temp.csv".format(TEMP, slug)
    prepfile = "{}/{}_prep.csv".format(PREPBASE, slug)
    subprocess.run("cp {} {}".format(RAWHEADER, tempfile), shell=True)
    subprocess.run('cat {}/{} >> {}'.format(
        RAWBASE, filename, tempfile), shell=True)
    subprocess.run("csvcut -t -c {} {} > {}".format(
        COLUMNS, tempfile, prepfile), shell=True)
    return prepfile


def clean_processing_directories():
    for directory in [TEMP, PREPBASE]:
        subprocess.run("rm {}/*".format(directory), shell=True)


def prep(filename):
    """
    Prepare a raw voter .txt file.

    - Add a raw header
    - Remove unwanted columns
    - Remove unwanted columns with csvcut
    - Process the rows, fixing race and party values
    - Replace column names with our preferred headings for Panda
    - Sort the results by last name
    - Output a final CSV ready for loading into Panda
    """
    prepstart = datetime.datetime.now()
    slug = filename[:3]
    loadfile = "{}/{}.csv".format(LOADBASE, slug)
    prepfile = stage_local_files(filename, slug)
    with open(prepfile, 'r') as prepped_file:
        biglist = []
        reader = csv.DictReader(prepped_file)
        header = reader.fieldnames
        for row in reader:
            race = row.get('race')
            if race in _RACE.keys():
                race = _RACE[race]
            party = row.get('party')
            if party in _PARTY.keys():
                party = _PARTY[party]
            biglist.append([
                row['lname'].strip(),
                row['fname'].strip(),
                row['mname'].strip(),
                row['suffix'].strip(),
                ' '.join(row['addr1'].split()),  # clean interior white spaces
                row['addr2'].strip(),
                row['city'].strip(),
                row['zip'].strip(),
                row['gender'].strip(),
                race,
                row['birthdate'].strip(),
                party,
                row['areacode'].strip(),
                row['phone'].strip(),
                row['email'].strip(),
                row['voter_ID'].strip()
            ])
        biglist = sorted(biglist)  # sorts list of lists based on last name
        with open(loadfile, 'w') as load_file:
            writer = csv.writer(load_file)
            writer.writerow(header)
            for entry in biglist:
                writer.writerow(entry)
    print("{} ready for loading; prepping took {}".format(
        loadfile, (datetime.datetime.now() - prepstart)))


def no_dotfiles(path):
    """Eliminate dot-files from consideration."""
    for local_file in os.listdir(path):
        if not local_file.startswith('.'):
            yield local_file


def prep_files():
    """Cycle through entries and prep them."""
    for each in no_dotfiles(RAWBASE):
        slug = each[:3]
        if slug in ALL_FL_COUNTIES:
            print(
                "Prepping voter data for {}".format(ALL_FL_COUNTIES.get(slug)))
            prep(each)
    clean_processing_directories()


def load_postgres():
    db = get_postgres_db_name()
    if db not in subprocess.getoutput('psql -l'):
        subprocess.run('createdb {}'.format(db), shell=True)
        subprocess.run(
            'psql {} -f create_voter_tables.sql'.format(db), shell=True)
        set_date_command = (
            'echo "ALTER TABLE voters_voter ALTER COLUMN source_date '
            'SET DEFAULT \'{}\';" | psql {}'.format(VOTER_DATA_DATE, db))
        subprocess.run(set_date_command, shell=True)
    for each in no_dotfiles(LOADBASE):
        slug = each[:3]
        if slug in ALL_FL_COUNTIES:
            load_to_postgres(db, each, slug)
    subprocess.run('psql {} -f index_voter_tables.sql'.format(db), shell=True)


def load_to_postgres(db, file_name, slug):
    set_county_command = (
        'echo "ALTER TABLE voters_voter ALTER COLUMN county_slug '
        'SET DEFAULT \'{}\';" | psql {}'.format(slug, db))
    subprocess.run(set_county_command, shell=True)
    load_county_command = (
        'echo "COPY voters_voter (lname, fname, mname, suffix, addr1, addr2, '
        'city, zip, gender, race, birthdate, party, areacode, phone, email, '
        'voter_id) FROM \'{}/{}.csv\' CSV HEADER;" | psql {}'.format(
            LOADBASE, slug, db))
    subprocess.run(load_county_command, shell=True)


def export_county(countyfile):
    """Export one county CSV file to Panda."""
    put_data = {'objects': []}
    putstart = datetime.datetime.now()
    putcount = 0
    slug = countyfile[:3]
    if slug not in ALL_FL_COUNTIES.keys():
        print("'{}'' isn't a standard Florida county voter slug".format(slug))
        return putcount
    else:
        county = ALL_FL_COUNTIES[slug]
        name = "{} voter registration {}".format(county, YEAR)
        dataset_slug = slugify(name)
        dataset_url = '{}/{}/'.format(PANDA_DATASET_BASE, dataset_slug)
        data_url = '{}data/'.format(dataset_url)
        # initialize new dataset
        dataset = {
            'name': name,
            'description': 'Data from {}'.format(VOTER_DATA_DATE),
            'categories': ['/api/1.0/category/all-dob/',
                           '/api/1.0/category/voters/']
        }
        panda_put(
            dataset_url,
            json.dumps(dataset),
            params={'columns': ','.join(VOTER_COLUMNS)})
        with open("{}/{}".format(LOADBASE, countyfile), 'r') as cfile:
            reader = csv.DictReader(cfile)
            for row in reader:
                put_data['objects'].append({
                    'external_id': (row['voter_ID']),
                    'data': [row[key] for key in VOTER_COLUMNS]
                })
                if len(put_data['objects']) % 500 == 0:
                    print("500 processed")
                if len(put_data['objects']) == 1000:
                    putcount += 1000
                    print('Updating {} rows'.format(len(put_data['objects'])))
                    panda_put(data_url, json.dumps(put_data), params={})
                    put_data['objects'] = []
                if putcount % 10000 == 0:
                    print("loaded so far: {}".format(putcount))
        if put_data['objects']:
            print('Updating {} rows'.format(len(put_data['objects'])))
            panda_put(data_url, json.dumps(put_data), params={})
            putcount += len(put_data['objects'])
            put_data['objects'] = []
        print("pushed {} rows to panda dataset {}; process took {}".format(
            putcount, name, (datetime.datetime.now() - putstart)))
        return putcount


def export_to_panda():
    """Export any prepped county files in LOADBASE directory to PANDA."""
    for countyfile in no_dotfiles(LOADBASE):
        export_county(countyfile)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] == 'prep_files':
            print("Prepping any county-slugged files in {}".format(RAWBASE))
            prep_files()
        elif sys.argv[1] == 'load_postgres':
            load_postgres()
        elif sys.argv[1] == 'export_to_panda':
            export_to_panda()
        else:
            voter_file = sys.argv[1]
            print("Prepping voter data for {} County".format(
                ALL_FL_COUNTIES.get(voter_file[:3]))
            )
            prep(voter_file)
    else:
        print(
            "Please provide either a single voter file name to process, "
            "or one of these arguments: \n"
            "- prep_files (to prep any county files in /VoterDetail)\n"
            "- load_postgres (to create and load county files to a database)\n"
            "- export_to_panda (to export any prepped county files to PANDA)."
        )
