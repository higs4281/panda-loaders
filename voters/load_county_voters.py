#!/usr/bin/env python
import csv
import datetime
import glob
import json
import os
import os.path
import subprocess
import sys

import requests
from dateutil import parser
from django.utils.text import slugify

# GET VOTER_DATA_DATE – should be set to the date on the voter disk
VOTER_DATA_DATE_STRING = os.getenv("VOTER_DATA_DATE")
if not VOTER_DATA_DATE_STRING:
    print(
        "Can't proceed without a VOTER_DATA_DATE. Please set an env var for that value"
    )
    sys.exit(1)

VOTER_DATA_DATE = parser.parse(VOTER_DATA_DATE_STRING).date()
YEAR = VOTER_DATA_DATE.year
SCRIPT_NAME = os.path.basename(__file__).split(".")[0]

# PANDA VARS
PANDA_AUTH_PARAMS = {
    "email": os.getenv("PANDA_USER"),
    "api_key": os.getenv("PANDA_API_KEY"),
}
PANDA_BASE = os.getenv("PANDA_BASE")
QUERY_LIMIT = 1200
PANDA_BULK_UPDATE_SIZE = 1000
PANDA_API = "{}/api/1.0".format(PANDA_BASE)
PANDA_ALL_DATA_URL = "{}/data/".format(PANDA_API)
PANDA_DATASET_BASE = "{}/dataset".format(PANDA_API)
PANDA_VOTERS_SUFFIX = "&category=voters"

# FILE SYSTEM VARS
BASE = os.getenv("PANDA_LOADERS_BASE_DIR", "/tmp")
YEARBASE = f"{BASE}/{YEAR}"
RAWBASE = f"{YEARBASE}/VoterDetail"
TEMP = f"{YEARBASE}/temp"
PREPBASE = f"{YEARBASE}/prep"
LOADBASE = f"{YEARBASE}/load"
LOADED = f"{YEARBASE}/loaded"
WORKING_DIRS = [RAWBASE, LOADBASE, LOADED, PREPBASE, TEMP]
PROCESSING_DIRS = WORKING_DIRS[1:]


def get_postgres_db_name():
    return f"voter_data_{VOTER_DATA_DATE_STRING.replace('-', '')}"


def empty_directory(path):
    for i in glob.glob(os.path.join(path, '*')):
        os.remove(i)


def purge_directories(dirs=None):
    if not dirs:
        dirs = WORKING_DIRS
    for directory in dirs:
        if len(os.listdir(directory)) > 0:
            empty_directory(directory)


def prep_directories():
    """Make sure working directories exist and processing dirs are empty."""
    for each in [YEARBASE] + WORKING_DIRS:
        if not os.path.isdir(each):
            os.mkdir(each)
    purge_directories(dirs=PROCESSING_DIRS)


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
        url, data, params=params, headers={"Content-Type": "application/json"}
    )


def panda_delete(url, params):
    """DELETE call to Panda API."""
    if not params:
        params = PANDA_AUTH_PARAMS
    else:
        params.update(PANDA_AUTH_PARAMS)
    return requests.delete(url, params=params)


HISTORY_CODES = {
    "A": "Voted by Mail",
    "B": "Vote-by-Mail Ballot Not Counted",
    "E": "Voted Early",
    "L": "Vote-by-Mail Ballot Not Counted, Received Late",
    "N": (
        "Did Not Vote (not all counties use this code "
        "nor are required to report this data)"
    ),
    "P": "Provisional Ballot Not Counted",
    "Y": "Voted at Polls",
}

VOTER_COLUMNS = [
    "lname",
    "fname",
    "mname",
    "suffix",
    "addr1",
    "addr2",
    "city",
    "zipcode",
    "gender",
    "race",
    "birthdate",
    "party",
    "areacode",
    "phone",
    "email",
    "voter_ID",
    "exemption_requested",  # bool
    "registration_date",
    "active",  # bool
]

_RACE = {
    "1": "American Indian or Alaskan Native",
    "2": "Asian or Pacific Islander",
    "3": "BL",  # using BL so searches for people named "Black" will work
    "4": "Hispanic",
    "5": "WH",  # using WH so searches for people named "White" will work
    "6": "Other",
    "7": "Multiracial",
    "9": "Unknown",  # yes, there is no code 8
}
_PARTY = {  # Codes for parties registered in Florida as of June 2025
    "AMF": "America First",  # new in June 2025
    "ASP": "American Solidarity",  # effective July 2024
    "BPP": "Boricua",
    "CPP": "Coalition With a Purpose",
    "CPF": "Constitution",
    "CSV": "Conservative",
    "DEM": "Democratic",
    "ECO": "Ecology",
    "FFP": "Florida Forward",
    "GRE": "Green",
    "IND": "Independent",
    "JEF": "Jeffersonian",
    "LPF": "Libertarian",
    "MGT": "MGTOW",  # Men Going Their Own Way, new in 2025
    "NAT": "Florida Natural Law",
    "NPA": "no party",
    "PSL": "Socialism and Liberation",
    "RFM": "Reform",
    "REP": "Republican",
    "UPF": "Unity",
    "PEO": "People’s",
    # deprecated abbreviations, kept for parsing older files
    "REF": "Reform",
    "AIP": "American's",
    "FPP": "Pirate",
    "FSW": "Socialist Workers",
    "IDP": "Independence Party",
    "INT": "Independent Party (deprecated)",
    "JPF": "Justice",
    "NO PARTY": "no party (deprecated)",
    "NP": "no party (deprecated)",
    "PEACE & FREEDOM": "Peace & Freedom",
    "PFP": "Peace & Freedom",
    "TPF": "Tea Party",
}

FL_COUNTIES = {
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
    "WAS": "Washington",
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
    "BRO": "Broward",
    "CLL": "Collier",
    "DAD": "Miami-Dade",
    "GLA": "Glades",
    "HEN": "Hendry",
    "MON": "Monroe",
    "MRT": "Martin",
    "PAL": "PalmBeach",
}

SUPPRESS_MAP = {"N": "false", "Y": "true"}

ACTIVE_MAP = {"ACT": "true", "INA": "false"}

"""
2023: columns to add: 

7: Requested public records exemption (N or Y)
23: Registration Date
29: voter status (ACT or INA – for active and inactive) 


We want to harvest these columns for the database
3: last name,
5: first name
6: middle name
4: name suffix
8: address 1
9: address 2 (e.g. apt #)
10: city
12: zipcode
20: gender
21: race
22: dob
24: party
35: area code
36: phone
38: email
2: voter ID
7: Requested public records exemption (N or Y)
23: Registration Date
29: voter status (ACT or INA – for active and inactive) 

We use the COLUMNS value to slice the CSV, using csvkit
"""
COLUMNS = "3,5,6,4,8,9,10,12,20,21,22,24,35,36,38,2,7,23,29"


def stage_local_files(filename, slug):
    """Handle local file-copying operations."""
    tempfile = f"{TEMP}/{slug}_temp.csv"
    prepfile = f"{PREPBASE}/{slug}_prep.csv"
    subprocess.run(f"cp HEADER.txt {tempfile}", shell=True)
    subprocess.run(f"cat {RAWBASE}/{filename} >> {tempfile}", shell=True)
    subprocess.run(
        f"csvcut -t -c {COLUMNS} {tempfile} > {prepfile}", shell=True
    )
    return prepfile


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
    with open(prepfile, "r") as prepped_file:
        biglist = []  # a list of row lists
        reader = csv.DictReader(prepped_file)
        header = reader.fieldnames
        for row in reader:
            race = _RACE.get(row.get("race"), "")
            party = _PARTY.get(row.get("party"), "OTHER")
            suppress = SUPPRESS_MAP.get(row.get("suppress"), "false")
            registered = row.get("RegDate", "")
            active = ACTIVE_MAP.get(row.get("VoterStatus"), "true")
            biglist.append(
                [
                    row["lname"].strip(),
                    row["fname"].strip(),
                    row["mname"].strip(),
                    row["suffix"].strip(),
                    " ".join(
                        row["addr1"].split()
                    ),  # clean interior white spaces
                    row["addr2"].strip(),
                    row["city"].strip(),
                    row["zip"].strip(),
                    row["gender"].strip(),
                    race,
                    row["birthdate"].strip(),
                    party,
                    row["areacode"].strip(),
                    row["phone"].strip(),
                    row["email"].strip(),
                    suppress,
                    registered,
                    active,
                    row["voter_ID"].strip(),
                ]
            )
        biglist = sorted(biglist)  # sorts list of row lists by last name
        with open(loadfile, "w") as load_file:
            writer = csv.writer(load_file)
            writer.writerow(header)
            for entry in biglist:
                writer.writerow(entry)
    print(
        "{} ready for loading; prepping took {}".format(
            filename, (datetime.datetime.now() - prepstart)
        )
    )


def no_dotfiles(path):
    """Eliminate dot-files from consideration."""
    for local_file in os.listdir(path):
        if not local_file.startswith("."):
            yield local_file


def prep_files():
    """Cycle through entries and prep them."""
    prep_directories()  # make sure prep directories exist
    voter_files = sorted(no_dotfiles(RAWBASE))
    valid_files = [f for f in voter_files if f[:3] in FL_COUNTIES]
    print(f"Prepping {len(valid_files)} county voter files")
    for i, each in enumerate(valid_files):
        slug = each[:3]
        print(f"{i + 1}: Prepping voter data for {FL_COUNTIES.get(slug)}")
        prep(each)


def load_to_postgres():
    db = get_postgres_db_name()
    if db not in subprocess.getoutput("psql -l"):
        subprocess.run("createdb {}".format(db), shell=True)
        subprocess.run(
            "psql {} -f create_voter_tables.sql".format(db), shell=True
        )
    for each in no_dotfiles(LOADBASE):
        slug = each[:3]
        if slug in FL_COUNTIES:
            load_county_to_postgres(db, each, slug)
    subprocess.run("psql {} -f index_voter_tables.sql".format(db), shell=True)
    print(f"Created database {db}\n Now purging directories ...")
    purge_directories()


def load_county_to_postgres(db, file_name, slug):
    set_county_command = (
        'echo "ALTER TABLE voters_voter ALTER COLUMN county_slug '
        "SET DEFAULT '{}';\" | psql {}".format(slug, db)
    )
    subprocess.run(set_county_command, shell=True)
    load_county_command = (
        'echo "COPY voters_voter (lname, fname, mname, suffix, addr1, addr2, '
        "city, zipcode, gender, race, birthdate, party, areacode, phone, email, "
        "exemption_requested, registration_date, active, voter_id) "
        f"FROM '{LOADBASE}/{slug}.csv' CSV HEADER;\" | psql {db}"
    )
    subprocess.run(load_county_command, shell=True)


def export_county(countyfile):
    """Export one county CSV file to Panda."""
    put_data = {"objects": []}
    putstart = datetime.datetime.now()
    putcount = 0
    slug = countyfile[:3]
    if slug not in FL_COUNTIES.keys():
        print("'{}' isn't a standard Florida county voter slug".format(slug))
        return putcount
    else:
        county = FL_COUNTIES[slug]
        name = "{} voter registration {}".format(county, YEAR)
        dataset_slug = slugify(name)
        dataset_url = "{}/{}/".format(PANDA_DATASET_BASE, dataset_slug)
        data_url = "{}data/".format(dataset_url)
        # initialize new dataset
        dataset = {
            "name": name,
            "description": "Data from {}".format(VOTER_DATA_DATE),
            "categories": [
                "/api/1.0/category/all-dob/",
                "/api/1.0/category/voters/",
            ],
        }
        panda_put(
            dataset_url,
            json.dumps(dataset),
            params={"columns": ",".join(VOTER_COLUMNS)},
        )
        with open("{}/{}".format(LOADBASE, countyfile), "r") as cfile:
            reader = csv.DictReader(cfile)
            for row in reader:
                put_data["objects"].append(
                    {
                        "external_id": (row["voter_ID"]),
                        "data": [row[key] for key in VOTER_COLUMNS],
                    }
                )
                if len(put_data["objects"]) % 500 == 0:
                    print("500 processed")
                if len(put_data["objects"]) == 1000:
                    putcount += 1000
                    print("Updating {} rows".format(len(put_data["objects"])))
                    panda_put(data_url, json.dumps(put_data), params={})
                    put_data["objects"] = []
                if putcount % 10000 == 0:
                    print("loaded so far: {}".format(putcount))
        if put_data["objects"]:
            print("Updating {} rows".format(len(put_data["objects"])))
            panda_put(data_url, json.dumps(put_data), params={})
            putcount += len(put_data["objects"])
            put_data["objects"] = []
        print(
            "pushed {} rows to panda dataset {}; process took {}".format(
                putcount, name, (datetime.datetime.now() - putstart)
            )
        )
        return putcount


def export_to_panda():
    """Export any prepped county files in LOADBASE directory to PANDA."""
    for countyfile in no_dotfiles(LOADBASE):
        export_county(countyfile)


if __name__ == "__main__":
    print(
        f"{SCRIPT_NAME} reporting for duty, "
        f"with voter data date of {VOTER_DATA_DATE}"
    )
    if len(sys.argv) == 2:
        if sys.argv[1] == "prep_files":
            prep_files()
        elif sys.argv[1] == "load_to_postgres":
            load_to_postgres()
        elif sys.argv[1] == "export_to_panda":
            export_to_panda()
        elif sys.argv[1] == "purge":
            purge_directories()
        else:
            voter_file = sys.argv[1]
            print(
                "Prepping voter data for {} County".format(
                    FL_COUNTIES.get(voter_file[:3])
                )
            )
            prep(voter_file)
    else:
        print(
            "Please provide either a single voter file name to process, "
            "or one of these arguments: \n"
            "• prep_files (to prep any county files in /VoterDetail)\n"
            "• load_to_postgres (to create and load a database)\n"
            "• purge (to clear voter files from prep directories)\n"
            "• export_to_panda (to export any prepped county files to PANDA)."
        )
