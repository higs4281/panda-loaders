## panda-loaders

These scripts began as a way to load voter data into a <a href="http://pandaproject.net/">PANDA</a> instance via its API.

Along the way, the end-of-life of Python 2 appeared on the horizon, and the wonderful PANDA project has fallen a bit out of date.

We're not giving up on PANDA, but for now, the voter script has been updated to run in Python 3.6+ and to be able to create a Postgres database of voter data as an alternative to feeding the data to a PANDA instance.

If you use the Postgres option, you'll get an indexed database ready to be plugged into a Django project, if desired. This repo is not a working Django project, but a Django model that can be used to hook up the Postgres voter db is included in `/voters/models.py`.

## Getting started

First, install the Python requirements:

```bash
pip install -r requirements/base.txt
```

The voter script is tailored to Florida but could be adapted. It requires some source data and a date value:
- County voter registration files, which in Florida are available from the state's Division of Elections.
- A VOTER_DATA_DATE value in YYYY-MM-DD form, provided by an environment variable of that name or by manually editing the script's global variable. This is used to name the database, to provide default source_date in the voters_voter table and to provide a YEAR value for accessing the right data directory.

With raw voter files in place, you can run the voter script with `python load_county_voters` plus one of four arguments:
1. `[RAW FILE]`: You can pass in a file name for a raw county voter file, such as `BAY_20190312.txt`. This will prep a single raw file for loading to a database or to PANDA, if the raw file is in /VoterDetail/.
2. `prep_files`: This preps all raw county files found in /VoterDetail/ and makes them ready for export to a database or to PANDA.
3. `load_postgres`: After files are prepped, this will create, load and index a Postgres voter database. If you put all 67 Florida county files in the /VoterDetail/ directory and prep them, this will create a statewide database of Florida voters. 
4. `export_to_panda`: To export all prepped county files to a PANDA instance, creating one dataset per county.

### Local directories
The script uses some local directories as it transforms the raw data.

The assumed directory structure includes these folders:

Folder | Use
:----  | :--
VoterDetail | Put raw county voter files here
load | Prepped voter files
loaded | Voter files that were sent to PANDA
prep | Processing folder
temp | Processing folder

The scripts will look for a local environment variable, `PANDA_LOADERS_BASE_DIR`, to use as the base directory for the above folders, and will default to `/tmp` as the base directory if no environment var is found.

If you use the script to load data into a PANDA instance, the process uses PANDA's API, which is slow. But the API method has some advantages over PANDA's manual data upload process.
- It sidesteps memory issues that you can encounter in PANDA's loading GUI.
- It only uses PANDA index space, rather than index space + file storage space.
- It results in a dataset with external_id values, which makes rows editable via the API.
