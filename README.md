## panda-loaders

These scripts began as a way to load voter data into a <a href="http://pandaproject.net/">PANDA</a> instance via its API.

Along the way, the end-of-life of Python 2 appeared on the horizon, and the wonderful PANDA project has fallen a bit out of date.

We're not giving up on PANDA, but for now, the voter script has been updated to run in Python 3.6+ and to be able to create a Postgres database of voter data as an alternative to feeding the data to a PANDA instance.

If you use the Postgres option, you'll get an indexed database ready to be plugged into a Django project, if desired. This repo is not a working Django project, but a Django model that can be used to hook up the postgres voter db is included in `/voters/models.py`.

As before, the voters scripts is tailored to Florida but could be adapted. It require some things:
- County voter registration files, which in Florida are available from the state's Division of Elections.
- A local environment with Python3.6+ and Postgres10+ installed.

With voter files in place, you can run the voter script with one of three arguments:
1. `prep_files`, which processes raw county files and preps them for use in PANDA or in a database.
2. `load_postgres`, which will create a postgres database, load it with any prepped files it finds and then creates  indexes for database performance.
3. A file name for a raw county voter file, such as `BAY_20190312.txt`. In this case, the script will just prep that file for subsequent use.

### Local directories
The script uses some local directories as it transforms the raw data, and it assumes data come from the current year.

The assumed directory structure includes these folders:
```bash
VoterDetail
load
loaded
prep
temp
```

The scripts will look for a local environment variable, `PANDA_LOADERS_BASE_DIR`, to use as the base directory for the above folders, and will default to `/tmp` as the base directory if no environment var is found.

If you use the script to load data into a PANDA instance, the process uses PANDA's API, which is slow. But the API method has some advantages over PANDA's manual data upload process.
• It sidesteps memory issues that you can encounter in PANDA's loading GUI.
• It only uses PANDA index space, rather than index space + file storage space.
• It results in a dataset with external_id values, which makes rows editable via the API.
