## panda-voters

These scripts began as a way to load voter data into a <a href="http://pandaproject.net/">PANDA</a> instance via its API.

Along the way, the end life of Python 2 appeared on the horizon, and the wonderful PANDA project has been falling out of date.

We're not giving up on PANDA, but for the time being, these scripts have been updated to run in Python 3.6+ and to be able to create and populate a Postgres database with voter data rather than just feed it to a PANDA instance.

If you use the Postgres option, you'll get a an indexed database ready to be plugged into a Django project, if desired. A Django model that works with the produced postgres database is included in `/voters/models.py`.

As before, the voters scripts require some things:
- County voter registration files, which are available from the Florida Division of Elections.
- A local environment with Python3.6+ and Postgres10+ installed and running.

With voter files in place, you can run the scripts with one of three arguments:
1. `prep_files`, which processes raw county files and preps them for use in PANDA or in a database.
2. `load_postgres`, which will create a postgres database, load it with any prepped files it finds and indexes the database.
3. A file name for raw county voter file, such as `BAY_20190312.txt`. In This case, the script will just prep that file for subsequent use.

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

If you use the script to load data into a PANDA instance, the process uses PANDA's API, which is slow but has some advantages over PANDA's manual data upload process.
• It sidesteps memory issues that you can encounter in PANDA's loading GUI
• It only uses PANDA index space, rather than index space + file storage space.
• It results in a dataset with external_id values, which makes rows editable via the API
