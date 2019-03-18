## panda-voters

These scripts began as a way to load voter data into a <a href="http://pandaproject.net/">PANDA</a> instance via its API.

Along the way, the end life of Python 2 appeared on the horizon, and the wonderful PANDA project has been falling out of date.

We're not giving up on PANDA, but for the time being, these scripts have been updated to run in Python 3.6+ and to be able to create and populate a Postgres database with voter data rather than just feed it to a PANDA instance.

If you use the Postgres option, you'll get a an indexed database ready to be plugged into a Django project, if desired. A Django model that works with the produced postgres database is included in `/voters/models.py`.

As before, the voters scripts require some things:
- County voter registration files, which are available from the Florida Division of Elections.
- A local environment with Python3.6+ and Postgres10+ installed and running.

If you are still using PANDA, the processes here use the PANDA API, which is slow but has advantages.
    • It sidesteps memory issues that you can encounter in PANDA's loading GUI
    • It only uses PANDA index space, rather than index space + file storage space.
    • It results in a dataset with external_id values, which makes rows editable via the API

The script works on a local directory structure and assumes the data is from the current year.
It also uses a tab-delimited file called HEADER.txt that matches the raw files, which have no headers.

The assumed directory structure includes these folders:
```bash
VoterDetail
load
loaded
prep
temp
```

The scripts will look for a local environment variable, `PANDA_LOADERS_BASE_DIR`, to use as the base directory for the above folders, and will default to `/tmp` if no var is found.
