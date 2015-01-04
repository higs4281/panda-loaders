panda-voters
============

These are sample scripts for loading data into a <a href="http://pandaproject.net/">PANDA</a> instance via its api.
The citations script just loads data from a csv into a new panda dataset.

The voters script is more involved and preps a directory of raw Florida voter registration data files before exporting them to Panda.
The raw files, which have three-letter county prefixes, are prepped with csvkit before export.

Caveat: this is a slow web-dependent process that loads about 250K rows an hour.
But it has these advantages, if you're not in a hurry:

     • it sidesteps memory issues that you can encounter in PANDA's loading GUI
     • it only uses PANDA index space
     • it results in a dataset with external_id values, which makes rows editable via the api

Dependencies are csvkit and requests. It also uses Django's slugify function.

The script works on a local directory structure and assumes the data is from the current year.
It also uses a tab-delimited file called HEADER.txt that matches the raw files, which have no headers.
