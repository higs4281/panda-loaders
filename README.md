panda-voters
============

This script loads voter registration data into a <a href="http://pandaproject.net/">PANDA</a> instance via api.
The script is local to Florida voter data but could be adapted.

It starts with a directory of raw county voter registration files, preps them with csvkit and ships them to PANDA.

Caveat: this is a slow web-dependent process that loads about 250K rows an hour.
But it has these advantages, if you're not in a hurry:

     • it sidesteps memory issues that you can encounter in PANDA's loading GUI
     • it only uses PANDA index space
     • it results in a dataset with external_id values, which makes them editable via the api

Dependencies are csvkit and requests. It also uses Django's slugify function, but it's a convenience.

The script works on a local directory structure and assumes the data is from the current year.
It also uses a tab-delimited file called HEADER.txt that matches the raw files, which have no headers.
