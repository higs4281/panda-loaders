panda-voters
============

Script to load voter registration data into a PANDA instance via api.
The script is local to Florida voter data but could be adapted.

It loads a directory of raw florida county voter registration files via panda's api

Caveat: this is a slow web-dependent process, but has these advantages if you're not in a hurry:
     -- it sidesteps memory issues that you can encounter in the Panda loading gui
     -- it only uses Panda index space
     -- it results in a dataset with external_id values, which makes them editable via the api
     
Dependencies are csvkit and requests, for which you could substitute csv and urllib. It also uses Django's slugify function, but it's a convenience.

The script works on a local directory structure and assumes the data is from the current year.
It also uses a file called HEADER.txt that matches the raw files, which have no headers.
