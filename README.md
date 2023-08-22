# OSM Services publics

Data matching tool between Wikidata, OSM, SIRENE and the French public administration directory

## Requirements and dependencies

- Python3
- Local running PostgreSQL instance with a database you can access (any name is fine)
- The following Python packages are dependencies:
	- OSMPythonTools
	- psycopg2
	- pyperclip
	- python-dotenv
	- SPARQLWrapper
	- phonenumbers

## How to use

- Run `./setup.py` to download and import data from official sources (Warning: this will take a LONG time and the SIRET import shows no progress bar. It takes tens of minutes on my machine to import the 15 000 000 records of the gigabyte CSV file)
- Run and write scripts like `find-infos-mairie.py`