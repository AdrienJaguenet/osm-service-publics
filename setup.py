#!/usr/bin/env python3

# This script fetches data files of the French establishment register (SIRENE), the administration contact data,
# and puts them into a PostgreSQL database.
import urllib.request
import tarfile
import zipfile
import glob
from getpass import getpass
import dotenv
import os
from utils import transfert_annuaire, transfert_siret

# This is always the latest file
URL_ANNUAIRE = 'https://lecomarquage.service-public.fr/donnees_locales_v4/all_latest.tar.bz2'
URL_SIRENE = 'https://files.data.gouv.fr/insee-sirene/StockEtablissement_utf8.zip'

def find_json_file():
	# Find the JSON file
	json_files = glob.glob('*-data.gouv_local.json')
	if len(json_files) == 1:
		return json_files[0]
	elif len(json_files) > 1:
		raise Error('Multiple json files found')
	else:
		raise Error('Could not find json file')

# Download and extract the files
def download_files():
	# Check whether all files are already downloaded
	if not os.path.isfile('all_latest.tar.bz2'):
		print("downloading all_latest.tar.bz2")
		urllib.request.urlretrieve(URL_ANNUAIRE, 'all_latest.tar.bz2')
	else:
		print('all_latest.tar.bz2 already downloaded')
	if not os.path.isfile('StockEtablissement_utf8.zip'):
		print("downloading StockEtablissement_utf8.zip")
		urllib.request.urlretrieve(URL_SIRENE, 'StockEtablissement_utf8.zip')
	else:
		print('StockEtablissement_utf8.zip already downloaded')

	print('Extracting files...')
	if len(glob.glob('*-data.gouv_local.json')) == 0:
		with tarfile.open('all_latest.tar.bz2') as tar:
			tar.extractall()
	else:
		print("administration contact data already extracted")
	if not os.path.isfile('StockEtablissement_utf8.csv'):
		with zipfile.ZipFile('StockEtablissement_utf8.zip') as zip:
			zip.extractall('.')
	else:
		print("SIRENE data already extracted")

def get_env_variables():
	# prompt the user for psql credentials
	dbname = input('Database name: ')
	user = input('User: ')
	password = getpass('Password: ')
	# store in a .env file
	# create the file if it doesn't exist
	if glob.glob('.env') == []:
		open('.env', 'a').close()
	dotenv_file = dotenv.find_dotenv()
	dotenv.set_key(dotenv_file, 'OSP_PSQL_DB', dbname)
	dotenv.set_key(dotenv_file, 'OSP_PSQL_USER', user)
	dotenv.set_key(dotenv_file, 'OSP_PSQL_PASSWORD', password)

get_env_variables()
download_files()
annuaire_json_file = find_json_file()
siret_csv_file = 'StockEtablissement_utf8.csv'

with open(annuaire_json_file) as f:
	transfert_annuaire.import_json(f)

with open(siret_csv_file) as f:
	transfert_siret.import_csv(f)
