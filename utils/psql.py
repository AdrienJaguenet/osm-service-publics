import psycopg2
import sys
import dotenv

def connect_to_db():
	# Load the environment file
	dotenv.load_dotenv()
	# Get the dbname and user from the environment file
	import os
	try:
		dbname = os.environ['OSP_PSQL_DB']
		user = os.environ['OSP_PSQL_USER']
		password = os.environ['OSP_PSQL_PASSWORD']
	except KeyError:
		print("Environment variables are missing. Please run setup.py first or source the environment file with the following variables:")
		print("\tOSP_PSQL_DB: the database name")
		print("\tOSP_PSQL_USER: the database user")
		print("\tOSP_PSQL_PASSWORD: the database password")
		sys.exit(1)
	# Open the database
	conn = psycopg2.connect("dbname=%s user=%s password=%s" % (dbname, user, password))
	return conn