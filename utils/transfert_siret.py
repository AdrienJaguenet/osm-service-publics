#!/usr/bin/env python3

# This script converts the SIRET CSV file to PostgreSQL

# First argument is the CSV file name

import csv
import psycopg2
import psycopg2.extras
from . import psql

def import_csv(f):
	print("Importing CSV file... this can take a while")
	reader = csv.DictReader(f)
	conn = psql.connect_to_db()
	cur = conn.cursor()
	cur.execute("DROP TABLE IF EXISTS siret;")
	# Create the table
	cur.execute("CREATE TABLE siret (%s)" % ','.join(['%s varchar' % x for x in reader.fieldnames]))
	cur.execute("ALTER TABLE siret ADD PRIMARY KEY (siret);")
	cur.execute("CREATE INDEX commune_idx ON siret (codeCommuneEtablissement);")
	cur.execute("CREATE INDEX siren_idx ON siret (siren);")
	cur.execute("CREATE INDEX ape_idx ON siret (activitePrincipaleEtablissement);")
	next(f)
	cur.copy_expert("COPY siret FROM STDIN WITH (FORMAT CSV)", f)
	conn.commit()
	cur.close()
	conn.close()
	print("CSV file successfully imported")

	