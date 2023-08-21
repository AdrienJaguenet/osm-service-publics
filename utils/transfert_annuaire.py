#!/usr/bin/env python3

# First argument is the JSON file

import json
import psycopg2
import psycopg2.extras
import re
from .base_annuaire import tables as TablesAnnuaire
from . import psql

SIREN_regex = re.compile(r'^\d{9}$')

# Return a shortened field if the user accepts, else close the program
def shorten_field(field, fieldname, max_length):
	if len(field) > max_length:
		field_shortened = field[:max_length]
		print('Shortened "%s" to "%s"' % (field, field_shortened))
		return field_shortened
	else:
		return field

def import_json(f):
	data = json.load(f)
	print("JSON file loaded.")
	# Open the database, and a connection
	conn = psql.connect_to_db()
	with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
		print("Creating tables...")
		for table in TablesAnnuaire.values():
			print("\t%s" % table.name)
			table.create_table(cur)
		conn.commit()
		print("Creating indices...")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_code_insee_commune_idx ON annuaire_ets (code_insee_commune);")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_siret_idx ON annuaire_ets (siret);")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_siren_idx ON annuaire_ets (siren);")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_type_organisme_idx ON annuaire_ets (type_organisme);")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_type_repertoire_idx ON annuaire_ets (type_repertoire);")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_category_idx ON annuaire_ets (categorie);")
		cur.execute("CREATE INDEX IF NOT EXISTS annuaire_itm_identifiant_idx ON annuaire_ets (itm_identifiant);")
		conn.commit()
		i = 0
		for item in data['service']:
			i += 1
			print("\rItem %d/%d                                                            " % (i, len(data['service'])), end='')
			for k, v in item.items():
				if type(v) is str:
					continue
			# insert the data
			# known errors in the file
			if item['siren'] != '' and SIREN_regex.match(item['siren']) is None:
				print("Error in SIREN: %s, item %s" % (item['siren'], item['id']))
				item['siren'] = item.get('siret', '')[:9]
				print("\tSIREN fixed to %s" % item['siren'])
			# fix siret (NIC can be duplicated)
			item['siret'] = shorten_field(item['siret'], 'siret', 14)
			try:
				TablesAnnuaire['annuaire_ets'].insert_item(cur, item)
			except psycopg2.Error as e:
				print("Error inserting %s: %s" % (item['id'], e))
				raise e
			for k, v in TablesAnnuaire.items():
				if k == 'annuaire_ets':
					continue
				if item[k] is None or k not in TablesAnnuaire:
					continue
				try:
					for subitem in item[k]:
						subitem['id'] = item['id']
						TablesAnnuaire[k].insert_item(cur, subitem)
				except psycopg2.Error as e:
					print("Error inserting %s, subtable %s: %s" % (item['id'], k, e))
					raise e

		conn.commit()
	conn.close()