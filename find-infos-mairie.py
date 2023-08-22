#!/usr/bin/env python3

import sys
import pyperclip
import psycopg2
import psycopg2.extras
from SPARQLWrapper import SPARQLWrapper, JSON
from utils.conversions import *
import utils.psql as psql
from utils.base_annuaire import tables as TablesAnnuaire
from phonenumbers import parse as phoneparse, format_number as phoneformat, PhoneNumberFormat

def get_social_networks(osm_attributes, id):
	social_query = """
	SELECT * FROM reseau_social
	WHERE id=%s;
	"""
	cur.execute(social_query, (row['id'],))
	social_networks = cur.fetchall()
	for social_network in social_networks:
		osm_attributes["contact:%s" % social_network['custom_dico2'].lower()] = social_network['valeur']

def get_telephone(osm_attributes, id):
	results = TablesAnnuaire['telephone'].get_by_id(cur, id)
	for telephone in results:
		osm_attributes["contact:phone"] = phoneformat(phoneparse(telephone['valeur'], "FR"), PhoneNumberFormat.INTERNATIONAL)
		if telephone['description'] is not None:
			osm_attributes["note:contact:phone"] = telephone['description']

def get_horaires(osm_attributes, id):
	# Fetch the opening hours from the annuaire tables
	horaires_query = """
	SELECT * FROM plage_ouverture
	WHERE id=%s;
	""" 
	cur.execute(horaires_query, (row['id'],))
	horaires = cur.fetchall()
	horaires_liste = list()
	for horaire in horaires:
		day_1 = convert_day(horaire['nom_jour_debut'])
		day_2 = convert_day(horaire['nom_jour_fin'])
		plage_jours = day_1 + "-" + day_2 if day_2 is not None and day_1 != day_2 else day_1
		horaire_1 = format_horaire(horaire['valeur_heure_debut_1']) + "-" + format_horaire(horaire['valeur_heure_fin_1'])
		if horaire['valeur_heure_debut_2'] is not None:
			horaires_liste.append(plage_jours + " " + horaire_1 + "," + format_horaire(horaire['valeur_heure_debut_2']) + "-" + format_horaire(horaire['valeur_heure_fin_2']))
		else:
			horaires_liste.append(plage_jours + " " + horaire_1)
		if 'commentaire' in horaire and horaire['commentaire'] is not None:
			if 'note:opening_hours' not in osm_attributes:
				osm_attributes['note:opening_hours'] = [horaire['commentaire']]
			else:
				osm_attributes['note:opening_hours'].append(horaire['commentaire'])

	osm_attributes['opening_hours'] = ";".join(horaires_liste)

def get_websites(osm_attributes, id):
	# Fetch websites
	website_query = """
	SELECT * FROM site_internet WHERE id=%s;
	"""
	cur.execute(website_query, (row['id'],))
	websites = cur.fetchall()
	liste_sites = list()
	for website in websites:
		liste_sites.append(website['valeur'])
	if len(liste_sites) > 0:
		osm_attributes['website'] = ";".join(liste_sites)

# First argument is the postcode of the town

if len(sys.argv) < 2:
	print("Usage: csv_read.py <INSEE code> <name>")
	sys.exit(1)

insee_code = sys.argv[1]

# Open the database
conn = psql.connect_to_db()
with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
	siret_query = """
	SELECT * FROM siret
	INNER JOIN annuaire_ets ON siret.siret=annuaire_ets.siret
	WHERE
		siret.codeCommuneEtablissement=%s AND
		siret.etatAdministratifEtablissement='A' AND
		siret.activitePrincipaleEtablissement='84.11Z' AND
		siret.etablissementSiege='true' AND
		siret.enseigne1Etablissement='MAIRIE'
	;
	"""
	cur.execute(siret_query, (insee_code,))
	if cur.rowcount > 1:
		print("Found %d results" % cur.rowcount)
		sys.exit(1)
	row = cur.fetchone()
	if row is None:
		print("No result")
		sys.exit(1)

	osm_attributes = dict()
	# Fetch the wikidata object of the commune from the INSEE code
	sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
	sparql.setQuery("""\
		SELECT ?item ?name WHERE {
		 	?item wdt:P374 "%s".
		 	?item wdt:P1448 ?name.
		}
	""" % insee_code)
	sparql.setReturnFormat(JSON)
	try:
		results = sparql.query().convert()
		if (len(results["results"]["bindings"]) == 0):
			print("No wikidata result for %s" % insee_code)
			sys.exit(1)
		elif (len(results["results"]["bindings"]) > 1):
			print("Too many wikidata results for %s" % insee_code)
			sys.exit(1)
		wikidata = results["results"]["bindings"][0]["item"]["value"].split('/')[-1]
		osm_attributes["operator:wikidata"] = wikidata
	except:
		print("Error while fetching wikidata")
		sys.exit(1)

	city_name = results["results"]["bindings"][0]["name"]["value"]

	get_websites(osm_attributes, row['id'])
	get_horaires(osm_attributes, row['id'])
	get_telephone(osm_attributes, row['id'])
	get_social_networks(osm_attributes, row['id'])
	if 'commentaires_plage_ouverture' in row and row['commentaires_plage_ouverture'] is not None:
		if 'note:opening_hours' not in osm_attributes:
			osm_attributes['note:opening_hours'] = [row['commentaires_plage_ouverture']]
		else:
			osm_attributes['note:opening_hours'].append(row['commentaires_plage_ouverture'])

	# Contact attributes
	if row['adresse_courriel'] != None:
		osm_attributes["contact:email"] = ";".join(row['adresse_courriel'])

	osm_attributes["ref:FR:SIRET"] = row['siret']
	osm_attributes["addr:postcode"] = row['codepostaletablissement']
	osm_attributes["addr:city"] = city_name
	if row['numerovoieetablissement'] is not None:
		repetition = row.get('indicerepetitionetablissement', '') 
		osm_attributes["addr:housenumber"] = "%s %s" % (row['numerovoieetablissement'], repetition)
	for k,v in osm_attributes.items():
		print("%s=%s" % (k, v))
	pyperclip.copy("\n".join(["%s=%s" % (k, v) for k, v in osm_attributes.items()]))
conn.close()