import psycopg2

class Table:
	def __init__(self, name, fields):
		self.name = name
		self.fields = fields
		fields['id'] = 'UUID'
		self.columns = fields.keys()
		insert_query = """
			INSERT INTO %s (\n%s\n) VALUES (\n%s\n);"""
		self.insert_query = insert_query % (self.name, ',\n'.join(self.columns), ',\n'.join(["%s" for k in self.columns]))

	def create_table(self, cursor):
		cursor.execute("DROP TABLE IF EXISTS %s CASCADE;" % self.name)
		cursor.execute("""
			CREATE TABLE %s (
			%s,\n%s);
		 """ % (self.name, ',\n'.join(['%s %s' % (k, self.fields[k]) for k in self.columns]),
		 """ CONSTRAINT fk_id FOREIGN KEY (id) REFERENCES annuaire_ets(id)"""))
		# Create an index for the id field
		cursor.execute("CREATE INDEX IF NOT EXISTS %s_id_idx ON %s (id);" % (self.name, self.name))

	def insert_item(self, cursor, item):
		item = self.fix_item(item)
		values = [item.get(k, None) for k in self.columns]
		values = list(map(lambda x : None if x == '' or (type(x) is list and len(x) == 0) else x, values))
		try:
			cursor.execute(self.insert_query, values)
		except psycopg2.Error as e:
			print("Error -- table %s" % self.name)
			print(e)
			print('ITEM:\n\t', item)
			print('VALUES:\n\t', values)
			print('COLUMNS:\n\t', self.columns)
			print(cursor.mogrify(self.insert_query, values).decode('unicode_escape'))
			raise e

	def fix_item(self, item):
		if len(item.get('code_postal', [])) > 5:
			item['code_postal'] = None
		return item
	
	def get_by_id(self, cursor, id):
		cursor.execute("SELECT * FROM %s WHERE id=%%s;" % self.name, (id,))
		return cursor.fetchall()

class PivotTable(Table):
	def fix_item(self, item):
		if type(item['code_insee_commune']) is not list:
			item['code_insee_commune'] = [item['code_insee_commune']]
		return item

tables = {
	'annuaire_ets' : 
		Table('annuaire_ets', {
			"siret" : " VARCHAR(14)",
			"code_insee_commune" : " VARCHAR(5)",
			"copyright" : " TEXT",
			"siren" : " VARCHAR(9)",
			"ancien_code_pivot" : " TEXT",
			"partenaire" : " VARCHAR",
			"nom" : " VARCHAR",
			"itm_identifiant" : " VARCHAR",
			"date_modification" : " TIMESTAMP",
			"service_disponible" : " VARCHAR",
			"partenaire_identifiant" : " VARCHAR",
			"commentaire_plage_ouverture" : " TEXT",
			"categorie" : " VARCHAR",
			"version_type" : " VARCHAR",
			"type_repertoire" : " VARCHAR",
			"version_etat_modification" : " VARCHAR",
			"mission" : "TEXT",
			"version_source" : " VARCHAR",
			"type_organisme" : "VARCHAR",
			"information_complementaire" : " TEXT",
			"date_diffusion" : "TIMESTAMP",
			"telecopie" : "VARCHAR[]",
			"adresse_courriel" : "VARCHAR[]",
			"ancien_nom" : "VARCHAR[]",
			"ancien_identifiant" : "VARCHAR[]",
			"sve" : "VARCHAR[]",
			"formulaire_contact" : "VARCHAR[]",
	}),
	'plage_ouverture' : Table('plage_ouverture', {
		'nom_jour_debut' : 'VARCHAR',
		'nom_jour_fin' : 'VARCHAR',
		'valeur_heure_debut_1' : 'TIME',
		'valeur_heure_fin_1' : 'TIME',
		'valeur_heure_debut_2' : 'TIME',
		'valeur_heure_fin_2' : 'TIME',
		'commentaire' : 'TEXT',
	}),
	'site_internet' : Table('site_internet', {
		'libelle' : 'VARCHAR',
		'valeur' : 'VARCHAR',
	}),
	'reseau_social' : Table('reseau_social', {
		'valeur' : 'VARCHAR',
		'description' : 'TEXT',
		'custom_dico2' : 'VARCHAR',
	}),
	'texte_reference' : Table('texte_reference', {
		'libelle' : 'VARCHAR',
		'valeur' : 'VARCHAR',
	}),
	'organigramme' : Table('organigramme', {
		'libelle' : 'VARCHAR',
		'valeur' : 'VARCHAR',
	}),
	'pivot' : PivotTable('pivot', {
		'type_service_local' : 'VARCHAR',
		'code_insee_commune' : 'VARCHAR(5)[]'
	}),
	'annuaire' : Table('annuaire', {
		'libelle' : 'VARCHAR',
		'valeur' : 'VARCHAR',
	}),
	'tchat' : Table('tchat', {}),
	'hierarchie' : Table('hierarchie', {
		"type_hierarchie" : 'VARCHAR',
		# Some services are orphans, so we cannot use a foreign key constraint here
		"service" : "UUID",
	}),
	'telephone_accessible' : Table('telephone_accessible', {
		"valeur" : "VARCHAR",
		"description" : "TEXT",
		"custom_dico1" : "TEXT[]",
	}),
	'telephone' : Table('telephone', {
		"valeur" : "VARCHAR",
		"description" : "TEXT",
	}),
	'adresse' : Table('adresse', {
		"type_adresse" : "VARCHAR",
		"complement1" : "VARCHAR",
		"complement2" : "VARCHAR",
		"numero_voie" : "VARCHAR",
		"service_distribution" : "VARCHAR",
		"code_postal" : "VARCHAR(5)",
		"nom_commune" : "VARCHAR",
		"pays" : "VARCHAR",
		"continent" : "VARCHAR",
		"longitude" : "FLOAT",
		"latitude" : "FLOAT",
		"accessibilite" : "VARCHAR",
		"note_accessibilite" : "TEXT",
	}),
}

tables['annuaire_ets'].fields['id'] = 'UUID PRIMARY KEY'