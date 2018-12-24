import boto3
import zipfile
import json
import datetime
import pprint
from errors import MalformedTableData
from css import stylesheet

boto3.setup_default_session(region_name="ap-southeast-2")
ddb = boto3.resource("dynamodb")

pp = pprint.PrettyPrinter(indent=4)

DATE_NOW = datetime.datetime.utcnow().isoformat()

def create_delete_record(key_fields, record):
	"""
	Makes a delete record containing just IDs
	"""
	dict = {}
	for key in key_fields:
		dict.update({key: record[key]})
	return dict
		
def get_nested_key_from_dict(dict, keys):
	"""
	Gets a value from a dict under nested keys
	"""
	val = dict
	for key in keys:
		val = val[key]
	return val
		
def check_for_nested_key_in_dict(in_dict, keys):
	"""
	Checks if dictionary contains keys
	"""
	dict = in_dict
	for key in keys:
		if key in dict:
			dict = dict[key]
		else:
			return False
	return True

def add_record_to_dict(dict, keys, record):
	"""
	Adds nested keys for a record and then adds data under the keys
	"""
	if len(keys) > 1:
		if not keys[0] in dict:
			dict[keys[0]] = {}
		add_record_to_dict(dict[keys[0]], keys[1:], record)
	else:
		dict[keys[0]] = record
	
def add_meta_data_to_record(record, file, action):
	"""
	Adds _meta field to record
	"""
	record.update({
		"_meta": {
			"ref_file": file,
			"action": action,
			"timestamp": DATE_NOW
		}
	})

def update_record_values(old, new, key_fields):
	"""
	Reads values from new and updates old using them
	"""
	# get keys in new excluding key fields
	new_keys = [key for key in new.keys() if key not in key_fields]
	# replace keys in old dict with values from new dict
	for new_key in new_keys:
		#print "replacing {nk}".format(nk=new_key)
		old.update({
			new_key: new[new_key]
		})

def expand_special_values(d):
	"""
	Recurses through dict and replaces special values
	
	Only one implemented so far is %NOW%
	"""
	# check if d is a dict
	if isinstance(d, dict):
		# loop through keys in dict, excluding special keys: _meta and _schema
		for key in [key for key in d.keys() if key not in ["_meta", "_schema"]]:
			#print "working on key {k}".format(k = key)
			d.update({
				key: expand_special_values(d[key])
			})
		return d
	else:
		# if d is not a dict we must be at a leaf, so check if it is a special value to overwrite
		if d == "%NOW%":
			return DATE_NOW
		else:
			# not a special value
			return d
		
def validate_and_process(data):
	"""
	Takes raw data and validates and processes for updates to dynamodb
	"""
	tables = {}
	for table in data:
		table_name = ""
		table_keys = []
		raw_data = data[table]
		keys = sorted(raw_data.keys())
		if len(keys) >= 1:
			# check we have a schema and it is valid
			if keys[0] == "000_schema.json":
				schema = raw_data[keys[0]]
				# get table name and keys from record
				if "table" in schema and "keys" in schema:
					table_name = schema["table"]
					table_keys = schema["keys"]
					if len(table_keys) == 0:
						raise MalformedTableData("Keys attribute in schema is length 0 for table {tn}, expecting at least one element".format(tn=table))
				else:
					# schema file is incomplete
					raise MalformedTableData("Schema file for {tn} does not contain table name or keys attribute".format(tn=table))
			else:
				# first element is not a schema definition
				raise MalformedTableData("000_schema.json is missing for this table: {tn}".format(tn=table))
			
			# init empty table
			tables[table_name] = {}
			tables[table_name]["_schema"] = {
				"table": table_name,
				"keys": table_keys
			}
			
			# loop through data records
			for key in keys[1:]:
				record = raw_data[key]
				if "action" in record and "data" in record:
					# check keys are specified in data
					if not all(key in record["data"] for key in table_keys):
						raise MalformedTableData("One or more key fields are missing in record file {rec} for table {tn}".format(rec=key, tn=table))
					key_values = []
					for k in table_keys:
						key_values.append(record["data"][k])
					if record["action"] == "create":
						# this is a create
						# must be the first time the key is seen
						if check_for_nested_key_in_dict(tables[table_name], key_values):
							raise MalformedTableData("Check record file {rec} for table {tn} as action is 'create' but keys have been seen before".format(rec=key, tn=table))
						else:
							data = record["data"]
							add_meta_data_to_record(record = data, file = key, action = record["action"])
							add_record_to_dict(tables[table_name], key_values, data)
					elif record["action"] == "update":
						# this is an update
						# must have seen the key before
						if check_for_nested_key_in_dict(tables[table_name], key_values):
							old_data = get_nested_key_from_dict(dict = tables[table_name], keys = key_values)
							# make sure this combination of keys has not been deleted before
							if old_data["_meta"]["action"] == "delete":
								raise MalformedTableData("Check record file {rec} for table {tn} as action is update but record has previously been deleted".format(rec=key, tn=table))
							new_data = record["data"]
							add_meta_data_to_record(record = data, file = key, action = record["action"])
							update_record_values(old = old_data, new = new_data, key_fields = table_keys)
						else:
							raise MalformedTableData("Check record file {rec} for table {tn} as action is 'update' but keys have not been seen before".format(rec=key, tn=table))
					elif record["action"] == "delete":
						# this is a delete
						# must have seen the key before
						if check_for_nested_key_in_dict(tables[table_name], key_values):
							data = get_nested_key_from_dict(dict = tables[table_name], keys = key_values)
							delete_record = create_delete_record(key_fields = table_keys, record = data)
							add_meta_data_to_record(record = delete_record, file = key, action = record["action"])
							add_record_to_dict(tables[table_name], key_values, delete_record)
						else:
							raise MalformedTableData("Check record file {rec} for table {tn} as action is 'delete' but keys have not been seen before".format(rec=key, tn=table))
					else:
						raise MalformedTableData("Action value is unknown in record file {rec} for table {tn}".format(rec=key, tn=table))
				else:
					raise MalformedTableData("Record file {rec} for table {tn} does not contain action and data attribute".format(rec=key, tn=table))
	return tables

def ddb_get_item_consistent(keys, table_name):
	table = ddb.Table(table_name)
	item = table.get_item(
		Key = keys,
		ConsistentRead = True
	)
	if "Item" in item:
		return item["Item"]
	else:
		return None

def compare_single_record(new, current, key_fields):
	new_attributes = {}
	changed_attributes = {}
	removed_attributes = {}
	for new_key in [key for key in new.keys() if key[0:1] != "_" and key not in key_fields]:
		if new_key in current:
			# check if blank in new
			if new[new_key] == "":
				removed_attributes.update({
					new_key: ""
				})
			else:
				if current[new_key] != new[new_key]:
					changed_attributes.update({
						new_key: {
							"current": current[new_key],
							"new": new[new_key]
						}
					})
		else:
			new_attributes.update({
				new_key: new[new_key]
			})
	return {
		"new": new_attributes,
		"changed": changed_attributes,
		"removed": removed_attributes
	}
	
def create_change_report_entries(data, schema):
	"""
	Creates the table rows for a given change entry
	
	Each row has the following data:
	 - ID columns
	 - requested action
	 - action that will be taken
	 - state of current row in ddb
	 - data to be created/updated
	"""
	#pp.pprint(data)
	if "_compare_result" in data:
		html = "<tr>"
		for key_field in schema["keys"]:
			html += "<td>{col}</td>".format(col=data[key_field])
		html += "<td><p class=\"label {action}\">{action}</p></td>".format(action=data["_meta"]["action"])
		html += "<td><p class=\"label {action}\">{action}</p></td>".format(action=data["_compare_result"]["action"])
		html += "<td><p class=\"label\">{reason}</p></td>".format(reason=data["_compare_result"]["state"])
		if data["_compare_result"]["action"] == "create":
			# need to show all the fields
			sub_table = "<table class=\"ResultsTable\">"
			sub_table += "<tr><th>Field</th><th>Value</th></tr>"
			for k in [key for key in data.keys() if key[0:1] != "_"]:
				sub_table += "<tr><td>{col}</td>".format(col = k)
				if isinstance(data[k], (list, dict)):
					sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data[k]))
				else:
					sub_table += "<td>{val}</td>".format(val=data[k])
				sub_table += "</tr>"
			sub_table += "</table>"
			html += "<td>{sub}</td>".format(sub = sub_table)
		elif data["_compare_result"]["action"] == "update":
			# need to show changed fields
			html += "<td class=\"row_data\">"
			# new fields
			if len(data["_compare_result"]["delta"]["new"]) == 0:
				html += "<p>No new fields</p>"
			else:
				sub_table = "<p>New Fields</p><table class=\"ResultsTable\">"
				sub_table += "<tr><th>Field</th><th>Value</th></tr>"
				for k in data["_compare_result"]["delta"]["new"]:
					sub_table += "<tr><td>{col}</td>".format(col = k)
					if isinstance(data["_compare_result"]["delta"]["new"][k], (list, dict)):
						sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data["_compare_result"]["delta"]["new"][k]))
					else:
						sub_table += "<td>{val}</td>".format(val=data["_compare_result"]["delta"]["new"][k])
					sub_table += "</tr>"
				sub_table += "</table>"
				html += sub_table
			# changed fields
			if len(data["_compare_result"]["delta"]["changed"]) == 0:
				html += "<p>No changed fields</p>"
			else:
				sub_table = "<p>Changed Fields</p><table class=\"ResultsTable\">"
				sub_table += "<tr><th>Field</th><th>Current Value</th><th>New Value</th></tr>"
				for k in data["_compare_result"]["delta"]["changed"]:
					sub_table += "<tr><td>{col}</td>".format(col = k)
					if isinstance(data["_compare_result"]["delta"]["changed"][k]["current"], (list, dict)):
						sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data["_compare_result"]["delta"]["changed"][k]["current"]))
					else:
						sub_table += "<td>{val}</td>".format(val=data["_compare_result"]["delta"]["changed"][k]["current"])
					if isinstance(data["_compare_result"]["delta"]["changed"][k]["new"], (list, dict)):
						sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data["_compare_result"]["delta"]["changed"][k]["new"]))
					else:
						sub_table += "<td>{val}</td>".format(val=data["_compare_result"]["delta"]["changed"][k]["new"])
					sub_table += "</tr>"
				sub_table += "</table>"
				html += sub_table
			# removed fields
			if len(data["_compare_result"]["delta"]["removed"]) == 0:
				html += "<p>No removed fields</p>"
			else:
				html += "<p>Removed Fields</p>"
				html += "<ul>"
				for k in data["_compare_result"]["delta"]["removed"]:
					html += "<li>{field}</li>".format(field=k)
				html += "</ul>"
			html += "</td>"
		elif data["_compare_result"]["action"] == "delete":
			# need to show no fields
			html += "<td>n/a</td>"
		elif data["_compare_result"]["action"] == "none":
			# need to show no fields
			html += "<td>n/a</td>"
		html += "</tr>"
		return [html]
	else:
		entries = []
		#for k in data:
		#	print k
		for k in [key for key in data.keys() if str(key)[0:1] != "_"]:
			#print k
			entries = entries + create_change_report_entries(
				data = data[k],
				schema = schema
			)
		return entries
	
def create_change_report(data, env_prefix):
	"""
	Writes a HTML report showing the changes that will be made
	"""
	html = "<html><head><title>Delta Report</title><style>{style}</style></head><body>".format(style=stylesheet)
	html += "<h1>DynamoDB Ref Data delta report</h1>"
	if env_prefix:
		html += "<h2>Environment: {env}</h2>".format(env=env_prefix)
	for table_key in data:
		table = data[table_key]
		schema = table["_schema"]
		html += "<h2>Table: {table}</h2>".format(table=schema["table"])
		html += "<table class=\"TableTable\"><tr>"
		for key_field in schema["keys"]:
			html += "<th class=\"fixed_width\">{key}</th>".format(key=key_field)
		html += "<th class=\"fixed_width\">Requested action</th>"
		html += "<th class=\"fixed_width\">Action which will be performed</th>"
		html += "<th class=\"fixed_width\">Reason</th>"
		html += "<th class=\"take_up_space\">Row data</th>"
		html += "</tr>"
		html += "".join(create_change_report_entries(table, schema))
		html += "</table>"
	html += "</body></html>"
	return html
		
def compare_to_dynamo(data, env_prefix, prev_keys, schema):
	"""
	Runs through table dict and compares to the data in dyanamo to confirm the actions that will be taken
	
	This is done via a DFS (depth first search)
	"""
	# check if this is a leaf
	if "_meta" in data:
		#print "\n***item"
		pp.pprint(prev_keys)
		pp.pprint(schema)
		pp.pprint(data)
		# create
		if data["_meta"]["action"] == "create":
			# need to check if this item exists in dynamodb
			item = ddb_get_item_consistent(
				keys = {k: v for (k, v) in data.iteritems() if k in schema["keys"]},
				table_name = "{env}_{name}".format(env=env_prefix, name=schema["table"])
			)
			if item:
				data.update({
					"_compare_result": {
						"state": "exists",
						"action": "none"
					}
				})
			else:
				data.update({
					"_compare_result": {
						"state": "does_not_exist",
						"action": "create"
					}
				})
		elif data["_meta"]["action"] == "update":
			# need to check if this item exists in dynamodb
			item = ddb_get_item_consistent(
				keys = {k: v for (k, v) in data.iteritems() if k in schema["keys"]},
				table_name = "{env}_{name}".format(env=env_prefix, name=schema["table"])
			)
			if item:
				delta = compare_single_record(
					new = data,
					current = item,
					key_fields = schema["keys"]
				)
				if len(delta["new"]) + len(delta["changed"]) + len(delta["removed"]) == 0:
					data.update({
						"_compare_result": {
							"state": "exists_no_changes",
							"action": "none",
							"delta": delta
						}
					})

				else:
					data.update({
						"_compare_result": {
							"state": "exists",
							"action": "update",
							"delta": delta
						}
					})
			else:
				data.update({
					"_compare_result": {
						"state": "does_not_exist",
						"action": "none"
					}
				})
		elif data["_meta"]["action"] == "delete":
			# need to check if this item exists in dynamodb
			item = ddb_get_item_consistent(
				keys = {k: v for (k, v) in data.iteritems() if k in schema["keys"]},
				table_name = "{env}_{name}".format(env=env_prefix, name=schema["table"])
			)
			if item:
				data.update({
					"_compare_result": {
						"state": "exists",
						"action": "delete"
					}
				})
			else:
				data.update({
					"_compare_result": {
						"state": "does_not_exist",
						"action": "none"
					}
				})
				
	else:
		# get the schema if we are at the root of the tree
		if "_schema" in data:
			schema = data["_schema"]
		# loop through the keys, searching again depth first
		for key in [key for key in data.keys() if key not in ["_schema"]]:
			compare_to_dynamo(
				data = data[key], 
				env_prefix = env_prefix, 
				prev_keys = prev_keys + [key],
				schema = schema
			)
	
def read_zip_file(zip_file):
	"""
	Reads a zip file and outputs a dictionary of reference data to be processed
	"""
	data = {}
	file = zipfile.ZipFile(zip_file, "r")
	for file_name in file.namelist():
		file_name_parts = file_name.split("/")
		if file_name_parts[1] == "":
			data.update({
				file_name_parts[0]: {}
			})
		else:
			if file_name_parts[0] not in data:
				data.update({
					file_name_parts[0]: {}
				})
			json_data = json.loads(file.read(file_name))
			data[file_name_parts[0]][file_name_parts[1]] = json_data
	file.close()
	return data	

if __name__ == "__main__":
	dict_valid_create_nested_key = {
		"test": {
			"_schema": {
				"table": "test",
				"keys": ["id1", "id2"]
			},
			1: {
				2: {
					"id1": 1,
					"id2": 2,
					"val1": "",
					"val2": 100,
					"val3": True,
					"val4": {
						"t1": "hello",
						"t2": "hello2"
					},
					"_meta": {
						"action": "update",
						"ref_file": "001_create.json",
						"timestamp": DATE_NOW
					}
				},
				3: {
					"id1": 1,
					"id2": 3,
					"val1": "test",
					"_meta": {
						"action": "create",
						"ref_file": "002_create.json",
						"timestamp": DATE_NOW
					}
				},
				4: {
					"id1": 1,
					"id2": 4,
					"_meta": {
						"action": "delete",
						"ref_file": "002_create.json",
						"timestamp": DATE_NOW
					}
				}
			}
		}
	}
	compare_to_dynamo(
		data = dict_valid_create_nested_key["test"],
		env_prefix = "dev",
		prev_keys = [],
		schema = {}
	)
	#pp.pprint(dict_valid_create_nested_key)
	report = create_change_report(
		data = dict_valid_create_nested_key,
		env_prefix = "dev"
	)
	