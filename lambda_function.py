import boto3
import zipfile
import json
import datetime
import pprint
from errors import MalformedTableData

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
	#pp.pprint(tables)
	return tables

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

#if __name__ == "__main__":
#	test()