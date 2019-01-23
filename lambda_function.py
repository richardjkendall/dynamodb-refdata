from boto3.session import Session
import boto3
import botocore
import zipfile
import tempfile
import shutil
import os
import json
import datetime
import StringIO
import sys
import traceback
from pprint import pprint
from errors import MalformedTableData, ProcessError
from css import stylesheet
from decimal_encoder import DecimalEncoder

boto3.setup_default_session(region_name="ap-southeast-2")

ddb = boto3.resource("dynamodb")
ddb_c = boto3.client("dynamodb")
code_pipeline = boto3.client("codepipeline")
sns = boto3.client("sns")

DATE_NOW = datetime.datetime.utcnow().isoformat()

def mark_cp_job_success(message, job):
	"""
	Marks a codepipeline job as successful
	"""
	print message
	code_pipeline.put_job_success_result(jobId=job)

def mark_cp_job_failed(message, job):
	"""
	Marks a codepipeline job as failed
	"""
	print message
	code_pipeline.put_job_failure_result(
		jobId = job, 
		failureDetails = {
			"message": message, 
			"type": "JobFailed"
		}
	)

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
		old.update({
			new_key: new[new_key]
		})

# need to deal with lists...
def expand_special_values(d):
	"""
	Recurses through dict and replaces special values
	
	Only one implemented so far is %NOW%
	"""
	# check if d is a dict
	if isinstance(d, dict):
		# loop through keys in dict, excluding special keys: _meta and _schema
		for key in [key for key in d.keys() if key not in ["_meta", "_schema"]]:
			d.update({
				key: expand_special_values(d[key])
			})
		return d
	elif isinstance(d, list):
		# loop through the entries in the list
		return list(map(lambda x: expand_special_values(x), d))
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
							add_meta_data_to_record(record = new_data, file = key, action = record["action"])
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
	tables = expand_special_values(tables)
	return tables

def ddb_get_item_consistent(keys, table_name):
	"""
	Performs a consistent read on table_name for keys
	"""
	table = ddb.Table(table_name)
	item = table.get_item(
		Key = keys,
		ConsistentRead = True
	)
	if "Item" in item:
		return item["Item"]
	else:
		return None

def ddb_create_item(keys, data, table_name):
	"""
	Writes data to table_name 
	"""
	table = ddb.Table(table_name)
	data_to_write = {k :v for (k, v) in data.iteritems() if k != "_compare_result"}
	condition_expression = ""
	key_count = 0
	for key in keys:
		if key_count == 0:
			condition_expression = "attribute_not_exists({key})".format(key=key)
		else:
			condition_expression += " AND attribute_not_exists({key})".format(key=key)
	try:
		table.put_item(
			ConditionExpression=condition_expression,
			Item=data_to_write
		)
		return True
	except ddb_c.exceptions.ConditionalCheckFailedException:
		traceback.print_tb(sys.exc_info()[2])
		return False

def ddb_delete_item(keys, table_name):
	"""
	Deletes an item from table_name using keys
	"""
	table = ddb.Table(table_name)
	table.delete_item(
		Key = keys
	)

def ddb_update_item(keys, delta, meta, table_name):
	"""
	Updates record with keys in table_name using delta
	"""
	table = ddb.Table(table_name)
	update_map = {}
	for k in delta["new"]:
		update_map.update({
			k: {
				"Value": delta["new"][k],
				"Action": "PUT"
			}
		})
	for k in delta["changed"]:
		update_map.update({
			k: {
				"Value": delta["changed"][k]["new"],
				"Action": "PUT"
			}
		})
	for k in delta["removed"]:
		update_map.update({
			k: {
				"Action": "DELETE"
			}
		})
	update_map.update({
		"_meta": {
			"Value": meta,
			"Action": "PUT"
		}
	})
	table.update_item(
		Key = keys,
		AttributeUpdates = update_map
	)

def deep_field_compare(new, current):
	"""
	Checks if the field meets the rules to be different
	
	Returns true when the same, false when different
	
	Ignores dict changes if the only changes are the DT_CREATED and DT_MODIFIED fields
	"""
	if isinstance(new, dict):
		# check keys for difference
		different_fields = []
		for key in new:
			if key in current:
				# this is an existing field
				if not deep_field_compare(new[key], current[key]):
					# which has changed
					different_fields.append(key)
			else:
				# this is a new field
				different_fields.append(key)
		for key in [key for key in current if key not in new]:
			# this is a removed field
			different_fields.append(key)
		different_fields = list(map(lambda x: x.upper(), different_fields))
		if not set(different_fields).issubset(set(["DT_CREATED", "DT_MODIFIED"])):
			return False
		else:
			return True
	elif isinstance(new, list):
		# check list elements for difference
		different = True
		if len(new) == len(current):
			for i in range(0, len(new)):
				if not deep_field_compare(new[i], current[i]):
					different = False
			return different
		else:
			return False
	else:
		return new == current
	
def compare_single_record(new, current, key_fields):
	"""
	Compares a new and current version of a record looking for added, changed and removed fields
	
	Returns a dict with keys "new", "changed" and "removed"
	
	Ignores changes to fields named dt_created (special field for creation date) and dt_modified if no other fields have changed
	"""
	new_attributes = {}
	changed_attributes = {}
	removed_attributes = {}
	for new_key in [key for key in new.keys() if key[0:1] != "_" and key not in key_fields]:
		if new_key in current:
			# check if blank in new
			if new[new_key] == "":
				# yes, so we will remove this attributed
				removed_attributes.update({
					new_key: ""
				})
			else:
				if new_key.upper() != "DT_CREATED":
					# need to do a deep compare of these objects to avoid DT changes
					if not deep_field_compare(new[new_key], current[new_key]):
					#if current[new_key] != new[new_key]:
						changed_attributes.update({
							new_key: {
								"current": current[new_key],
								"new": new[new_key]
							}
						})
		else:
			if new[new_key] != "":
				new_attributes.update({
					new_key: new[new_key]
				})
	# if there is only one changed attribute and it is the DT_MODIFIED field then remove it from the list of changes
	if len(changed_attributes) == 1 and len(new_attributes) + len(removed_attributes) == 0:
		if list(changed_attributes.keys())[0].upper() == "DT_MODIFIED":
			changed_attributes = {}
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
					sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data[k], indent=2, sort_keys=True, cls=DecimalEncoder))
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
						sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data["_compare_result"]["delta"]["new"][k], indent=2, sort_keys=True, cls=DecimalEncoder))
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
						sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data["_compare_result"]["delta"]["changed"][k]["current"], indent=2, sort_keys=True, cls=DecimalEncoder))
					else:
						sub_table += "<td>{val}</td>".format(val=data["_compare_result"]["delta"]["changed"][k]["current"])
					if isinstance(data["_compare_result"]["delta"]["changed"][k]["new"], (list, dict)):
						sub_table += "<td><pre>{val}</pre></td>".format(val=json.dumps(data["_compare_result"]["delta"]["changed"][k]["new"], indent=2, sort_keys=True, cls=DecimalEncoder))
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
		for k in [key for key in data.keys() if str(key)[0:1] != "_"]:
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

def apply_to_dynamo(data, env_prefix, schema):
	"""
	Applies changes to dynamo DB table from local copy of data
	"""
	# check if this is a leaf with a compare result:
	if "_compare_result" in data:
		compare_result = data["_compare_result"]
		keys = {k: v for (k, v) in data.iteritems() if k in schema["keys"]}
		if compare_result["action"] == "create":
			result = ddb_create_item(
				keys = keys,
				data = data,
				table_name = "{env}_{name}".format(env=env_prefix, name=schema["table"])
			)
			if result:
				data.update({
					"_result": "completed"
				})
			else:
				data.update({
					"_result": "not_completed"
				})
		elif compare_result["action"] == "update":
			ddb_update_item(
				keys = keys,
				delta = compare_result["delta"],
				meta = data["_meta"],
				table_name = "{env}_{name}".format(env=env_prefix, name=schema["table"])
			)
		elif compare_result["action"] == "delete":
			ddb_delete_item(
				keys = keys,
				table_name = "{env}_{name}".format(env=env_prefix, name=schema["table"])
			)
			data.update({
				"_result": "completed"
			})
	else:
		if "_schema" in data:
			schema = data["_schema"]
		for key in [key for key in data.keys() if key not in ["_schema"]]:
			apply_to_dynamo(
				data = data[key],
				env_prefix = env_prefix,
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

def get_s3_client(creds = None):
	"""
	Gets an S3 client using creds if specified
	"""
	if creds:
		# need to create a new S3 client with the creds
		session = Session(
			aws_access_key_id = creds["accessKeyId"],
			aws_secret_access_key = creds["secretAccessKey"],
			aws_session_token = creds["sessionToken"]
		)
		client = session.client("s3", config=botocore.client.Config(
			signature_version="s3v4"
		))
		return client
	else:
		return boto3.client("s3")
	
def get_file_from_s3(bucket, path, creds = None):
	"""
	Downloads the file at path from S3 bucket to a new temp file.
	
	Returns the temp file path
	
	Uses creds if specified
	"""
	client = get_s3_client(creds)
	temp_dir = tempfile.mkdtemp()
	file_name = path.split("/").pop()
	download_loc = os.path.join(temp_dir, file_name)
	client.download_file(bucket, path, download_loc)
	return download_loc

def put_html_file_in_s3(bucket, path, html, creds = None):
	"""
	Puts HTML report file in S3 at path
	
	Uses creds if specified
	"""
	client = get_s3_client(creds)
	data = StringIO.StringIO(html)
	client.put_object(
		Bucket=bucket,
		Key="{p}".format(p=path), 
		Body=data#,
		#ServerSideEncryption="aws:kms"
	)

def get_presigned_url_for_review(bucket, path, expires):
	"""
	Uses plain client to generate a presigned URL
	"""
	client = get_s3_client()
	url = client.generate_presigned_url(
		ClientMethod="get_object",
		Params={
			"Bucket": bucket,
			"Key": path
		},
		ExpiresIn=expires
	)
	return url

def read_folder(folder):
	"""
	Reads a folder and create data structure
	"""
	data = {}
	for dir in os.listdir(folder):
		# ignore folders which start with .
		if not dir[:1] == ".":
			data[dir] = {}
			for file in os.listdir("{r}/{d}".format(r=folder, d=dir)):
				json_file = "{r}/{d}/{f}".format(r=folder, d=dir, f=file)
				with open(json_file) as f:
					json_data = json.load(f)
					f.close()
				data[dir][file] = json_data
	return data
				
	
def local_run(folder, environment):
	"""
	Runs locally for testing, only does a compare, not a commit
	"""
	raw = read_folder(folder)
	#print(json.dumps(raw))
	tables = validate_and_process(raw)
	#print(json.dumps(tables))
	#pprint(tables)
	for table in tables:
		compare_to_dynamo(
			data = tables[table],
			env_prefix = environment,
			prev_keys = [],
			schema = {}
		)
	report = create_change_report(
		data = tables,
		env_prefix = environment
	)
	print report
	
def cp_event_handler(event, context):
	"""
	Gets event from codepipeline and uses the data to update DynamoDB data
	
	 - Gets S3 file as input
	 - reads it
	 - compares data to dynamo
	 - creates a report
	 - optionally it performs the changes
	"""
	job_id = event["CodePipeline.job"]["id"]
	success = False
	try:
		job_data = event["CodePipeline.job"]["data"]
		action = job_data["actionConfiguration"]["configuration"]
		s3creds = job_data["artifactCredentials"]
		input_artifact = job_data["inputArtifacts"][0]
		
		# need to get user parameters
		user_parameters = action["UserParameters"]
		parameters = {}
		for parameter in user_parameters.split(","):
			kvp = parameter.split("=")
			if len(kvp) != 2:
				raise ProcessError("This is an invalid parameter {p}".format(p = parameter))
			else:
				parameters.update({
					kvp[0]: kvp[1]
				})
		
		# check basic parameters are present
		if "mode" not in parameters:
			raise ProcessError("Mode not specified")
		if "env" not in parameters:
			raise ProcessError("Env not specified")
		
		# get S3 file
		temp_zip_file = get_file_from_s3(
			bucket = input_artifact["location"]["s3Location"]["bucketName"],
			path = input_artifact["location"]["s3Location"]["objectKey"],
			creds = s3creds
		)
		
		# read zip file
		raw = read_zip_file(temp_zip_file)
		
		# process the tables
		tables = validate_and_process(raw)
		
		# for each table we need to compare to dynamodb
		for table in tables:
			compare_to_dynamo(
				data = tables[table],
				env_prefix = parameters["env"],
				prev_keys = [],
				schema = {}
			)
		
		# if mode=report then produce the change report
		if parameters["mode"] == "report":
			# check mandatory parameters are present
			if "reportbucket" not in parameters:
				raise ProcessError("Report bucket not specified")
			if "topic" not in parameters:
				raise ProcessError("Topic not specified")
			
			# create report
			report = create_change_report(
				data = tables,
				env_prefix = parameters["env"]
			)
			# upload it to the reports bucket
			put_html_file_in_s3(
				bucket = parameters["reportbucket"],
				path = "{id}/report.html".format(id = job_id),
				html = report
			)
			# get URL for report
			url = get_presigned_url_for_review(
				bucket = parameters["reportbucket"],
				path = "{id}/report.html".format(id = job_id),
				expires = 600
			)
			# send sns message with URL for review
			sns.publish(
				TopicArn=parameters["topic"],
				Message="""
Please review this report and approve if it can be deployed.  You will have been sent a separate notification asking for that approval.

{url}
				""".format(url=url)
			)
			# tell CP we were successful
			success = True
			mark_cp_job_success(
				message = "Report is ready @ URL: {url}".format(url=url),
				job = job_id
			)
			
		# if the mode=commit then we need to make changes to dynamo DB
		elif parameters["mode"] == "commit":
			apply_to_dynamo(
				data = tables,
				env_prefix = parameters["env"],
				schema = {}
			)
			# tell CP we were successful
			success = True
			mark_cp_job_success(
				message = "Database changes have been made",
				job = job_id
			)
	except:
		traceback.print_tb(sys.exc_info()[2])
		success = True
		mark_cp_job_failed(
			message = "Unexpected err: {err}".format(err = sys.exc_info()[1]),
			job = job_id
		)
	finally:
		if not success:
			mark_cp_job_failed(
				message = "Hit catch all and failed",
				job = job_id
			)

def lambda_handler(event, context):
	"""
	Entry point for AWS lambda
	"""
	cp_event_handler(event, context)

if __name__ == "__main__":
	# entry point for local running
	local_run(
		folder=sys.argv[1],
		environment=sys.argv[2]
	)