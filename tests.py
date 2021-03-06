import unittest
import json
import zipfile
import tempfile
import shutil
import os
import pprint
from time import sleep

from lambda_function import validate_and_process, read_zip_file, expand_special_values, DATE_NOW, deep_field_compare
from errors import MalformedTableData

pp = pprint.PrettyPrinter(indent=4)

record_missing_data = """
{
	"action": "create"
}
"""

record_missing_action = """
{
	"data": {
		"id1": 1
	}
}
"""

invalid_action = """
{
	"action": "blah",
	"data": {
		"id1": 1,
		"id2": 2
	}
}
"""

delete_missing_key = """
{
	"action": "delete",
	"data": {
		"id1": 1
	}
}
"""

valid_delete = """
{
	"action": "delete",
	"data": {
		"id1": 1,
		"id2": 2
	}
}
"""

update_missing_key = """
{
	"action": "update",
	"data": {
		"id1": 1,
		"val1": 1,
		"val2": "testing2",
		"val3": true
	}
}
"""

valid_update = """
{
	"action": "update",
	"data": {
		"id1": 1,
		"id2": 2,
		"val1": 2,
		"val2": "testing2",
		"val3": false
	}
}
"""

valid_update_single_col = """
{
	"action": "update",
	"data": {
		"id1": 1,
		"id2": 2,
		"val1": 100
	}
}
"""

valid_create_dual_key_multi_field = """
{
	"action": "create",
	"data": {
		"id1": 1,
		"id2": 2,
		"val1": 1,
		"val2": "testing",
		"val3": true
	}
}
"""

valid_single_key_schema = """
{
	"table": "test",
	"keys": [
		"id1"
	]
}
"""

dict_single_key_schema = {
	"test": {
		"_schema": {
			"table": "test",
			"keys": ["id1"]
		}
	}
}

valid_create_single_key = """
{
	"action": "create",
	"data": {
		"id1": 1,
		"val1": "test"
	}
}
"""

dict_valid_create_single_key = {
	"test": {
		"_schema": dict_single_key_schema["test"]["_schema"],
		1: {
			"id1": 1,
			"val1": "test",
			"_meta": {
				"action": "create",
				"ref_file": "001_create.json",
				"timestamp": DATE_NOW
			}
		}
	}
}

create_missing_key = """
{
	"action": "create",
	"data": {
		"val1": "test"
	}
}
"""

valid_dual_key_schema = """
{
	"table": "test",
	"keys": [
		"id1",
		"id2"
	]
}
"""

dict_dual_key_schema = {
	"test": {
		"_schema": {
			"table": "test",
			"keys": ["id1", "id2"]
		}
	}
}

dict_valid_update = {
	"test": {
		"_schema": dict_dual_key_schema["test"]["_schema"],
		1: {
			2: {
				"id1": 1,
				"id2": 2,
				"val1": 2,
				"val2": "testing2",
				"val3": False,
				"_meta": {
					"action": "update",
					"ref_file": "002_update.json",
					"timestamp": DATE_NOW
				}
			}
		}
	}
}

dict_valid_update_single_col = {
	"test": {
		"_schema": dict_dual_key_schema["test"]["_schema"],
		1: {
			2: {
				"id1": 1,
				"id2": 2,
				"val1": 100,
				"val2": "testing",
				"val3": True,
				"_meta": {
					"action": "update",
					"ref_file": "002_update.json",
					"timestamp": DATE_NOW
				}
			}
		}
	}
}

dict_valid_delete = {
	"test": {
		"_schema": dict_dual_key_schema["test"]["_schema"],
		1: {
			2: {
				"id1": 1,
				"id2": 2,
				"_meta": {
					"action": "delete",
					"ref_file": "002_delete.json",
					"timestamp": DATE_NOW
				}
			}
		}
	}
}

valid_create_dual_key = """
{
	"action": "create",
	"data": {
		"id1": 1,
		"id2": 2,
		"val1": "test"
	}
}
"""

dict_valid_create_dual_key = {
	"test": {
		"_schema": dict_dual_key_schema["test"]["_schema"],
		1: {
			2: {
				"id1": 1,
				"id2": 2,
				"val1": "test",
				"_meta": {
					"action": "create",
					"ref_file": "001_create.json",
					"timestamp": DATE_NOW
				}
			}
		}
	}
}

valid_create_dual_nested_key = """
{
	"action": "create",
	"data": {
		"id1": 1,
		"id2": 3,
		"val1": "test"
	}
}
"""

dict_valid_create_nested_key = {
	"test": {
		"_schema": dict_dual_key_schema["test"]["_schema"],
		1: {
			2: {
				"id1": 1,
				"id2": 2,
				"val1": "test",
				"_meta": {
					"action": "create",
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
			}
		}
	}
}

valid_update_dual_nested_key = """
{
	"action": "update",
	"data": {
		"id1": 1,
		"id2": 3,
		"val1": "testing"
	}
}
"""

dict_valid_update_nested_key = {
	"test": {
		"_schema": dict_dual_key_schema["test"]["_schema"],
		1: {
			2: {
				"id1": 1,
				"id2": 2,
				"val1": "test",
				"_meta": {
					"action": "create",
					"ref_file": "001_create.json",
					"timestamp": DATE_NOW
				}
			},
			3: {
				"id1": 1,
				"id2": 3,
				"val1": "testing",
				"_meta": {
					"action": "update",
					"ref_file": "003_update.json",
					"timestamp": DATE_NOW
				}
			}
		}
	}
}

invalid_schema_missing_id = """
{
	"table": "test",
	"keys": [
	]
}
"""

invalid_schema_missing_table_name = """
{
	"keys": [
		"id1"
	]
}
"""

invalid_schema_missing_keys_field = """
{
	"table": "test"
}
"""

now_list_input_dict = {
	"_schema": {
		"table": "test",
		"keys": ["id1"],
		"should_not_change": "%NOW%"
	},
	1: {
		"id1": 1,
		"dt_now": "%NOW%",
		"l_m_map": [
			{
				"id": 1,
				"dt_now": "%NOW%"
			},
			{
				"id": 2,
				"dt_now": "%NOW%"
			}
		],
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	},
	2: {
		"id1": 1,
		"dt_now": "%NOW%",
		"l_m_map": [
			{
				"id": 1,
				"dt_now": "%NOW%"
			},
			{
				"id": 2,
				"dt_now": "%NOW%"
			}
		],
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	}
}

now_list_output_dict = {
	"_schema": {
		"table": "test",
		"keys": ["id1"],
		"should_not_change": "%NOW%"
	},
	1: {
		"id1": 1,
		"dt_now": DATE_NOW,
		"l_m_map": [
			{
				"id": 1,
				"dt_now": DATE_NOW
			},
			{
				"id": 2,
				"dt_now": DATE_NOW
			}
		],
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	},
	2: {
		"id1": 1,
		"dt_now": DATE_NOW,
		"l_m_map": [
			{
				"id": 1,
				"dt_now": DATE_NOW
			},
			{
				"id": 2,
				"dt_now": DATE_NOW
			}
		],
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	}
}

now_input_dict = {
	"_schema": {
		"table": "test",
		"keys": ["id1"],
		"should_not_change": "%NOW%"
	},
	1: {
		"id1": 1,
		"dt_now": "%NOW%",
		"m_map": {
			"dt_now": "%NOW%"
		},
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	},
	2: {
		"id1": 1,
		"dt_now": "%NOW%",
		"m_map": {
			"dt_now": "%NOW%"
		},
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	}
}

now_output_dict = {
	"_schema": {
		"table": "test",
		"keys": ["id1"],
		"should_not_change": "%NOW%"
	},
	1: {
		"id1": 1,
		"dt_now": DATE_NOW,
		"m_map": {
			"dt_now": DATE_NOW
		},
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	},
	2: {
		"id1": 1,
		"dt_now": DATE_NOW,
		"m_map": {
			"dt_now": DATE_NOW
		},
		"_meta": {
			"action": "create",
			"should_not_change": "%NOW%"
		}
	}
}

dict_compare_created_only_no_changes_current = {
	"field1": "hello",
	"field2": "test1",
	"dt_Created": "blah"
}

dict_compare_created_only_no_changes_new = {
	"field1": "hello",
	"field2": "test1",
	"dt_Created": "blah1"
}

dict_compare_created_only_with_changes_current = {
	"field1": "hello",
	"field2": "test1",
	"dt_Created": "blah"
}

dict_compare_created_only_with_changes_new = {
	"field1": "hello",
	"field2": "test2",
	"dt_Created": "blah1"
}

dict_compare_modified_only_no_changes_current = {
	"field1": "hello",
	"field2": "test1",
	"dt_Modified": "blah"
}

dict_compare_modified_only_no_changes_new = {
	"field1": "hello",
	"field2": "test1",
	"dt_Modified": "blah1"
}

dict_compare_modified_only_with_changes_current = {
	"field1": "hello",
	"field2": "test1",
	"dt_Modified": "blah"
}

dict_compare_modified_only_with_changes_new = {
	"field1": "hello",
	"field2": "test2",
	"dt_Modified": "blah1"
}

dict_compare_created_and_modified_no_changes_current = {
	"field1": "hello",
	"field2": "test1",
	"dt_Modified": "blah",
	"dt_Created": "blah"
}

dict_compare_created_and_modified_no_changes_new = {
	"field1": "hello",
	"field2": "test1",
	"dt_Modified": "blah1",
	"dt_Created": "blah1"
}

dict_compare_created_and_modified_with_changes_current = {
	"field1": "hello",
	"field2": "test1",
	"dt_Modified": "blah",
	"dt_Created": "blah"
}

dict_compare_created_and_modified_with_changes_new = {
	"field1": "hello",
	"field2": "test2",
	"dt_Modified": "blah1",
	"dt_Created": "blah1"
}

dict_list_compare_no_changes_current = [
	dict_compare_created_and_modified_no_changes_current,
	dict_compare_created_and_modified_no_changes_current,
	dict_compare_created_and_modified_no_changes_current,
	dict_compare_created_and_modified_no_changes_current
]

dict_list_compare_no_changes_new = [
	dict_compare_created_and_modified_no_changes_new,
	dict_compare_created_and_modified_no_changes_new,
	dict_compare_created_and_modified_no_changes_new,
	dict_compare_created_and_modified_no_changes_new
]

dict_list_compare_with_changes_current = [
	dict_compare_created_and_modified_with_changes_current,
	dict_compare_created_and_modified_with_changes_current,
	dict_compare_created_and_modified_with_changes_current,
	dict_compare_created_and_modified_with_changes_current
]

dict_list_compare_with_changes_new = [
	dict_compare_created_and_modified_with_changes_new,
	dict_compare_created_and_modified_no_changes_new,
	dict_compare_created_and_modified_no_changes_new,
	dict_compare_created_and_modified_no_changes_new
]

class TestZipExtractor(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.complete_dict = {
			"test": {
				"000_schema.json": json.loads(valid_dual_key_schema),
				"001_create.json": json.loads(valid_create_dual_key)
			},
			"test2": {
				"000_schema.json": json.loads(valid_dual_key_schema),
				"001_create.json": json.loads(valid_create_dual_key)
			}
		}
	
	def test_read_zip_file(self):
		"""
		Tests that a valid zip file yields a valid output dictionary
		"""
		temp_dir = tempfile.mkdtemp()
		try:
			tmp_archive = os.path.join(temp_dir, "test.zip")
			zf = zipfile.ZipFile(tmp_archive, "w", zipfile.ZIP_DEFLATED)
			zf.writestr("test/000_schema.json", valid_dual_key_schema)
			zf.writestr("test/001_create.json", valid_create_dual_key)
			zf.writestr("test2/000_schema.json", valid_dual_key_schema)
			zf.writestr("test2/001_create.json", valid_create_dual_key)
			zf.close()
			self.assertDictEqual(read_zip_file(tmp_archive), self.complete_dict)
		finally:
			shutil.rmtree(temp_dir)

class TestSchema(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.valid_single_key_schema = json.loads(valid_single_key_schema)
		self.valid_dual_key_schema = json.loads(valid_dual_key_schema)
		self.invalid_schema_missing_id = json.loads(invalid_schema_missing_id)
		self.invalid_schema_missing_table_name = json.loads(invalid_schema_missing_table_name)
		self.invalid_schema_missing_keys_field = json.loads(invalid_schema_missing_keys_field)
	
	def test_missing_schema_key(self):
		"""
		Tests for valid exception when the '000_schema.json' key is missing from a table's data
		"""
		test = {
			"test": {
				"00_schema.json": {}
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "000_schema.json is missing for this table: test"):
			validate_and_process(test)
	
	def test_missing_schema_ids(self):
		"""
		Tests for valid exception when the keys attribute in the schema is length 0
		"""
		test = {
			"test": {
				"000_schema.json": self.invalid_schema_missing_id
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Keys attribute in schema is length 0 for table test, expecting at least one element"):
			validate_and_process(test)

	
	def test_missing_table_name(self):
		"""
		Tests for valid exception when the table name attribute is missing from the schema
		"""
		test = {
			"test": {
				"000_schema.json": self.invalid_schema_missing_table_name
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Schema file for test does not contain table name or keys attribute"):
			validate_and_process(test)
	
	def test_missing_keys_field(self):
		"""
		Tests for valid exception when the keys attribute is missing from the schema
		"""
		test = {
			"test": {
				"000_schema.json": self.invalid_schema_missing_keys_field
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Schema file for test does not contain table name or keys attribute"):
			validate_and_process(test)
	
	def test_valid_single_id_schema(self):
		"""
		Tests that a valid schema with a single ID field is created
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_single_key_schema
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_single_key_schema)
	
	def test_valid_dual_id_schema(self):
		"""
		Tests that a valid schema with dual ID fields is created
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_dual_key_schema)

class TestCreate(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.valid_single_key_schema = json.loads(valid_single_key_schema)
		self.valid_dual_key_schema = json.loads(valid_dual_key_schema)
		self.valid_create_single_key = json.loads(valid_create_single_key)
		self.valid_create_dual_key = json.loads(valid_create_dual_key)
		self.valid_create_dual_nested_key = json.loads(valid_create_dual_nested_key)
		self.create_missing_key = json.loads(create_missing_key)
	
	def test_valid_create_single_key(self):
		"""
		Tests that a valid create record for a single key schema works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_single_key_schema,
				"001_create.json": self.valid_create_single_key
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_create_single_key)
	 
	def test_create_missing_id_single(self):
		"""
		Tests that a valid exception is thrown for a create missing an ID field
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_single_key_schema,
				"001_create.json": self.create_missing_key
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "One or more key fields are missing in record file 001_create.json for table test"):
			validate_and_process(test)
	
	def test_create_duplicated_key_single(self):
		"""
		Tests that a valid exception is thrown when attempting a create with a duplicated key
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_single_key_schema,
				"001_create.json": self.valid_create_single_key,
				"002_create.json": self.valid_create_single_key
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Check record file 002_create.json for table test as action is 'create' but keys have been seen before"):
			validate_and_process(test)
	
	def test_valid_dual_key(self):
		"""
		Tests that a valid create record for a dual key schema works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_create_dual_key)
	
	def test_missing_dual_key(self):
		"""
		Tests that a valid exception is thrown when one key field from a dual key schema is missing
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_single_key
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "One or more key fields are missing in record file 001_create.json for table test"):
			validate_and_process(test)
	
	def test_valid_dual_nested_key(self):
		"""
		Tests that a valid create record for a dual nested key schema works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key,
				"002_create.json": self.valid_create_dual_nested_key
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_create_nested_key)
	
	def test_create_duplicated_key_dual(self):
		"""
		Tests that a valid exception is thrown when attempting a create with a duplicated key
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key,
				"002_create.json": self.valid_create_dual_key
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Check record file 002_create.json for table test as action is 'create' but keys have been seen before"):
			validate_and_process(test)

class TestUpdate(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.valid_single_key_schema = json.loads(valid_single_key_schema)
		self.valid_dual_key_schema = json.loads(valid_dual_key_schema)
		self.valid_create_dual_key_multi_field = json.loads(valid_create_dual_key_multi_field)
		self.update_missing_key = json.loads(update_missing_key)
		self.valid_update = json.loads(valid_update)
		self.valid_update_single_col = json.loads(valid_update_single_col)
		self.valid_create_dual_key = json.loads(valid_create_dual_key)
		self.valid_create_dual_nested_key = json.loads(valid_create_dual_nested_key)
		self.valid_update_dual_nested_key = json.loads(valid_update_dual_nested_key)
		self.valid_delete = json.loads(valid_delete)
	
	def test_missing_key(self):
		"""
		Tests that an update with a missing key field fails
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key_multi_field,
				"002_update.json": self.update_missing_key
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "One or more key fields are missing in record file 002_update.json for table test"):
			validate_and_process(test)
	
	def test_update_without_create(self):
		"""
		Tests that a valid exception is thrown when an update is attempted on a record which does not exist
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"002_update.json": self.valid_update
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Check record file 002_update.json for table test as action is 'update' but keys have not been seen before"):
			validate_and_process(test)
	
	def test_update_all_cols(self):
		"""
		Tests a valid update of all columns works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key_multi_field,
				"002_update.json": self.valid_update
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_update)
	
	def test_update_single_col(self):
		"""
		Test a valid update of a single column works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key_multi_field,
				"002_update.json": self.valid_update_single_col
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_update_single_col)
	
	def test_update_single_entry(self):
		"""
		Tests that a single record is updated for a table with multiple records
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key,
				"002_delete.json": self.valid_delete,
				"003_update.json": self.valid_update
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Check record file 003_update.json for table test as action is update but record has previously been deleted"):
			validate_and_process(test)
	
	def test_update_of_deleted_record(self):
		"""
		Tests that a deleted record cannot be updated and a valid exception is thrown
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key,
				"003_update.json": self.valid_update_dual_nested_key
			}
		}

class TestDelete(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.valid_single_key_schema = json.loads(valid_single_key_schema)
		self.valid_dual_key_schema = json.loads(valid_dual_key_schema)
		self.valid_create_dual_key_multi_field = json.loads(valid_create_dual_key_multi_field)
		self.delete_missing_key = json.loads(delete_missing_key)
		self.valid_delete = json.loads(valid_delete)
	
	def test_missing_key(self):
		"""
		Tests that an delete with a missing key field fails
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key_multi_field,
				"002_delete.json": self.delete_missing_key
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "One or more key fields are missing in record file 002_delete.json for table test"):
			validate_and_process(test)
	
	def test_delete_without_create(self):
		"""
		Tests that a valid exception is thrown when an delete is attempted on a record which does not exist
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"002_delete.json": self.valid_delete
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Check record file 002_delete.json for table test as action is 'delete' but keys have not been seen before"):
			validate_and_process(test)
	
	def test_delete(self):
		"""
		Tests a valid delete works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key_multi_field,
				"002_delete.json": self.valid_delete
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_delete)

class TestMisc(unittest.TestCase):
	def setUp(self):
		self.maxDiff = None
		self.valid_dual_key_schema = json.loads(valid_dual_key_schema)
		self.invalid_action = json.loads(invalid_action)
		self.record_missing_data = json.loads(record_missing_data)
		self.record_missing_action = json.loads(record_missing_action)
	
	def test_invalid_action(self):
		"""
		Tests that a valid exception is thrown for an invalid action
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_action.json": self.invalid_action
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Action value is unknown in record file 001_action.json for table test"):
			validate_and_process(test)
	
	def test_data_attribute_missing(self):
		"""
		Tests that a valid exception is thrown when the data attribute is missing
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_action.json": self.record_missing_data
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Record file 001_action.json for table test does not contain action and data attribute"):
			validate_and_process(test)
	
	def test_action_attribute_missing(self):
		"""
		Tests that a valid exception is thrown when the action attribute is missing
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_action.json": self.record_missing_action
			}
		}
		with self.assertRaisesRegexp(MalformedTableData, "Record file 001_action.json for table test does not contain action and data attribute"):
			validate_and_process(test)
	
	def test_now_replacer(self):
		"""
		Tests that %NOW% is expanded to DATE_NOW
		"""
		test = expand_special_values(now_input_dict)
		self.assertDictEqual(test, now_output_dict)

	def test_now_list_replacer(self):
		"""
		Tests that %NOW% is expanded to DATE_NOW for list cases
		"""
		test = expand_special_values(now_list_input_dict)
		self.assertDictEqual(test, now_list_output_dict)
	
	def test_deep_compare_dict_created_only_no_changes(self):
		"""
		Tests that deep compare works for dictionary fields where no changes in dict other than DT_CREATED
		"""
		self.assertTrue(deep_field_compare(dict_compare_created_only_no_changes_new, dict_compare_created_only_no_changes_current))
	
	def test_deep_compare_dict_modified_only_no_changes(self):
		"""
		Tests that deep compare works for dictionary fields where no changes in dict other than DT_MODIFIED
		"""
		self.assertTrue(deep_field_compare(dict_compare_modified_only_no_changes_new, dict_compare_modified_only_no_changes_current))
	
	def test_deep_compare_dict_created_and_modified_no_changes(self):
		"""
		Tests that deep compare works for dictionary fields where no changes in dict other than DT_MODIFIED / DT_CREATED
		"""
		self.assertTrue(deep_field_compare(dict_compare_created_and_modified_no_changes_new, dict_compare_created_and_modified_no_changes_current))
		
	def test_deep_compare_dict_created_only_with_changes(self):
		"""
		Tests that deep compare works for dictionary fields where other changes from DT_CREATED
		"""
		self.assertFalse(deep_field_compare(dict_compare_created_only_with_changes_new, dict_compare_created_only_with_changes_current))
	
	def test_deep_compare_dict_modified_only_with_changes(self):
		"""
		Tests that deep compare works for dictionary fields where other changes from DT_CREATED
		"""
		self.assertFalse(deep_field_compare(dict_compare_modified_only_with_changes_new, dict_compare_modified_only_with_changes_current))
	
	def test_deep_compare_dict_created_and_modified_with_changes(self):
		"""
		Tests that deep compare works for dictionary fields with changes in dict other than DT_MODIFIED / DT_CREATED
		"""
		self.assertFalse(deep_field_compare(dict_compare_created_and_modified_with_changes_new, dict_compare_created_and_modified_with_changes_current))
	
	def test_deep_compare_dict_list_no_changes(self):
		"""
		Tests that deep compare works for a list of dict with no changes
		"""
		self.assertTrue(deep_field_compare(dict_list_compare_no_changes_new, dict_list_compare_no_changes_current))
	
	def test_deep_compare_dict_list_with_changes(self):
		"""
		Tests that deep compare works for a list of dict with with changes
		"""
		self.assertFalse(deep_field_compare(dict_list_compare_with_changes_new, dict_list_compare_with_changes_current))
		
			
if __name__ == "__main__":
	unittest.main()
