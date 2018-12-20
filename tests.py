import unittest
import json

from lambda_function import validate_and_process, DATE_NOW
from errors import MalformedTableData

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
					"action": "create",
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
		self.valid_create_dual_key = json.loads(valid_create_dual_key)
		self.valid_create_dual_nested_key = json.loads(valid_create_dual_nested_key)
		self.valid_update_dual_nested_key = json.loads(valid_update_dual_nested_key)
	
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
		Tests a valid update works
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key_multi_field,
				"002_update.json": self.valid_update
			}
		}
		self.assertDictEqual(validate_and_process(test), dict_valid_update)
	
	def test_update_single_entry(self):
		"""
		Tests that a single record is updated for a table with multiple records
		"""
		test = {
			"test": {
				"000_schema.json": self.valid_dual_key_schema,
				"001_create.json": self.valid_create_dual_key,
				"002_create.json": self.valid_create_dual_nested_key
			}
		}
		
		
			
if __name__ == "__main__":
	unittest.main()
