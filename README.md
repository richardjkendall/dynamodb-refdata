![build status](https://travis-ci.org/richardjkendall/dynamodb-refdata.svg?branch=master "Build status")

# DynamoDB Reference Data Manager
I created this set of scripts to help provide the same kind of functionality that you get with Flyway https://flywaydb.org/

I needed to manage a set of reference data stored in DynamoDB using my CI/CD pipelines.  I previously used RDS and Flyway, but found no similar tool for DynamoDB.  This set of code is designed to be run by CodePipeline, and I provide examples here on how to do this.  It can also be used from the command line as well.

## Table of contents

[How it works](#how-it-works)
[Structure of the reference data](#structure-of-the-reference-data)


## How it works
This is designed to be deployed as a lambda function and run by AWS CodePipeline.  It works in two steps:

1. create a report showing what will change when the tool runs
2. enact the changes shown in the report

The idea is that an approval stage is put between these two steps so you don't make changes you were not expecting.

## Structure of the reference data
The tool expects to be presented with a zip file containing one folder per table that will be managed.  Each folder should contain a list of JSON files number sequentially.  For example if you had two tables called TableOne and TableTwo the tool would expect:

```
TableOne
  000_schema.json
  001_create_record.json
  002_update_record.json
  ...
TableTwo
  000_schema.json
  001_create_record.json
  002_update_record.json
```

Each folder must contain a file called 000_schema.json, and this file must have particular content.  The remaining files create, update and delete records and they must be ordered sequentially.  It is recommended to use as many leading zeros as you think will be needed for future proofing when creating these numbers.

### 000_schema.json


## Permissions needed

