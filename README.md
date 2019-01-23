# DynamoDB Reference Data Manager
I created this set of scripts to help provide the same kind of functionality that you get with Flyway https://flywaydb.org/

I needed to manage a set of reference data stored in DynamoDB using my CI/CD pipelines.  I previously used RDS and Flyway, but found no similar tool for DynamoDB.  This set of code is designed to be run by CodePipeline, and I provide examples here on how to do this.  It can also be used from the command line as well.

## How it works

## Permissions needed

