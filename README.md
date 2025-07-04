# Tsurugi DB output plugin for Embulk

Tsurugi DB output plugin for Embulk loads records to Tsurugi using [Tsubakuro](https://github.com/project-tsurugi/tsubakuro).

## Overview

* **Plugin type**: output
* Embulk 0.10 or later
* Java11 or later

## Example

```yaml
out:
  type: tsurugidb
  endpoint: tcp://localhost:12345
  table: test
  mode: insert_direct
  method: insert
```




## Configuration

* **endpoint**: endpoint for Tsurugi (string, required)
* **connection_label**: connection label (string, default: `embulk-output-tsurugidb`)
* **tx_type**: transaction type (`OCC`, `LTX`) (string, default: `LTX`)
* **tx_label**: transaction label (string, default: `embulk-output-tsurugidb`)
* **tx_write_preserve**: (LTX only) write preserve (list of string, defualt: empty list)
* **tx_inclusive_read_area**: (LTX only) inclusive read area (list of string, defualt: empty list)
* **tx_exclusive_read_area**: (LTX only) exclusive read area (list of string, defualt: empty list)
* **tx_priority**: (LTX only) transaction priority (string, default: `null`)
* **commit_type**: commit type (string, default: `default`)
* **session_keep_alive**: session shutdown type (boolean, default: `null`)
* **session_shutdown_type**: session shutdown type (string, default: `nothing`)
* **table**: destination table name (string, required)
* **mode**: "insert_direct" (string, default: `insert_direct`)
* **method**: insert method. see below. (string, default: `insert`)
* **method_option**: method option. see below. (string, default: `insert` for SQL, `put_overwrite` for KVS)
* **retry_limit**: max retry count for database operations (integer, default: 12). When intermediate table to create already created by another process, this plugin will retry with another table name to avoid collision.
* **retry_wait**: initial retry wait time in milliseconds (integer, default: 1000 (1 second))
* **max_retry_wait**: upper limit of retry wait, which will be doubled at every retry (integer, default: 1800000 (30 minutes))
* **batch_size**: size of a single batch insert (integer, default: 16777216)
* **default_timezone**:
* **column_options**: advanced: key-value pairs where key is a column name and value is options for the column.
  * **type**:  type of a column when this plugin creates new tables (string, default: `null`)
  * **value_type**: This plugin converts input column type (embulk type) into a database type to build a INSERT statement. (string, default: `coerce`)
  * **timestamp_format**:
  * **timezone**:
* **connect_timeout**: timeout for establishment of a database connection (integer (seconds), default: 300)
* **begin_timeout**: timeout for begin transaction (integer (seconds), default: 300)
* **insert_timeout**: timeout for insert (integer (seconds), default: 300)
* **commit_timeout**: timeout for commit (integer (seconds), default: 300)
* **session_shutdown_timeout**: timeout for session shutdown (integer (seconds), default: 300)
* **validate_nul_char**: (ver 0.1.3) validate nul-char (boolean, default: true)
* **log_level_on_invalid_record**: (ver 0.1.3) log level for invalid data (string, default: DEBUG)
* **stop_on_invalid_record**: (ver 0.1.3) Stop bulk load transaction if a file includes invalid record (such as invalid char) (boolean, default: false)

### method

| method                 | API  | description                                        |
| ---------------------- | ---- | -------------------------------------------------- |
| `insert`               | SQL  | Insert and wait for insert completion all at once. |
| `insert_wait`          | SQL  | Insert and wait for each insert to complete.       |
| `insert_multi_values`  | SQL  | Insert with multiple values. (ver 1.0.1)           |
| `insert_batch`         | SQL  | Insert using batch API.                            |
| `put`                  | KVS  | Put and wait for put completion all at once.       |
| `put_wait`             | KVS  | Put and wait for each put to complete.             |
| `put_batch`            | KVS  | Put using batch API.                               |

### method_option

| method_option          | API  | description                                                                     |
| ---------------------- | ---- | ------------------------------------------------------------------------------- |
| `insert`               | SQL  | insert (if record exists, unique constraint will be violated)                   |
| `insert_or_replace`    | SQL  | insert or replace (insert if record does not exist, update if it exists)        |
| `insert_if_not_exists` | SQL  | insert if not exists (insert if record does not exist, do nothing if it exists) |
| `put_overwrite`        | KVS  | put (similar to `insert_or_replace`)                                            |
| `put_if_absent`        | KVS  | put only when record does not exist (similar to `insert_if_not_exists`)         |
| `put_if_present`       | KVS  | put only when record exists                                                     |


## Install

1. install plugin
   ```
   $ java -jar embulk-0.11.3.jar install io.github.hishidama.embulk:embulk-output-tsurugidb:1.5.0
   ```

2. add setting to $HOME/.embulk/embulk.properties
   ```
   plugins.output.tsurugidb=maven:io.github.hishidama.embulk:tsurugidb:1.5.0
   ```

| version       | Tsurugi       | Tsubakuro |
|---------------|---------------|-----------|
| 1.0.0 - 1.0.2 | 1.0.0         | 1.6.0     |
| 1.1.0         | 1.1.0 - 1.2.0 | 1.7.0     |
| 1.3.0         | 1.3.0         | 1.8.0     |
| 1.4.0         | 1.4.0         | 1.9.0     |
| 1.5.0         | 1.5.0         | 1.10.0    |


## Build

### Test

```
./gradlew test -Pendpoint=tcp://localhost:12345
ls build/reports/tests/test/
```

### Build to local Maven repository

```
./gradlew generatePomFileForMavenJavaPublication
mvn install -f build/publications/mavenJava/pom-default.xml
./gradlew publishToMavenLocal
```

