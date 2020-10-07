hive-jq-udtf
============

jq for Hive

Installation
------------

1. Build jar with maven and install `target/hive-jq-udtf-$VERSION.jar` into your `hive.aux.jars.path`.

   ```
   mvn clean package -DskipTests
   ```

   Alternatively, you can download the pre-built jar at [Maven Central](http://central.maven.org/maven2/jp/co/cyberagent/hive/hive-jq-udtf-v3/).

2. CREATE FUNCTION

   ```sql
   CREATE FUNCTION jq3 AS 'jp.co.cyberagent.hive.udtf.jsonquery.v3.JsonQueryUDTF';
   ```

   You can choose whatever name of the function, but we recommend to use `jq<version>` style naming where `<version>` is the version number in the package name. We increment the version number always when we change something that breaks compatibility with the older versions. This allows multiple versions of this plugin to co-exist at the same time when migrating to a newer version, etc.

See [Deploying Jars for User Defined Functions and User Defined SerDes](https://cwiki.apache.org/confluence/display/Hive/HivePlugins#HivePlugins-DeployingJarsforUserDefinedFunctionsandUserDefinedSerDes) section of the official Hive documentation for more deployment details.

### Requirements

* Java 1.8
* [Apache Hive](https://hive.apache.org/) >= 1.1.0

Usage
-----

There are two variants (i.e. overloads) of the UDTF:

* `jq(JSON, JQ, TYPE)`
* `jq(JSON, JQ, FIELD_1:TYPE_1, ..., FIELD_N:TYPE_N)`

The UDTF parses `JSON` text and feed it to a `JQ` filter, which in turn produces 0 or more results. The filter results are still JSON, so the UDTF converts each of the results to a row suitable in Hive.
This final conversion process differs slightly depending on which variant to use.

Note that `JQ`, `TYPE` and `FIELD_N:TYPE_N` must be a **constant** string (or a constant expression which evaluates to a string).

### jq(JSON, JQ, TYPE)

This variant converts each `JQ` result to a Hive row containing a single `TYPE` column.

#### Example

1. Extracting a single integer from JSON.

   ```sql
   SELECT jq('{"region": "Asia", "timezones": [{"name": "Tokyo", "offset": 540}, {"name": "Taipei", "offset": 480}, {"name": "Kamchatka", "offset": 720}]}',
              '.timezones[]|select(.name == "Tokyo").offset',
              'int');
   ```
   ```
   +-------+
   | col1  |
   +-------+
   | 540   |
   +-------+
   ```

2. `JQ` is allowed to produce more than one results and the UDTF also supports more complex types.

   ```sql
   SELECT jq('{"region": "Asia", "timezones": [{"name": "Tokyo", "offset": 540}, {"name": "Taipei", "offset": 480}, {"name": "Kamchatka", "offset": 720}]}',
              '.region as $region | .timezones[] | {name: ($region + "/" + .name), offset}',
              'struct<name:string,offset:int>');
   ```
   ```
   +-----------------------------------------+
   |                  col1                   |
   +-----------------------------------------+
   | {"name":"Asia/Tokyo","offset":540}      |
   | {"name":"Asia/Taipei","offset":480}     |
   | {"name":"Asia/Kamchatka","offset":720}  |
   +-----------------------------------------+
   ```

### jq(JSON, JQ, FIELD_1:TYPE_1, ..., FIELD_N:TYPE_N)

This variant can produce rows with more than one columns (`FIELD_1`,..., `FIELD_N`).
The fields (`FIELD_1`, ..., `FIELD_N`) of the `JQ` result are individually converted to respective Hive types (`TYPE_1`, ..., `TYPE_N`), which eventually assemble to a Hive row.

#### Example

1. Transforming a JSON into Hive rows with multiple columns.

   ```sql
   SELECT jq('{"region": "Asia", "timezones": [{"name": "Tokyo", "offset": 540}, {"name": "Taipei", "offset": 480}, {"name": "Kamchatka", "offset": 720}]}',
              '.region as $region | .timezones[] | {name: ($region + "/" + .name), offset}',
              'name:string', 'offset:int');
   ```
   ```
   +-----------------+---------+
   |      name       | offset  |
   +-----------------+---------+
   | Asia/Tokyo      | 540     |
   | Asia/Taipei     | 480     |
   | Asia/Kamchatka  | 720     |
   +-----------------+---------+
   ```

### Using lateral views

> Lateral view is used in conjunction with user-defined table generating functions such as explode(). [...]
> A lateral view first applies the UDTF to each row of base table and then joins resulting output rows to the input rows to form a virtual table having the supplied table alias. &mdash; <cite>[Hive Language Manual, Lateral View][1]</cite>

#### Example

```sql
-- Prepare `regions` table for LATERAL VIEW example
CREATE TABLE regions (region STRING, timezones STRING);
INSERT INTO regions (region, timezones) VALUES ('Asia', '[{"name":"Tokyo","offset":540},{"name":"Taipei","offset":480},{"name":"Kamchatka","offset":720}]');
```

```sql
SELECT r.region, tz.name, tz.offset FROM regions r LATERAL VIEW jq(r.timezones, '.[]', 'name:string', 'offset:int') tz;
```
```
+-----------+------------+------------+
| r.region  |  tz.name   | tz.offset  |
+-----------+------------+------------+
| Asia      | Tokyo      | 540        |
| Asia      | Taipei     | 480        |
| Asia      | Kamchatka  | 720        |
+-----------+------------+------------+
```

### Handling corrupt JSON inputs

If the UDTF fails to parse JSON, jq input (`.`) becomes `null` and `$error` object is set to something like below.

```javascript
{
  "message": "Unrecognized token 'string': was expecting ('true', 'false' or 'null')\n at [Source: \"corrupt \"string; line: 1, column: 33]",
  "class": "jp.co.cyberagent.hive.udtf.jsonquery.v3.shade.com.fasterxml.jackson.core.JsonParseException",
  "input": "\"corrupt \"string"
}
```

#### Example

1. To substitute something in case of a currupt JSON,

   ```sql
   SELECT jq('"corrupt "string', 'if $error then "INVALID" else . end', 'string');
   ```
   ```
   +----------+
   |   col1   |
   +----------+
   | INVALID  |
   +----------+
   ```

2. To skip a corrupt JSON,

   ```sql
   SELECT jq('"corrupt "string', 'if $error then empty else . end', 'string');
   ```
   ```
   +----------+
   |   col1   |
   +----------+
   +----------+
   ```

3. To abort a query on a corrupt JSON,

   ```sql
   SELECT jq('"corrupt "string', 'if $error then error($error.message) else . end', 'string');
   ```
   ```
   Error: java.io.IOException: org.apache.hadoop.hive.ql.metadata.HiveException: jq returned an error "Unrecognized token 'string': was expecting ('true', 'false' or 'null')  at [Source: "corrupt "string; line: 1, column: 33]" from input: "corrupt "string (state=,code=0)
   ```

### Supported Hive types

* `int`, `bigint`, `float`, `double`, `boolean`, `string`
* `struct<...>`, `array<T>`, `map<string, T>`

License
-------

Copyright (c) CyberAgent, Inc. All Rights Reserved.

[The Apache License, Version 2.0](LICENSE)

[1]: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView

