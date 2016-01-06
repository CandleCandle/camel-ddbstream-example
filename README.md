
DynamoDB Streams with Apache Camel
==================================

Example code to demonstrate how to use the Camel DynamoDB Stream Endpoint.

Building
--------

```
mvn clean install assembly:single
```

Running
-------

```
java -jar target/camel-ddbstream-example-0.1-SNAPSHOT-jar-with-dependencies.jar
```

Honoured -D options
-------------------

* -DLOG.<package name>=[off,error,warn,etc...]
* -DHTTPS\_PROXY=http://host:port
* -Dhttps\_proxy=http://host:port

Parameters
----------

* --iterator-type VAL   : One of: trim\_horizon, latest, at\_sequence\_number or after\_sequence\_number
* --region VAL          : The AWS region to use
* --sequence-number VAL : The sequence number to start to start producing exchanges at. Required if --iterator-type is one of {at,after}\_sequence\_number
* --table-name VAL      : DynamoDB table from which to obtain the change stream


