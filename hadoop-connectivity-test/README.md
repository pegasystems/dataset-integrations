## Hadoop connectivity test tool
Application tests hadoop configuration. Connects to hadoop and performs tests to check if principal has read and modification rights

## How to use

1. Build the project
2. Create a property file that contains hadoop configuration properties and properties specific to this tool. Tool-specific properties are prefixed by "client." There are following tool-specific properties:
```
   client.principal - principal that connects to hadoop
   client.keytabPath - path to keytab used to log in
   client.testWorkDir - a directory in HDFS in which tests are performed
   client.krb5conf - location of krb5.conf. Optional if krb5.conf file is in default location
```
Property file can be created from connectivity.properties.sample file in root directory of this project
3. Run: `java -jar build/libs/hadoop-connectivity-test-1.0-SNAPSHOT.jar com.pega.hdfs.Main /path/to/propertiesfile`


