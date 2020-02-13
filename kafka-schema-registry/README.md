## Confluent schema registry support

This component provides [Avro](https://avro.apache.org) data format support for Kafka data set and integration with [Confluent schema registry](https://docs.confluent.io/current/schema-registry/index.html).

Main features:

* Serialization from Clipboard page to Avro
* Deserialization from Avro to Clipboard page
* [Confluent avro-serializer](https://github.com/confluentinc/schema-registry/tree/master/avro-serializer) compatible SerDe for Kafka data set
* End user data instance UI to configure SerDe parameters

## Using schema registry with Kafka data set

See [Using schema registry with Kafka data set](./docs/USAGE.md)

## Supported platform versions

The component has been tested with the following platform versions:

* Pega platform 8.2.x

## Building from source

In order to build the component from source, you need to have access to **coreBuildDistributionImage** and have **Gradle 4.8** or higher.

1. Extract **coreBuildDistributionImage.zip/archives/pegadbinstall-classes.zip** to a temporary folder
2. Extract **coreBuildDistributionImage.zip/archives/prweb.war** to a temporary folder
3. Copy the following jar files to the **libs** folder:
   1. pegadbinstall-classes/lib/pega/prpublic.jar → libs/prpublic.jar
   2. pegadbinstall-classes/lib/pega/prprivate.jar → libs/prprivate.jar
   3. pegadbinstall-classes/lib/pega/prprivcommon.jar → libs/prprivcommon.jar
   4. pegadbinstall-classes/lib/pega/printegrext.jar → libs/printegrext.jar
   5. pegadbinstall-classes/lib/pega/printegrint.jar → libs/printegrint.jar
   6. pegadbinstall-classes/lib/pega/prenginext.jar → libs/prenginext.jar
   7. pegadbinstall-classes/lib/pega/prcommons-lang.jar → libs/prcommons-lang.jar
   8. pegadbinstall-classes/lib/pega/pricu2jdk.jar → libs/pricu2jdk.jar
   9. pegadbinstall-classes/lib/pega/d-node-x.y.x.jar → libs/d-node.jar
   10. prweb.war/WEB-INF/lib/prbootstrap-x.y.x.jar → libs/prbootstrap.jar
   11. prweb.war/WEB-INF/lib/prbootstrap-api-x.y.x.jar → libs/prbootstrap-api.jar
4. Run `./gradlew createWrapperJar` to generate **build/libs/schema-registry-integration-1.0.0.jar** component jar, which can be installed as a component

## Avro and Java Types

Avro has 8 primitive types and 5 complex types and 1 other type called 'union' which is a combination of multiple types. 
The following table maps these 13 Avro types to their input interfaces (the Java types which can be put into GenericRecord fields) and their output implementations (the concrete Java types which are returned from GenericRecord fields).

Avro Type | Input Interface | Output Implementation
--- | --- | ---
null | | null                    
boolean | java.lang.Boolean | java.lang.Boolean         
int | java.lang.Integer | java.lang.Integer
long | java.lang.Long | java.lang.Long            
float | java.lang.Float | java.lang.Float          
double | java.lang.Double | java.lang.Double         
bytes | java.nio.ByteBuffer | java.nio.HeapByteBuffer  
string | java.lang.CharSequence | org.apache.avro.util.Utf8 
record | *.GenericRecord | *.GenericData$Record      
enum | GenericEnumSymbol | *.GenericData$EnumSymbol  
array | java.util.Collection | *.GenericData$Array       
map | java.util.Map | java.util.HashMap         
fixed | *.GenericFixed | *.GenericData$Fixed

Reference: https://stackoverflow.com/a/34234039
