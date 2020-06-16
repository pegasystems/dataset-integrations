### Usage of Data Transform SerDe in Kafka DataSet

DataTransformSerde class enables users to use custom data transform objects for serializing/deserializing data to Kafka topics.
This document demonstrates a basic example of configuring Kafka dataset with Data Transform SerDe.

For example, we may have a simple data type like this:

![Alt text](./data_transform_images/01.png?raw=true)

Example Kafka dataset configuration Data Transform SerDe:

![Alt text](./data_transform_images/02.png?raw=true)

Custom record type should be selected and __com.pega.integration.kafka.DataTransformSerde__ should be provided as serialization implementation class.
Additionaly, the mandatory parameter __data.transform.name__ should be provided.

__MyDataTransform__ may look like this:

![Alt text](./data_transform_images/03.png?raw=true)

Here is a mapping of _score_ field in clipboard page to _kafkaScore_ field in Kafka messages. 
This implies that messages on the topic associated with your Kafka dataset will be in JSON format and have _kafkaScore_ field originating from
_score_ field of your source clipboard pages. When you browse the data in your Kafka dataset, the data of _kafkaScore_ field will be mapped back to _score_ field in the clipboard pages.  