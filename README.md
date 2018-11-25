# debezium_kafka_to_kinesis
Using Redhat Debezium embedded engine

I did proof of concept of using Debezium Embedded Engine API. The original article is at https://debezium.io/blog/2018/08/30/streaming-mysql-data-changes-into-kinesis/.

I modified the POM file so it can compile and build the FAT jar file.

After clone this repo, you can build the FAT jar using command -> mvn clean package.

In case that you want to run the jar file, simply using command -> java -jar target/abc.jar (abc is the output jar file).

For testing, please follow the instructions in the above original article at Debezium blog website.

Don't forget to modify the variable "regionName" in the main java file to match your AWS Kinesis region.

# For consume the data from Kinesis
