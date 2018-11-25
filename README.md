# debezium_kafka_to_kinesis
Using Redhat Debezium embedded engine

I did proof of concept of using Debezium Embedded Engine API. The original article is at https://debezium.io/blog/2018/08/30/streaming-mysql-data-changes-into-kinesis/.

I modified the POM file so it can compile and build the FAT jar file.

After clone this repo, you can build the FAT jar using command -> mvn clean package.

In case that you want to run the jar file, simply using command -> java -jar target/abc.jar (abc is the output jar file).

For testing, please follow the instructions in the above original article at Debezium blog website.

Don't forget to modify the variable "regionName" in the main java file to match your AWS Kinesis region.

# For consume the data from Kinesis

I use the Python library that consume data from AWS Kinesis Stream from https://github.com/NerdWalletOSS/kinesis-python.git

I think it easy to use than the original library from AWS.

The code is inside the folder "kinesis-consumer-python" of this repo.

# How to run the AWS Kinesis Consumer (in Python)

0. Cd to folder "kinesis-consumer-python"

1. Go to your AWS account and create Dynamodb table with Primary partition key value = shard (String). Name the table whatever you want.

2. Run command -> pip install -r requirements.txt

3. Modify the content of file test-consumer.py to match the stream_name of Kinesis Stream and DynamoDB table you've just created.

4. Move the file test-consumer.py to the src folder.

5. Run test-consumer.py with command -> python test-consumer.py

