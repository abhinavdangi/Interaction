# Interaction
This project gives a small demo of Spark structured streaming. Given a streaming data of people's 
location, and static information of customers for MyMall, we have to estimate the best Product 
category for the person and send required notification.
It is an independent project without any external requirements.

##Setup
#####Pre-requisites: 
IDE, Java 8, Scala 2.11.x SDK
#####Steps
1. Clone the project.
2. Open the project using pom file
3. For Intellij IDE: Goto File -> Project Structure -> Platform Settings -> SDK -> Add 1.8s, remove all others.
4. For Intellij IDE: Goto File -> Project Structure -> Libraries -> + Scala SDK -> Add 2.11.x, remove all others.
5. Do mvn clean and mvn install. 

##Execution
To run the project, follow the steps:
1. Run StartKafka. This will start the zookeeper and kafka in local box. Verify using kafka commands.
2. Run ProductCategory. It will start the spark structured streaming.
3. Run EventGenerator. This generates random events for people's location

#####Kafka Commands
######For checking topics in Kafka
bin/kafka-topics.sh --zookeeper localhost:2181 --list
######For consuming the data
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic person_product_category --from-beginning
