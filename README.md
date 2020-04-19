# twitter

This is a flink program which is used to fetch tweets via twitter API. Also this filters out only english tweets and then sends those
messages to Kafka in Json Format. 

To run this program 
first add your twitter consumer access keys and secrets in fetchTweets.java

Then you have to run a apache kafka broker locally. To help with that, the easiest is make use of docker to run clonfluent kafka. 

follow this : https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html#ce-docker-quickstart

Now after running kafka just run this as a java application. And you can see messages in kafka coming. 
