# Kafka POC

This is a simple Proof-of-Concept, which does the following:

* Read four text files containing JSON and put them on a Kafka topic each 
* Consume the topics
* Group each stream to a common key and aggregate the content in lists
* Merge the streams
* Publish the result on a topic

To execute the POC:
1. Run docker compose on the full-stack-kafka-yml (found under resources)
2. Run KafkaCustomerFileTopicDispatch (found in the test package)
3. Run PocApplication

The population of the topics can be seen using Kafka-Topic-UI by opening localhost:8000

> I join two streams in the morning  
> I join two streams at night  
> I join two streams in the afternoon  
> It makes me feel alright  
> I join two streams in time of peace  
> And two in time of war  
> I join two streams before I join two streams  
> And then I join two more  