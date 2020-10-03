
# Books Catalog Event Sourcing with Kafka Streams

ProofOfConcept on how to implement [Command Query Responsibility Segregation](https://docs.microsoft.com/en-us/azure/architecture/patterns/cqrs) 
and [Event Sourcing](https://docs.microsoft.com/en-us/azure/architecture/patterns/event-sourcing) with:

- Scala
- Kafka Streams
- Kafka Streams - Interactive Queries
- Akka Http

## Logical Architecture

![logical-view](docs/logical-view.drawio.png)


## Technical Architecture

![technical-view](docs/technical-view.drawio.png)


## TODO

- http apis (commands and queries)
- create topics at startup with correct properties (compact, retention, ...) 
- test with multiple instances
