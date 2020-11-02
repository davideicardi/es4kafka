
# Event Sourcing with Kafka Streams

Event driven/event sourcing microservice example written with:

- [Scala](https://scala-lang.org/)
- [Kafka Streams](https://kafka.apache.org/documentation/streams/)
    - [Kafka Streams - Interactive Queries](https://docs.confluent.io/current/streams/developer-guide/interactive-queries.html)
- [Akka Http](https://doc.akka.io/docs/akka-http/current/index.html)
- [Kaa Schema Registry](https://github.com/davideicardi/kaa)

## Patterns

- [Command Query Responsibility Segregation](https://docs.microsoft.com/en-us/azure/architecture/patterns/cqrs)
- [Event Driven](https://docs.microsoft.com/en-us/azure/architecture/guide/architecture-styles/event-driven)
- [Domain Driven Design](https://martinfowler.com/bliki/DomainDrivenDesign.html)
- [Event Sourcing](https://docs.microsoft.com/en-us/azure/architecture/patterns/event-sourcing)

## Domain Model

In this example I want to implement a very simple books catalog, when the user insert a list of authors, a list of books and can query the books for a specific author:

![domain-model](docs/domain-model.drawio.png)

NOTE: For the current model this is for sure an over-kill architecture, but the idea is just to keep the model simple for demostration purpose.

## Logical Architecture

![logical-view](docs/logical-view.drawio.png)


## Technical Architecture

![technical-view](docs/technical-view.drawio.png)

## Kafka Stream Topology

![event-sourcing-topology](docs/event-sourcing-topology.drawio.png)

## Microservices

This example can be used as a template for a service/microservice. You can use the `es4kafka` library to share common features.

One important aspect to note regarding microservice architecture, is that every microservice should expose a public "interface" to the rest of the world.
In this case the public interace is composed by:

- **HTTP Api**
    - get author
    - create author
    - get all authors
    - ...
- **Kafka topics**
    - events
    - snapshots

Other microservice should just rely on this public interface. Potentially the implementation can change,
we can use another technology instead of Kafka Streams, but the public interface can remain the same.

## Why?

Why CQRS?
- Write and read models not always are the same
- Multiple/optimized read models
- Number of reads != number of writes

Why Event Driven?
- Allow to react to internal or external events (extensibility)
- Real time architecture

Why Event Sourcing?
- Consistency and reliability
- Events are the single source of truth
- Business logic separation
- Adding projections easily
- Schema evolution can be a little easier (but always a pain!)
- Dedicated storage can be added if needed for a specific projection (Elasticsearch, MongoDb, Cassandra, ...)
- Easy auditing/replay of events

Why JVM?
- The official implementation of Kafka Streams is available only for Java

Why Scala?
- It supports Java but with a functional approach and less verbose

Why Kafka?
- Fast, scalable and reliable storage
- It can be used for both storage and message bus (Reduce infrastructure components)

Why Kafka Streams?
- Scalable (each instance works on a set of partition)
- Event driven architecture are very hard to implement, Kafka Streams makes it a little less harder
- "Easy" [exactly-once](https://www.confluent.io/blog/enabling-exactly-once-kafka-streams/) semantic with Kafka
- Advanced stream processing capabilities (join, aggregates, ...)
- disadvantages:
    - quite hard to find good examples
    - reset state can be difficult

Why Akka Http?
- Good framework for REST API with a rich ecosystem (Akka, Akka Stream, Alpakka, ...)
- We can use Akka also for Kafka ingestion and for distribution, see [Alpakka](https://doc.akka.io/docs/alpakka/current/index.html))
- Akka Stream can substitute Kafka Streams in certain scenarios

Why AVRO?
- Fast and compact serialization format

Why Kaa Schema Registry?
- Simple [schema registry](https://medium.com/slalom-technology/introduction-to-schema-registry-in-kafka-915ccf06b902) library with Kafka persistence
- It doesn't require and external service (less infrastructure to maintain)
- disadvantages:
    - to be tested in production for potential issues
    - schemas are available only in Kafka, but it should be easy to create an API to expose it via HTTP or similar


## Usage

Requirements:
- scala sbt
- OpenJDK 11 (64 bit)
- Docker (for integrations tests)
    - Docker-compose

Run unit tests:

```
sbt test
```

Run the sample app:

```
sbt sample/run
```

To use `es4kafka` in other project locally you can publish it to the local repository:

```
sbt publishLocal
```

HTTP RPC style API are available at: http://localhost:9081/

- `GET /authors/all` - gel all authors
- `GET /authors/one/{code}` - gel one author
- `POST /authors/commands` - send a command
    - request body: `CreateAuthor`/`UpdateAuthor`/`DeleteAuthor` class as json
    ```
    {
        "_type": "CreateAuthor",
        "code": "luke",
        "firstName": "Luke",
        "lastName": "Skywalker"
    }
    ```
    - response body: event

(see PostMan collection inside `./docs/books-catalog.postman_collection`)

## Credits and other references

Inspired by:

- Event Sourcing with Kafka Stream:
    - https://www.youtube.com/watch?v=b17l7LvrTco
    - https://github.com/amitayh/event-sourcing-kafka-streams
    - https://speakerdeck.com/amitayh/building-event-sourced-systems-with-kafka-streams
- Event sourcing with Kafka by Confluent:
    - https://www.confluent.io/blog/event-sourcing-cqrs-stream-processing-apache-kafka-whats-connection/
    - https://www.confluent.io/blog/event-sourcing-using-apache-kafka/
- Kafka Streams Interactive Queries with Akka Http:
    - https://sachabarbs.wordpress.com/2019/05/08/kafkastreams-interactive-queries/

