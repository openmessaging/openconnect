# OpenMessaging Connect

## Introduction
OpenMessaging Connect is a standard to connect between data sources and data destinations. Users could easily create connector instances with configurations via REST API.

There are two types of connectors: source connector and sink connector. A source connector is used for pulling data from a data source (e.g. RDBMS).
The data is sent to corresponding message queue and expected to be consumed by one or many sink connectors.
A sink connector receives message from the queue and loads into a data destination (e.g. data warehouse).
Developers should implement source or sink connector interface to run their specific job.

Usually, connectors rely on a concrete message queue for data transportation. The message queue decouples source connectors from sink connectors.
In the meantime, it provides capabilities such as failover, rate control and one to many data transportation etc.
Some message queues (e.g. Kafka) provide bundled connect frameworks and a various of connectors developed officially or by the community.
However, these frameworks are lack of interoperability, which means a connector developed for Kafka cannot run with 
RabbitMQ without modification and vice versa.

![dataflow](flow.png "dataflow")
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-connect.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-connect?ref=badge_shield)

A connector follows OpenMessaging Connect could run with any message queues which support OpenMessaging API.
OpenMessaging Connect provides a standalone runtime which uses OpenMessaging API for sending and consuming message,
as well as the key/value operations for offset management.



## Connector

`void verifyAndSetConfig(KeyValue config)`
<br>Should invoke before start the connector.

`void start()`
<br>Start the connector.

`void stop()`
<br>Stop the connector.

`void pause()`
<br>Pause the connector.

`void resume()`
<br>Resume the connector.

`Class<? extends Task> taskClass()`
<br>Returns the Task implementation for this Connector.

`List<KeyValue> taskConfigs()`
<br>Returns a set of configurations for Task based on the current configuration.

## Task

`void start(KeyValue config)`
<br>Start the task with the given config.

`void stop()`
<br>Stop the task.

`void pause()`
<br>Pause the task.

`void resume()`
<br>Resume the task.

### Source task

`Collection<SourceDataEntry> poll()`
<br>Return a collection of message entries to send.

### Sink task

`void put(Collection<SinkDataEntry> message)`
<br>Put the data entries to the sink.




## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-connect.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fopenmessaging%2Fopenmessaging-connect?ref=badge_large)