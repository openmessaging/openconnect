# openmessaging-connector

## Introduction
Open Messaging connector is a standard to connect between data sources and data destinations. Users could easily create connector instances with configurations via REST api.

There is two type of connector, source connector to read data and push them as message to corresponding queue, and sink connector to receive message from queue and load into data destinations.

Users should implement source or sink connector to run their specific job.

### Connector

Connector life cycle:

`onStart(KeyValue config)`
<br>When the connector starts. Do initialization here.
        

`onPause()`
<br>When connector is paused by REST api. Good place to release any resources.

`onStop()`
<br>When connector is stopped before restart.

`onFailed(Exception e)`
<br>When connector encounters error.

`onDestroyed()` 
<br>When connector is to be deleted.

### Source connector
`Collection<Message> poll()`
<br>Messages to be sent.

### Sink connector
`Collection<Message> put()`
<br>Messages received either by pushConsumer or pollConsumer.


## REST API

POST /connectors/{connector name}
*Create connector with configurations.*

GET /connectors/{connector name}
*Get connector's configurations.*

GET /connectors/{connector name}/status
*Get connector's status.*

PUT /connectors/{connector name}/pause
*Pause connector. This will block pushing or receiving of connectors.*

PUT /connectors/{connector name}/resume
*Resume connector.*

POST /connectors/{connector name}/restart
*Restart connector.*
   
DELETE /connectors/{connector name}
*Delete connector.*
