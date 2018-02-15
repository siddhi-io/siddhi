# Siddhi Streaming SQL Guide 4.0

## Introduction

Siddhi Streaming SQL is designed to process event streams in a streaming manner, detect complex event occurrences, 
and notify them in real-time. 

## Siddhi Application
Streaming processing and Complex Event Processing rules can be written is Siddhi Streaming SQL and they can be put 
together as a `SiddhiApp` in a single file. 

**Purpose**

Each Siddhi Application is an isolated processing unit that allows you to deploy and execute queries independent of other Siddhi applications in the system.

The following diagram depicts how **event flows** work with some of the key Siddhi Streaming SQL elements 
of the Siddhi Application.

![Event Flow](../images/event-flow.png?raw=true "Event Flow")

Below table provides brief description of a few key elements in the Siddhi Streaming SQL Language.

| Elements     | Description |
| ------------- |-------------|
| Stream    | A logical series of events ordered in time with a uniquely identifiable name, and set of defined attributes with specific data types defining its schema. |
| Event     | An event is associated with only one stream, and all events of that stream have an identical set of attributes that are assigned specific types (or the same schema). An event contains a timestamp and set of attribute values according to the schema.|
| Table     | A structured representation of data stored with a defined schema. Stored data can be backed by `In-Memory`, `RDBMs`, `MongoDB`, etc. to be accessed and manipulated at runtime.
| Query	    | A logical construct that processes events in streaming manner by combining existing streams and/or tables, and generates events to an output stream or table. A query consumes one or more input streams, and zero or one table. Then it processes these events in a streaming manner and publishes the output events to streams or tables for further processing or to generate notifications. 
| Source    | A contract that consumes data from external sources (such as `TCP`, `Kafka`, `HTTP`, etc)in the form of events, then converts each event (which can be in `XML`, `JSON`, `binary`, etc. format) to a Siddhi event, and passes that to a Stream for processing.
| Sink      | A contract that takes events arriving at a stream, maps them to a predefined data format (such as `XML`, `JSON`, `binary`, etc), and publishes them to external endpoints (such as `E-mail`, `TCP`, `Kafka`, `HTTP`, etc).
| Input Handler | A mechanism to programmatically inject events into streams. |
| Stream/Query Callback | A mechanism to programmatically consume output events from streams and queries. |
| Partition	| A logical container that isolates the processing of queries based on partition keys. Here, a separate instance of queries is generated for each partition key to achieve isolation. 
| Inner Stream | A positionable stream that connects portioned queries within their partitions, preserving isolation.  

**Grammar**

An element of Siddhi SQL can be composed together as a script in a Siddhi application, Here each construct must be separated 
by a semicolon `( ; )` as shown in the below syntax. 

```
<siddhi app>  : 
        <app annotation> * 
        ( <stream definition> | <table definition> | ... ) + 
        ( <query> | <partition> ) +
        ;
```

**Example**
Siddhi Application named `Temperature-Analytics` defined with a stream named `TempStream` and a query 
named `5minAvgQuery` for processing it.

```sql
@app:name('Temperature-Analytics')

define stream TempStream (deviceID long, roomNo int, temp double);

@name('5minAvgQuery')
from TempStream#window.time(5 min)
select roomNo, avg(temp) as avgTemp
  group by roomNo
insert into OutputStream;
```

## Stream
A stream is a logical series of events ordered in time. Its schema is defined via the **stream definition**.
A stream definition contains a unique name and a set of attributes with specific types and uniquely identifiable names within the stream.
All the events that are selected to be received into a specific stream have the same schema (i.e., have the same attributes in the same order). 

**Purpose**

By defining a schema it unifies common types of events together. This enables them to be processed via queries using their defined attributes in a streaming manner, and allow sinks and sources to map events to/from various data formats.

**Syntax**

The syntax for defining a new stream is as follows.
```sql
define stream <stream name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );
```
The following parameters are configured in a stream definition.

| Parameter     | Description |
| ------------- |-------------|
| `stream name`      | The name of the stream created. (It is recommended to define a stream name in `PascalCase`.) |
| `attribute name`   | The schema of an stream is defined by its attributes with uniquely identifiable attribute names. (It is recommended to define attribute names in `camalCase`.)|    |
| `attribute type`   | The type of each attribute defined in the schema. <br/> This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOL` or `OBJECT`.     |

To improve the throughput of a stream, you can add the `@Async` annotation as shown in the extract below.

```sql
@app:Async(buffer.size = '1024') define stream <stream name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );
```
This annotation adds a disruptor to allow events in the stream to be processed in parallel via multiple threads. You can specify the number of events to be kept in the buffer before they are directed to the threads to be processed in parallel. This is done via the `buffer.size` parameter.

**Example**
```sql
define stream TempStream (deviceID long, roomNo int, temp double);
```
The above creates a stream named `TempStream` with the following attributes.

+ `deviceID` of type `long`
+ `roomNo` of type `int` 
+ `temp` of type `double` 

### Source
Sources receive events via multiple transports and in various data formats, and direct them into streams for processing.

A source configuration allows you to define a mapping in order to convert each incoming event from its native data format to a Siddhi event. When customizations to such mappings are not provided, Siddhi assumes that the arriving event adheres to the predefined format based on the stream definition and the selected message format. </br>

**Purpose**

Source allows Siddhi to consume events from external systems, and map the events to adhere to the associated stream. 

**Syntax**

To configure a stream that consumes events via a source, add the source configuration to a stream definition by adding the `@source` annotation with the required parameter values. 
The source syntax is as follows:
```sql
@source(type='source_type', static.option.key1='static_option_value1', static.option.keyN='static_option_valueN',
    @map(type='map_type', static.option_key1='static_option_value1', static.option.keyN='static_option_valueN',
        @attributes( attributeN='attribute_mapping_N', attribute1='attribute_mapping_1')
    )
)
define stream stream_name (attribute1 Type1, attributeN TypeN);
```
This syntax includes the following annotations.
**Source**

The `type` parameter of `@source` defines the source type that receives events. The other parameters to be configured 
depends on the source type selected, some of the the parameters are optional. </br>

For detailed information about the parameters see the documentation for the relevant source.

The following is the list of source types that are currently supported:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-http/">HTTP</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/">Kafka</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-tcp/">TCP</a>
* In-memory
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-wso2event/">WSO2Event</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/">Email</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/">JMS</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-file/">File</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-rabbitmq/">RabbitMQ</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-mqtt/">MQTT</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-websocket/">WebSocket</a>

**Source Mapper**

Each `@source` configuration has a mapping denoted by the `@map` annotation that converts the incoming messages format to Siddhi events.

The `type` parameter of the `@map` defines the map type to be used to map the data. The other parameters to be 
configured depends on the mapper selected. Some of these parameters are optional. </br>
For detailed information about the parameters see the documentation for the relevant mapper.

!!! tip 
    When the `@map` annotation is not provided, `@map(type='passThrough')` is used as default. This default mapper type can be used when source consumes Siddhi events and when it does not need any mappings.
    

**Map Attributes**

`@attributes` is an optional annotation used with `@map` to define custom mapping. When `@attributes` is not provided, each mapper
assumes that the incoming events  adhere to its own default data format. By adding the `@attributes` annotation, you 
can configure mappers to extract data from the incoming message selectively, and assign them to attributes. 

There are two ways you can configure map attributes. 

1. Defining attributes as keys and mapping content as values in the following format: <br/>
```@attributes( attributeN='mapping_N', attribute1='mapping_1')``` 
2. Defining the mapping content of all attributes in the same order as how the attributes are defined in stream definition: <br/>
```@attributes( 'mapping_1', 'mapping_N')``` 

**Supported Mapping Types**

The following is a list of currently supported source mapping types:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-wso2event/">WSO2Event</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-xml/">XML</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/">TEXT</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/">JSON</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-binary/">Binary</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/">Key Value</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-csv/">CSV</a>

**Example**

This query receives events via the `HTTP` source in the `JSON` data format, and directs them to the `InputStream` stream for processing. 
Here the HTTP source is configured to receive events on all network interfaces on the `8080`port, on the `foo` context, and 
it is secured via basic authentication.

```sql
@source(type='http', receiver.url='http://0.0.0.0:8080/foo', is.basic.auth.enabled='true', 
  @map(type='json'))
define stream InputStream (name string, age int, country string);
```
### Sink

Sinks publish events from the streams via multiple transports to external endpoints in various data formats.

A sink configuration allows you to define a mapping to convert the Siddhi event to the required output data format (such as `JSON`, `TEXT`, `XML`, etc.).
When customization to such mappings is not provided, Siddhi converts events to its default format based on the stream definition and 
the selected data format to publish the events.

**Purpose**

Sinks provide a way to publish Siddhi events to external systems in the preferred data format. 

**Syntax**

To configure a stream to publish events via a sink, add the sink configuration to a stream definition by adding the `@sink` 
annotation with the required parameter values. The sink syntax is as follows:

```sql
@sink(type='sink_type', static_option_key1='static_option_value1', dynamic_option_key1='{{dynamic_option_value1}}',
    @map(type='map_type', static_option_key1='static_option_value1', dynamic_option_key1='{{dynamic_option_value1}}',
        @payload('payload_mapping')
    )
)
define stream stream_name (attribute1 Type1, attributeN TypeN);
```

!!! Note "Dynamic Properties" 
    The sink and sink mapper properties that are categorized as `dynamic` have the ability to absorb attributes values 
    from their associated streams. This can be done by using the attribute names in double curly braces as `{{...}}` when configuring the property value. 
    
    Some valid dynamic properties values are: 
    
    * `'{{attribute1}}'`
    * `'This is {{attribute1}}'` 
    * `{{attribute1}} > {{attributeN}}`  
    
    Here the attribute names in the double curly braces will be replaced with event values during execution. 

This syntax includes the following annotations.

**Sink**

The `type` parameter of the `@sink` annotation defines the sink type that publishes the events. The other parameters to be configured 
depends on the sink type selected. Some of these parameters are optional, and some can have dynamic values. </br>

For detailed information about the parameters see documentation for the relevant sink.

The following is a list of currently supported sink types.

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-http/">HTTP</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/">Kafka</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-tcp/">TCP</a>
* In-memory
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-wso2event/">WSO2Event</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/">Email</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/">JMS</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-file/">File</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-rabbitmq/">RabbitMQ</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-mqtt/">MQTT</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-websocket/">WebSocket</a>


**Sink Mapper**

Each `@sink` annotation has a mapping denoted by the  `@map` annotation that converts the Siddhi event to an outgoing message format.

The `type` parameter of the `@map` annotation defines the map type based on which the event is mapped. The other parameters to be configured depends on the mapper selected. Some of these parameters are optional and some have dynamic values. </br> 

For detailed information about the parameters see the documentation for the relevant mapping type.

!!! tip 
    When the `@map` annotation is not provided, `@map(type='passThrough')` is used by default. This can be used when the sink publishes in the Siddhi event format, or when it does not need any mappings.

**Map Payload**

`@payload` is an optional annotation used with the `@map` annotation to define a custom mapping. When the `@payload` annotation is not provided, each mapper
maps the outgoing events to its own default data format. By defining the `@payload` annotation you can configure mappers to produce the output payload with attribute names of your choice, using dynamic properties by selectively assigning 
the attributes in your preferred format. 

There are two ways you can configure the `@payload` annotation. 

1. Some mappers such as `XML`, `JSON`, and `Test` accept only one output payload using the following format: <br/>
```@payload( 'This is a test message from {{user}}.' )``` 
2. Some mappers such `key-value` accept series of mapping values defined as follows: <br/>
```@payload( key1='mapping_1', key2='user : {{user}}')``` 

**Supported Mapping Types**

The following is a list of currently supported sink mapping types:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-wso2event/">WSO2Event</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-xml/">XML</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/">TEXT</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/">JSON</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-binary/">Binary</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/">Key Value</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-csv/">CSV</a>


**Example**

This query publishes events from the `OutputStream` stream via the `HTTP` sink. Here the events are mapped to the default `JSON` payloads and sent to `http://localhost:8005/endpoint`
 using the `POST` method, with the`Accept` header, and secured via basic authentication where `admin` is both the username and the password.
```sql
@sink(type='http', publisher.url='http://localhost:8005/endpoint', method='POST', headers='Accept-Date:20/02/2017', 
  basic.auth.username='admin', basic.auth.password='admin', basic.auth.enabled='true',
  @map(type='json'))
define stream OutputStream (name string, ang int, country string);
```

## Query

Each Siddhi query can consume one or more streams, and 0-1 tables, process the events in a streaming manner, and then generate an
 output event to a stream or perform a CRUD operation to a table.

**Purpose**

A query enables you to perform complex event rrocessing and stream processing operations by processing incoming events one by one in the order they arrive.

**Syntax**

All queries contain an input and an output section. Some also contain a projection section. A simple query with all three sections is as follows.

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
insert into <output stream/table>
```
**Example**

This query included in a Siddhi Application consumes events from the `TempStream` stream (that is already defined) and outputs the room temperature and the room number to the `RoomTempStream` stream.

```sql
define stream TempStream (deviceID long, roomNo int, temp double);

from TempStream 
select roomNo, temp
insert into RoomTempStream;
```
!!! tip "Inferred Stream"
    Here, the `RoomTempStream` is an inferred Stream, which means it can be used as any other defined stream 
    without explicitly defining its stream definition. The definition of the `RoomTempStream` is inferred from the 
    first query that produces the stream.  

###Query Projection

Siddhi queries supports the following for query projections.

<table style="width:100%">
    <tr>
        <th>Action</th>
        <th>Description</th>
    </tr>
    <tr>
        <td>Selecting required objects for projection</td>
        <td>This involves selecting only some of the attributes from the input stream to be inserted into an output stream.
            <br><br>
            E.g., The following query selects only the `roomNo` and `temp` attributes from the `TempStream` stream.
            <pre style="align:left">from TempStream<br>select roomNo, temp<br>insert into RoomTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Selecting all attributes for projection</td>
        <td>Selecting all the attributes in an input stream to be inserted into an output stream. This can be done by using asterisk ( * ) or by omitting the `select` statement.
            <br><br>
            E.g., Both the following queries select all the attributes in the `NewTempStream` stream.
            <pre>from TempStream<br>select *<br>insert into NewTempStream;</pre>
            or
            <pre>from TempStream<br>insert into NewTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Renaming attributes</td>
        <td>This selects attributes from the input streams and inserts them into the output stream with different names.
            <br><br>
            E.g., This query renames `roomNo` to `roomNumber` and `temp` to `temperature`.
            <pre>from TempStream <br>select roomNo as roomNumber, temp as temperature<br>insert into RoomTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Introducing the constant value</td>
        <td>This adds constant values by assigning it to an attribute using `as`.
            <br></br>
            E.g., This query specifies 'C' to be used as the constant value for `scale` attribute. 
            <pre>from TempStream<br>select roomNo, temp, 'C' as scale<br>insert into RoomTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Using mathematical and logical expressions</td>
        <td>This uses attributes with mathematical and logical expressions in the precedence order given below, and assigns them to the output attribute using `as`.
            <br><br>
            <b>Operator precedence</b><br>
            <table style="width:100%">
                <tr>
                    <th>Operator</th>
                    <th>Distribution</th>
                    <th>Example</th>
                </tr>
                <tr>
                    <td>
                        ()
                    </td>
                    <td>
                        Scope
                    </td>
                    <td>
                        <pre>(cost + tax) * 0.05</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                         IS NULL
                    </td>
                    <td>
                        Null check
                    </td>
                    <td>
                        <pre>deviceID is null</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        NOT
                    </td>
                    <td>
                        Logical NOT
                    </td>
                    <td>
                        <pre>not (price > 10)</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                         *   /   %  
                    </td>
                    <td>
                        Multiplication, division, modulo
                    </td>
                    <td>
                        <pre>temp * 9/5 + 32</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        +   -  
                    </td>
                    <td>
                        Addition, substraction
                    </td>
                    <td>
                        <pre>temp * 9/5 - 32</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        <   <=   >   >=
                    </td>
                    <td>
                        Comparators: less-than, greater-than-equal, greater-than, less-than-equal
                    </td>
                    <td>
                        <pre>totalCost >= price * quantity</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        ==   !=  
                    </td>
                    <td>
                        Comparisons: equal, not equal
                    </td>
                    <td>
                        <pre>totalCost !=  price * quantity</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        IN
                    </td>
                    <td>
                        Contains in table
                    </td>
                    <td>
                        <pre>roomNo in ServerRoomsTable</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        AND
                    </td>
                    <td>
                        Logical AND
                    </td>
                    <td>
                        <pre>temp < 40 and (humidity < 40 or humidity >= 60)</pre>
                    </td>
                </tr>
                <tr>
                    <td>
                        OR
                    </td>
                    <td>
                        Logical OR
                    </td>
                    <td>
                        <pre>temp < 40 or (humidity < 40 and humidity >= 60)</pre>
                    </td>
                </tr>
            </table>
            E.g., Converting Celsius to Fahrenheit and identifying rooms with room number between 10 and 15 as server rooms.
            <pre>from TempStream<br>select roomNo, temp * 9/5 + 32 as temp, 'F' as scale, roomNo > 10 and roomNo < 15 as isServerRoom<br>insert into RoomTempStream;</pre>       
    </tr>
    
</table>

###Function

A function consumes zero, one or more parameters and always produces a result value. It can be used in any location where
 an attribute can be used. 

**Purpose**

Functions encapsulates complex execution logic that makes Siddhi applications simple and easy to understand. 

**Function Parameters**

Functions parameters can be attributes, constant values, results of other functions, results of mathematical or logical expressions or time parameters. 
Function parameters vary depending on the function being called.

Time is a special parameter that can be defined using the integer time value followed by its unit as `<int> <unit>`. 
Following are the supported unit types. Upon execution, time returns the value in the scale of milliseconds as a long value. 

<table style="width:100%">
    <tr>
        <th>
            Unit  
        </th>
        <th>
            Syntax
        </th>
    </tr>
    <tr>
        <td>
            Year
        </td>
        <td>
            year | years
        </td>
    </tr>
    <tr>
        <td>
            Month
        </td>
        <td>
            month | months
        </td>
    </tr>
    <tr>
        <td>
            Week
        </td>
        <td>
            week | weeks
        </td>
    </tr>
    <tr>
        <td>
            Day
        </td>
        <td>
            day | days
        </td>
    </tr>
    <tr>
        <td>
            Hour
        </td>
        <td>
           hour | hours
        </td>
    </tr>
    <tr>
        <td>
           Minutes
        </td>
        <td>
           minute | minutes | min
        </td>
    </tr>
    <tr>
        <td>
           Seconds
        </td>
        <td>
           second | seconds | sec
        </td>
    </tr>
    <tr>
        <td>
           Milliseconds
        </td>
        <td>
           millisecond | milliseconds
        </td>
    </tr>
</table>

E.g. Passing 1 hour and 25 minutes to `test()` function.

<pre>test(1 hour 25 min)</pre>

!!! note
    Functions, mathematical expressions, and logical expressions can be used in a nested manner.

Following are some inbuilt functions shipped with Siddhi, for more functions refer execution <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>.

+ eventTimestamp
+ log
+ UUID
+ default
+ cast
+ convert
+ ifThenElse
+ minimum
+ maximum
+ coalesce
+ instanceOfBoolean
+ instanceOfDouble
+ instanceOfFloat
+ instanceOfInteger
+ instanceOfLong
+ instanceOfString

**Example**

The following configuration converts the `roomNo` to `string` and adds a `messageID` to each event using the `convert` and `UUID` functions.
```sql
from TempStream
select convert(roomNo, 'string') as roomNo, temp, UUID() as messageID
insert into RoomTempStream;
```

### Filter

Filters are included in queries to filter information from input streams based on a specified condition.

**Purpose**

A filter allows you to separate events that match a specific condition as the output, or for further processing.

**Syntax**

Filter conditions should be defined in square brackets next to the input stream name as shown below.

```sql
from <input stream>[<filter condition>]
select <attribute name>, <attribute name>, ...
insert into <output stream>
```

**Example**

This query filters all server rooms of which the room number is within the range of 100-210, and having temperature greater than 40 degrees 
from the `TempStream` stream, and inserts the results into the `HighTempStream` stream.

```sql
from TempStream[(roomNo >= 100 and roomNo < 210) and temp > 40]
select roomNo, temp
insert into HighTempStream;
```

### Window

Windows allow you to capture a subset of events based on a specific criterion from an input stream for calculation. 
Each input stream can only have a maximum of one window.

**Purpose**

To create subsets of events within a stream based on time duration, number of events, etc for processing. 
A window can operate in a sliding or tumbling (batch) manner.

**Syntax**

The `#window` prefix should be inserted next to the relevant stream in order to use a window.

```sql
from <input stream>#window.<window name>(<parameter>, <parameter>, ... )
select <attribute name>, <attribute name>, ...
insert <event type> into <output stream>
```
!!! note 
    Filter condition can be applied both before and/or after the window
    
**Example**

If you want to identify the maximum temperature out of the last 10 events, you need to define a `length` window of 10 events.
 This window operates in a sliding mode where the following 3 subsets are calculated when a list of 12 events are received in a sequential order.

|Subset|Event Range|
|------|-----------|
| 1 | 1-10 |
| 2 | 2-11 |
|3| 3-12 |

The following query finds the maximum temperature out of **last 10 events** from the `TempStream` stream, 
and inserts the results into the `MaxTempStream` stream.

```sql
from TempStream#window.length(10)
select max(temp) as maxTemp
insert into MaxTempStream;
```

If you define the maximum temperature reading out of every 10 events, you need to define a `lengthBatch` window of 10 events.
This window operates as a batch/tumbling mode where the following 3 subsets are calculated when a list of 30 events are received in a sequential order.

|Subset|Event Range|
|------|-----------|
| 1    | 1-10      |
| 2    | 11-20     |
| 3    | 21-30     |

The following query finds the maximum temperature out of **every 10 events** from the `TempStream` stream, 
and inserts the results into the `MaxTempStream` stream.

```sql
from TempStream#window.lengthBatch(10)
select max(temp) as maxTemp
insert into MaxTempStream;
```

!!! note
    Similar operations can be done based on time via `time` windows and `timeBatch` windows and for others. 
    Code segments such as `#window.time(10 min)` considers events that arrive during the last 10 minutes in a sliding manner, and the `#window.timeBatch(2 min)` considers events that arrive every 2 minutes in a tumbling manner. 

Following are some inbuilt windows shipped with Siddhi. For more window types, see execution <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>. 

* time
* timeBatch
* timeLength
* length
* lengthBatch
* sort
* frequent
* lossyFrequent
* cron
* externalTime
* externalTimeBatch

**Output event types**<a id="output-event-types" class='anchor' aria-hidden='true'></a> 

Projection of the query depends on the output event types such as, `current` and `expired` event types.
 By default all queries produce `current` events and only queries with windows produce `expired` events 
 when events expire from the window. You can specify whether the output of a query should be only current events, only expired events or both current and expired events.
 
 **Note!** Controlling the output event types does not alter the execution within the query, and it does not affect the accuracy of the query execution.  
 
 The following keywords can be used with the output stream to manipulate output. 
 
| Output event types | Description |
|-------------------|-------------|
| `current events` | Outputs events when incoming events arrive to be processed by the query. </br> This is default when no specific output event type is specified.|
| `expired events` | Outputs events when events expires from the window. |
| `all events` | Outputs events when incoming events arrive to be processed by the query as well as </br> when events expire from the window. |

The output event type keyword can be used between `insert` and `into` as shown in the following example.

**Example**

This query delays all events in a stream by 1 minute.  

```sql
from TempStream#window.time(1 min)
select *
insert expired events into DelayedTempStream
```

### Aggregate function

Aggregate functions perform aggregate calculations in the query. 
When a window is defined the aggregation is restricted within that window. If no window is provided aggregation is performed from the start of the Siddhi application.

**Syntax**

```sql
from <input stream>#window.<window name>(<parameter>, <parameter>, ... )
select <aggregate function>(<parameter>, <parameter>, ... ) as <attribute name>, <attribute2 name>, ...
insert into <output stream>;
```

**Aggregate Parameters**

Aggregate parameters can be attributes, constant values, results of other functions or aggregates, results of mathematical or logical expressions, or time parameters. 
Aggregate parameters configured in a query  depends on the aggregate function being called.

**Example**

The following query calculates the average value for the `temp` attribute of the `TempStream` stream. This calculation is done for the last 10 minutes in a sliding manner, and the result is output as `avgTemp` to the `AvgTempStream` output stream.

```sql
from TempStream#window.time(10 min)
select avg(temp) as avgTemp, roomNo, deviceID
insert into AvgTempStream;
```
Following are some inbuilt aggregation functions shipped with Siddhi, for more aggregation functions, see execution <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>. 

* avg
* sum
* max
* min
* count
* distinctCount
* maxForever
* minForever
* stdDev

### Group By

Group By allows you to group the aggregate based on specified attributes.

**Syntax**
The syntax for the Group By aggregate function is as follows:

```sql
from <input stream>#window.<window name>(...)
select <aggregate function>( <parameter>, <parameter>, ...) as <attribute1 name>, <attribute2 name>, ...
group by <attribute1 name>, <attribute2 name> ...
insert into <output stream>;
```

**Example**
The following query calculates the average temperature per `roomNo` and `deviceID` combination, for events that arrive at the `TempStream` stream
for a sliding time window of 10 minutes.

```sql
from TempStream#window.time(10 min)
select avg(temp) as avgTemp, roomNo, deviceID
group by roomNo, deviceID
insert into AvgTempStream;
```

### Having

Having allows you to filter events after processing the `select` statement.

**Purpose**
This allows you to filter the aggregation output.

**Syntax**
The syntax for the Having aggregate function is as follows:

```sql
from <input stream>#window.<window name>( ... )
select <aggregate function>( <parameter>, <parameter>, ...) as <attribute1 name>, <attribute2 name>, ...
group by <attribute1 name>, <attribute2 name> ...
having <condition>
insert into <output stream>;
```

**Example**

The following query calculates the average temperature per room for the last 10 minutes, and alerts if it exceeds 30 degrees.
```sql
from TempStream#window.time(10 min)
select avg(temp) as avgTemp, roomNo
group by roomNo
having avgTemp > 30
insert into AlertStream;
```

### Join (Stream) 
Joins allow you to get a combined result from two streams in real-time based on a specified condition. 

**Purpose**
Streams are stateless. Therefore, in order to join two streams, they need to be connected to a window so that there is a pool of events that can be used for joining. Joins also accept conditions to join the appropriate events from each stream.
 
During the joining process each incoming event of each stream is matched against all the events in the other 
stream's window based on the given condition, and the output events are generated for all the matching event pairs.

!!! Note
    Join can also be performed with [stored data](#join-table), [aggregation](#join-aggregation) or externally [defined windows](#join-window).

**Syntax**

The syntax for a join is as follows:

```sql
from <input stream>#window.<window name>(<parameter>, ... ) {unidirectional} {as <reference>}
         join <input stream>#window.<window name>(<parameter>,  ... ) {unidirectional} {as <reference>}
    on <join condition>
select <attribute name>, <attribute name>, ...
insert into <output stream>
```
Here, the `<join condition>` allows you to match the attributes from both the streams. 

**Unidirectional join operation**

By default, events arriving at either stream can trigger the joining process. However, if you want to control the 
join execution, you can add the `unidirectional` keyword next to a stream in the join definition as depicted in the 
syntax in order to enable that stream to trigger the join operation. Here, events arriving at other stream only update the 
 window of that stream, and this stream does not trigger the join operation.
 
!!! Note
    The `unidirectional` keyword cannot be applied to both the input streams because the default behaviour already allows both streams to trigger the join operation.

**Example**

Assuming that the temperature of regulators are updated every minute. 
Following is a Siddhi App that controls the temperature regulators if they are not already `on` for all the rooms with a room temperature greater than 30 degrees.  

```sql
define stream TempStream(deviceID long, roomNo int, temp double);
define stream RegulatorStream(deviceID long, roomNo int, isOn bool);
  
from TempStream[temp > 30.0]#window.time(1 min) as T
  join RegulatorStream[isOn == false]#window.length(1) as R
  on T.roomNo == R.roomNo
select T.roomNo, R.deviceID, 'start' as action
insert into RegulatorActionStream;
```

**Supported join types** 

Following are the supported operations of a join clause.

 *  **Inner join (join)** 

    This is the default behaviour of a join operation. `join` is used as the keyword to join both the streams. The output is generated only if there is a matching event in both the streams.

 *  **Left outer join** 

    The left outer join operation allows you to join two streams to be merged based on a condition. `left outer join` is used as the keyword to join both the streams.
   
    Here, it returns all the events of left stream even if there are no matching events in the right stream by 
    having null values for the attributes of the right stream.

     **Example**

    The following query generates output events for all events from the `StockStream` stream regardless of whether a matching 
    symbol exists in the `TwitterStream` stream or not.

    <pre>
    from StockStream#window.time(1 min) as S
      left outer join TwitterStream#window.length(1) as T
      on S.symbol== T.symbol
    select S.symbol as symbol, T.tweet, S.price
    insert into outputStream ;    </pre>

 *  **Right outer join** 

    This is similar to a left outer join. `Right outer join` is used as the keyword to join both the streams.
    It returns all the events of the right stream even if there are no matching events in the left stream. 

 *  **Full outer join** 

    The full outer join combines the results of left outer join and right outer join. `full outer join` is used as the keyword to join both the streams.
    Here, output event are generated for each incoming event even if there are no matching events in the other stream.

    **Example**

    The following query generates output events for all the incoming events of each stream regardless of whether there is a 
    match for the `symbol` attribute in the other stream or not.

    <pre>
    from StockStream#window.time(1 min) as S
      full outer join TwitterStream#window.length(1) as T
      on S.symbol== T.symbol
    select S.symbol as symbol, T.tweet, S.price
    insert into outputStream ;    </pre>



### Pattern

This is a state machine implementation that allows you to detect patterns in the events that arrive over time. This can correlate events within a single stream or between multiple streams. 

**Purpose** 

Patterns allow you to identify trends in events over a time period.

**Syntax**

The following is the syntax for a pattern query:

```sql
from (every)? <event reference>=<input stream>[<filter condition>] -> 
    (every)? <event reference>=<input stream [<filter condition>] -> 
    ... 
    (within <time gap>)?     
select <event reference>.<attribute name>, <event reference>.<attribute name>, ...
insert into <output stream>
```
| Items| Description |
|-------------------|-------------|
| `->` | This is used to indicate an event that should be following another event. The subsequent event does not necessarily have to occur immediately after the preceding event. The condition to be met by the preceding event should be added before the sign, and the condition to be met by the subsequent event should be added after the sign. |
| `<event reference>` | This allows you to add a reference to the the matching event so that it can be accessed later for further processing. |
| `(within <time gap>)?` | The `within` clause is optional. It defines the time duration within which all the matching events should occur. |
| `every` | `every` is an optional keyword. This defines whether the event matching should be triggered for every event arrival in the specified stream with the matching condition. <br/> When this keyword is not used, the matching is carried out only once. |

Siddhi also supports pattern matching with counting events and matching events in a logical order such as (`and`, `or`, and `not`). These are described in detail further below in this guide.

**Example**

This query sends an alert if the temperature of a room increases by 5 degrees within 10 min.

```sql
from every( e1=TempStream ) -> e2=TempStream[ e1.roomNo == roomNo and (e1.temp + 5) <= temp ]
    within 10 min
select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
```

Here, the matching process begins for each event in the `TempStream` stream (because `every` is used with `e1=TempStream`), 
and if  another event arrives within 10 minutes with a value for the `temp` attribute that is greater than or equal to `e1.temp + 5` 
of the event e1, an output is generated via the `AlertStream`.

####Counting Pattern

Counting patterns allow you to match multiple events that may have been received for the same matching condition.
The number of events matched per condition can be limited via condition postfixes.

**Syntax**

Each matching condition can contain a collection of events with the minimum and maximum number of events to be matched as shown in the syntax below. 

```sql
from (every)? <event reference>=<input stream>[<filter condition>] (<<min count>:<max count>>)? ->  
    ... 
    (within <time gap>)?     
select <event reference>([event index])?.<attribute name>, ...
insert into <output stream>
```

|Postfix|Description|Example
---------|---------|---------
|`<n1:n2>`|This matches `n1` to `n2` events (including `n1` and not more than `n2`).|`1:4` matches 1 to 4 events.
|`<n:>`|This matches `n` or more events (including `n`).|`<2:>` matches 2 or more events.
|`<:n>`|This matches up to `n` events (excluding `n`).|`<:5>` matches up to 5 events.
|`<n>`|This matches exactly `n` events.|`<5>` matches exactly 5 events.

Specific occurrences of the event in a collection can be retrieved by using an event index with its reference.
Square brackets can be used to indicate the event index where `1` can be used as the index of the first event and `last` can be used as the index
 for the `last` available event in the event collection. If you provide an index greater then the last event index,
 the system returns `null`. The following are some valid examples.

+ `e1[3]` refers to the 3rd event.
+ `e1[last]` refers to the last event.
+ `e1[last - 1]` refers to the event before the last event.

**Example**

The following Siddhi App calculates the temperature difference between two regulator events.

```sql
define stream TempStream (deviceID long, roomNo int, temp double);
define stream RegulatorStream (deviceID long, roomNo int, tempSet double, isOn bool);
  
from every( e1=RegulatorStream) -> e2=TempStream[e1.roomNo==roomNo]<1:> -> e3=RegulatorStream[e1.roomNo==roomNo]
select e1.roomNo, e2[0].temp - e2[last].temp as tempDiff
insert into TempDiffStream;
```
#### Logical Patterns

Logical patterns match events that arrive in temporal order and correlate them with logical relationships such as `and`, 
`or` and `not`. 

**Syntax**

```sql
from (every)? (not)? <event reference>=<input stream>[<filter condition>] 
          ((and|or) <event reference>=<input stream>[<filter condition>])? (within <time gap>)? ->  
    ... 
select <event reference>([event index])?.<attribute name>, ...
insert into <output stream>
```

Keywords such as `and`, `or`, or `not` can be used to illustrate the logical relationship.

Key Word|Description
---------|---------
`and`|This allows both conditions of `and` to be matched by two events in any order.
`or`|The state succeeds if either condition of `or` is satisfied. Here the event reference of the other condition is `null`.
`not <condition1> and <condition2>`| When `not` is included with `and`, it identifies the events that match <condition2> arriving before any event that match <condition1>. 
`not <condition> for <time period>`| When `not` is included with `for`, it allows you to identify a situation where no event that matches `<condition1>` arrives during the specified `<time period>`.  e.g.,`from not TemperatureStream[temp > 60] for 5 sec`. 

Here the `not` pattern can be followed by either an `and` clause or the effective period of `not` can be concluded after a given `<time period>`. Further in Siddhi more than two streams cannot be matched with logical conditions using `and`, `or`, or `not` clauses at this point.

**Example**

Following Siddhi App, sends the `stop` control action to the regulator when the key is removed from the hotel room. 
```sql
define stream RegulatorStateChangeStream(deviceID long, roomNo int, tempSet double, action string);
define stream RoomKeyStream(deviceID long, roomNo int, action string);

  
from every( e1=RegulatorStateChangeStream[ action == 'on' ] ) -> 
      e2=RoomKeyStream[ e1.roomNo == roomNo and action == 'removed' ] or e3=RegulatorStateChangeStream[ e1.roomNo == roomNo and action == 'off']
select e1.roomNo, ifThenElse( e2 is null, 'none', 'stop' ) as action
having action != 'none'
insert into RegulatorActionStream;
```

This Siddhi Application generates an alert if we have switch off the regulator before the temperature reaches 12 degrees.  

```sql
define stream RegulatorStateChangeStream(deviceID long, roomNo int, tempSet double, action string);
define stream TempStream (deviceID long, roomNo int, temp double);

from e1=RegulatorStateChangeStream[action == 'start'] -> not TempStream[e1.roomNo == roomNo and temp < 12] and e2=RegulatorStateChangeStream[action == 'off']
select e1.roomNo as roomNo
insert into AlertStream;
```

This Siddhi Application generates an alert if the temperature does not reduce to 12 degrees within 5 minutes of switching on the regulator.  

```sql
define stream RegulatorStateChangeStream(deviceID long, roomNo int, tempSet double, action string);
define stream TempStream (deviceID long, roomNo int, temp double);

from e1=RegulatorStateChangeStream[action == 'start'] -> not TempStream[e1.roomNo == roomNo and temp < 12] for '5 min'
select e1.roomNo as roomNo
insert into AlertStream;
```


### Sequence

Sequence is a state machine implementation that allows you to detect the sequence of event occurrences over time. 
Here **all matching events need to arrive consecutively** to match the sequence condition, and there cannot be any non-matching events arriving within a matching sequence of events.
This can correlate events within a single stream or between multiple streams. 

**Purpose** 

This allows you to detect a specified event sequence over a specified time period. 

**Syntax**

The syntax for a sequence query is as follows:

```sql
from (every)? <event reference>=<input stream>[<filter condition>], 
    <event reference>=<input stream [<filter condition>], 
    ... 
    (within <time gap>)?     
select <event reference>.<attribute name>, <event reference>.<attribute name>, ...
insert into <output stream>
```

| Items | Description |
|-------------------|-------------|
| `,` | This represents the immediate next event i.e., when an event that matches the first condition arrives, the event that arrives immediately after it should match the second condition. |
| `<event reference>` | This allows you to add a reference to the the matching event so that it can be accessed later for further processing. |
| `(within <time gap>)?` | The `within` clause is optional. It defines the time duration within which all the matching events should occur. |
| `every` | `every` is an optional keyword. This defines whether the matching event should be triggered for every event that arrives at the specified stream with the matching condition. <br/> When this keyword is not used, the matching is carried out only once. |


**Example**

This query generates an alert if the increase in the temperature between two consecutive temperature events exceeds one degree.

```sql
from every e1=TempStream, e2=TempStream[e1.temp + 1 < temp]
select e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
```

**Counting Sequence**

Counting sequences allow you to match multiple events for the same matching condition.
The number of events matched per condition can be limited via condition postfixes such as **Counting Patterns**, or by using the 
`*`, `+`, and `?` operators.

The matching events can also be retrieved using event indexes, similar to how it is done in **Counting Patterns**.

**Syntax**

Each matching condition in a sequence can contain a collection of events as shown below. 

```sql
from (every)? <event reference>=<input stream>[<filter condition>](+|*|?)?, 
    <event reference>=<input stream [<filter condition>](+|*|?)?, 
    ... 
    (within <time gap>)?     
select <event reference>.<attribute name>, <event reference>.<attribute name>, ...
insert into <output stream>
```

|Postfix symbol|Required/Optional |Description|
|---------|---------|---------|
| `+` | Optional |This matches **one or more** events to the given condition. |
| `*` | Optional |This matches **zero or more** events to the given condition. |
| `?` | Optional |This matches **zero or one** events to the given condition. |


**Example**

This Siddhi application identifies temperature peeks.

```sql
define stream TempStream(deviceID long, roomNo int, temp double);
  
from every e1=TempStream, e2=TempStream[e1.temp <= temp]+, e3=TempStream[e2[last].temp > temp]
select e1.temp as initialTemp, e2[last].temp as peakTemp
insert into PeekTempStream;
```

**Logical Sequence**

Logical sequences identify logical relationships using `and`, `or` and `not` on consecutively arriving events.

**Syntax**
The syntax for a logical sequence is as follows:

```sql
from (every)? (not)? <event reference>=<input stream>[<filter condition>] 
          ((and|or) <event reference>=<input stream>[<filter condition>])? (within <time gap>)?, 
    ... 
select <event reference>([event index])?.<attribute name>, ...
insert into <output stream>
```

Keywords such as `and`, `or`, or `not` can be used to illustrate the logical relationship, similar to how it is done in **Logical Patterns**. 

**Example**

This Siddhi application notifies the state when a regulator event is immediately followed by both temperature and humidity events. 

```sql
define stream TempStream(deviceID long, temp double);
define stream HumidStream(deviceID long, humid double);
define stream RegulatorStream(deviceID long, isOn bool);
  
from every e1=RegulatorStream, e2=TempStream and e3=HumidStream
select e2.temp, e3.humid
insert into StateNotificationStream;
```

### Output rate limiting

Output rate limiting allows queries to output events periodically based on a specified condition.

**Purpose**

This allows you to limit the output to avoid overloading the subsequent executions, and to remove unnecessary information.

**Syntax**

The syntax of an output rate limiting configuration is as follows:

```sql
from <input stream> ...
select <attribute name>, <attribute name>, ...
output <rate limiting configuration>
insert into <output stream>
```
Siddhi supports three types of output rate limiting configurations as explained in the following table: 

Rate limiting configuration|Syntax| Description
---------|---------|--------
Based on time | `<output event> every <time interval>` | This outputs `<output event>` every `<time interval>` time interval.
Based on number of events | `<output event> every <event interval> events` | This outputs `<output event>` for every `<event interval>` number of events.
Snapshot based output | `snapshot every <time interval>`| This outputs all events in the window (or the last event if no window is defined in the query) for every given `<time interval>` time interval.

Here the `<output event>` specifies the event(s) that should be returned as the output of the query. 
The possible values are as follows:
* `first` : Only the first event processed by the query during the specified time interval/sliding window is emitted.
* `last` : Only the last event processed by the query during the specified time interval/sliding window is emitted.
* `all` : All the events processed by the query during the specified time interval/sliding window are emitted. **When no `<output event>` is defined, `all` is used by default.**

**Examples**

+ Returning events based on the number of events

    Here, events are emitted every time the specified number of events arrive. You can also specify whether to emit only the first event/last event, or all the events out of the events that arrived.
    
    In this example, the last temperature per sensor is emitted for every 10 events.
    
    <pre>
    from TempStreamselect 
    select temp, deviceID
    group by deviceID
    output last every 10 events
    insert into LowRateTempStream;    </pre>

+ Returning events based on time

    Here events are emitted for every predefined time interval. You can also specify whether to emit only the first event, last event, or all events out of the events that arrived during the specified time interval.

    In this example, emits all temperature events every 10 seconds  
      
    <pre>
    from TempStreamoutput 
    output every 10 sec
    insert into LowRateTempStream;    </pre>

+ Returning a periodic snapshot of events

    This method works best with windows. When an input stream is connected to a window, snapshot rate limiting emits all the current events that have arrived and do not have corresponding expired events for every predefined time interval. 
    If the input stream is not connected to a window, only the last current event for each predefined time interval is emitted.
    
    This query emits a snapshot of the events in a time window of 5 seconds every 1 second. 

    <pre>
    from TempStream#window.time(5 sec)
    output snapshot every 1 sec
    insert into SnapshotTempStream;    </pre>
    

## Partition

Partitions divide streams and queries into isolated groups in  order to process them in parallel and in isolation. 
A partition can contain one or more queries and there can be multiple instances where the same queries and streams are replicated for each partition. 
Each partition is tagged with a partition key. Those partitions only process the events that match the corresponding partition key. 

**Purpose** 

Partitions allow you to process the events groups in isolation so that event processing can be performed using the same set of queries for each group. 

**Partition key generation**

A partition key can be generated in the following two methods:

* Partition by value
  
    This is created by generating unique values using input stream attributes.

    **Syntax**
    
    <pre>
    partition with ( &lt;expression> of &lt;stream name>, &lt;expression> of &lt;stream name>, ... )
    begin
        &lt;query>
        &lt;query>
        ...
    end; </pre>
    
    **Example**
    
    This query calculates the maximum temperature recorded within the last 10 events per `deviceID`.
    
    <pre>
    partition with ( deviceID of TempStream )
    begin
        from TempStream#window.length(10)
        select roomNo, deviceID, max(temp) as maxTemp
        insert into DeviceTempStream;
    end;
    </pre>

* Partition by range

    This is created by mapping each partition key to a range condition of the input streams numerical attribute.

    **Syntax**
    
    <pre>
    partition with ( &lt;condition> as &lt;partition key> or &lt;condition> as &lt;partition key> or ... of &lt;stream name>, ... )
    begin
        &lt;query>
        &lt;query>
        ...
    end;
    </pre>

    **Example**
    
    This query calculates the average temperature for the last 10 minutes per office area.
    
    <pre>
    partition with ( roomNo >= 1030 as 'serverRoom' or 
                     roomNo < 1030 and roomNo >= 330 as 'officeRoom' or 
                     roomNo < 330 as 'lobby' of TempStream)
    begin
        from TempStream#window.time(10 min)
        select roomNo, deviceID, avg(temp) as avgTemp
        insert into AreaTempStream
    end;
    </pre>  

### Inner Stream

Queries inside a partition block can use inner streams to communicate with each other while preserving partition isolation.
Inner streams are denoted by a "#" placed before the stream name, and these streams cannot be accessed outside a partition block. 

**Purpose**
  
Inner streams allow you to connect queries within the partition block so that the output of a query can be used as an input only by another query 
within the same partition. Therefore, you do not need to repartition the streams if they are communicating within the partition.

**Example**

This partition calculates the average temperature of every 10 events for each sensor, and sends an output to the `DeviceTempIncreasingStream` stream if the consecutive average temperature values increase by more than 
5 degrees.

<pre>
partition with ( deviceID of TempStream )
begin
    from TempStream#window.lengthBatch(10)
    select roomNo, deviceID, avg(temp) as avgTemp
    insert into #AvgTempStream
  
    from every (e1=#AvgTempStream),e2=#AvgTempStream[e1.avgTemp + 5 < avgTemp]
    select e1.deviceID, e1.avgTemp as initialAvgTemp, e2.avgTemp as finalAvgTemp
    insert into DeviceTempIncreasingStream
end;
</pre>

## Table

A table is a stored version of an stream or a table of events. Its schema is defined via the **table definition** that is
similar to a stream definition. These events are by default stored `in-memory`, but Siddhi also provides store extensions to work with data/events stored in various data stores through the 
table abstraction.

**Purpose**

Tables allow Siddhi to work with stored events. By defining a schema for tables Siddhi enables them to be processed by queries using their defined attributes with the streaming data. You can also interactively query the state of the stored events in the table.

**Syntax**

The syntax for a new table definition is as follows:

```sql
define table <table name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );
```
The following parameters are configured in a table definition:

| Parameter     | Description |
| ------------- |-------------|
| `table name`      | The name of the table defined. (`PascalCase` is used for table name as a convention.) |
| `attribute name`   | The schema of the table is defined by its attributes with uniquely identifiable attribute names (`camalCase` is used for attribute names as a convention.)|    |
| `attribute type`   | The type of each attribute defined in the schema. <br/> This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOL` or `OBJECT`.     |


**Example**

The following defines a table named `RoomTypeTable` with `roomNo` and `type` attributes of data types `int` and `string` respectively.

```sql
define table RoomTypeTable ( roomNo int, type string );
```

**Primary Keys**

Tables can be configured with primary keys to avoid the duplication of data. 

Primary keys are configured by including the `@PrimaryKey( 'key1', 'key2' )` annotation to the table definition. 
Each event table configuration can have only one `@PrimaryKey` annotation. 
The number of attributes supported differ based on the table implementations. When more than one attribute 
 is used for the primary key, the uniqueness of the events stored in the table is determined based on the combination of values for those attributes.

**Examples**

This query creates an event table with the `symbol` attribute as the primary key. 
Therefore each entry in this table must have a unique value for `symbol` attribute.

```sql
@PrimaryKey('symbol')
define table StockTable (symbol string, price float, volume long);
```

**Indexes**
 
Indexes allow tables to be searched/modified much faster. 

Indexes are configured by including the `@Index( 'key1', 'key2' )` annotation to the table definition.
 Each event table configuration can have 0-1 `@Index` annotations. 
 Support for the `@Index` annotation and the number of attributes supported differ based on the table implementations. 
 When more then one attribute is used for index, each one of them is used to index the table for fast access of the data. 
 Indexes can be configured together with primary keys. 

**Examples**

This query creates an indexed event table named `RoomTypeTable` with the `roomNo` attribute as the index key.

```sql
@Index('roomNo')
define table RoomTypeTable (roomNo int, type string);
```

**Operators on Table**

The following operators can be performed on tables.


### Insert

This allows events to be inserted into tables. This is similar to inserting events into streams. 

!!! warning
    If the table is defined with primary keys, and if you insert duplicate data, primary key constrain violations can occur. 
    In such cases use the `update or insert into` operation. 
    
**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
insert into <table>
```

Similar to streams, you need to use the `current events`, `expired events` or the `all events` keyword between `insert` and `into` keywords in order to insert only the specific output event types. 
For more information, see [output event type](#output-event-types)

**Example**

This query inserts all the events from the `TempStream` stream to the `TempTable` table.

```sql
from TempStream
select *
insert into TempTable;
```

### Join (Table)

This allows a stream to retrieve information from a table in a streaming manner.

!!! Note
    Joins can also be performed with [two streams](#join-stream), [aggregation](#join-aggregation) or against externally [defined windows](#join-window).

**Syntax**

```sql
from <input stream> join <table>
    on <condition>
select (<input stream>|<table>).<attribute name>, (<input stream>|<table>).<attribute name>, ...
insert into <output stream>
```

!!! Note 
    A table can only be joint with a stream. Two tables cannot be joint because there must be at least one active 
    entity to trigger the join operation.

**Example**

This Siddhi App performs a join to retrieve the room type from `RoomTypeTable` table based on the room number, so that it can filter the events related to `server-room`s.

```sql
define table RoomTypeTable (roomNo int, type string);
define stream TempStream (deviceID long, roomNo int, temp double);
   
from TempStream join RoomTypeTable
    on RoomTypeTable.roomNo == TempStream.roomNo
select deviceID, RoomTypeTable.type as roomType, type, temp
    having roomType == 'server-room'
insert into ServerRoomTempStream;
```

### Delete

To delete selected events that are stored in a table.

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
delete <table> (for <output event type>)?
    on <condition>
```

The `condition` element specifies the basis on which events are selected to be deleted. 
When specifying the condition, table attributes should be referred to with the table name.
 
To execute delete for specific output event types, use the `current events`, `expired events` or the `all events` keyword with `for` as shown
in the syntax. For more information, see [output event type](#output-event-types)

!!! note 
    Table attributes must be always referred to with the table name as follows: 
    `<table name>.<attibute name>`
    
**Example**

In this example, the script deletes a record in the `RoomTypeTable` table if it has a value for the `roomNo` attribute that matches the value for the `roomNumber` attribute of an event in the `DeleteStream` stream.


```sql
define table RoomTypeTable (roomNo int, type string);

define stream DeleteStream (roomNumber int);
  
from DeleteStream
delete RoomTypeTable
    on RoomTypeTable.roomNo == roomNumber;
```

### Update

This operator updates selected event attributes stored in a table based on a condition. 

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
update <table> (for <output event type>)? 
    set <table>.<attribute name> = (<attribute name>|<expression>)?, <table>.<attribute name> = (<attribute name>|<expression>)?, ...
    on <condition>
```

The `condition` element specifies the basis on which events are selected to be updated.
When specifying the `condition`, table attributes must be referred to with the table name.

You can use the `set` keyword to update selected attributes from the table. Here, for each assignment, the attribute specified in the left must be the table attribute, and the one specified in the right can be a stream/table attribute a mathematical operation, or other. When the `set` clause is not provided, all the attributes in the table are updated.
   
To execute an update for specific output event types use the `current events`, `expired events` or the `all events` keyword with `for` as shown
in the syntax. For more information, see [output event type](#output-event-types).

!!! note 
    Table attributes must be always referred to with the table name as shown below:
     `<table name>.<attibute name>`.

**Example**

This Siddhi application updates the room occupancy in the `RoomOccupancyTable` table for each room number based on new arrivals and exits from the `UpdateStream` stream.

```sql
define table RoomOccupancyTable (roomNo int, people int);
define stream UpdateStream (roomNumber int, arrival int, exit int);
  
from UpdateStream
select *
update RoomTypeTable
    set RoomTypeTable.people = RoomTypeTable.people + arrival - exit
    on RoomTypeTable.roomNo == roomNumber;
```

### Update or Insert Into

This allows you update if the event attributes already exist in the table based on a condition, or 
else insert the entry as a new attribute.

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
update or insert into <table> (for <output event type>)? 
    set <table>.<attribute name> = <expression>, <table>.<attribute name> = <expression>, ...
    on <condition>
```
The `condition` element specifies the basis on which events are selected for update.
When specifying the `condition`, table attributes should be referred to with the table name. 
If a record that matches the condition does not already exist in the table, the arriving event is inserted into the table.

The `set` clause is only used when the update operation is performed and it is used during the insert operation. 
When `set` clause is used the left hand side attribute should be always a table attribute and 
the right hand side attribute can be stream/table attribute, mathematical 
 operations or other. When `set` clause is not provided all attributes in the table will be updated.  
 
To execute update upon specific output event types use the `current events`, `expired events` or the `all events` keyword with `for` as shown
in the syntax. To understand more refer [output event type](http://127.0.0.1:8000/documentation/siddhi-4.0/#output-event-types) section.

!!! note 
    Table attributes should be always referred to with the table name as `<table name>.<attibute name>`.

**Example**

The following query update for events in the `UpdateTable` event table that have room numbers that match the same in the `UpdateStream` stream. When such events are founding the event table, they are updated. When a room number available in the stream is not found in the event table, it is inserted from the stream.
 
```sql
define table RoomAssigneeTable (roomNo int, type string, assignee string);
define stream RoomAssigneeStream (roomNumber int, type string, assignee string);
   
from RoomAssigneeStream
select roomNumber as roomNo, type, assignee
update or insert into RoomAssigneeTable
    set RoomAssigneeTable.assignee = assignee
    on RoomAssigneeTable.roomNo == roomNo;
```

### In
 
This allows the stream to check whether the expected value exists in the table as a part of a conditional operation.

**Syntax**

```sql
from <input stream>[<condition> in <table>]
select <attribute name>, <attribute name>, ...
insert into <output stream>
```

The `condition` element specifies the basis on which events are selected to be compared. 
When constructing the `condition`, the table attribute must be always referred to with the table name as shown below:
`<table>.<attibute name>`.

**Example**

This Siddhi application filters only room numbers that are listed in the `ServerRoomTable` table.

```sql
define table ServerRoomTable (roomNo int);
define stream TempStream (deviceID long, roomNo int, temp double);
   
from TempStream[ServerRoomTable.roomNo == roomNo in ServerRoomTable]
insert into ServerRoomTempStream;
```

## Incremental Aggregation

Incremental aggregation allows you to obtain aggregates in an incremental manner for a specified set of time periods.

This not only allows you to calculate aggregations with varied time granularity, but also allows you to access them in an interactive
 manner for reports, dashboards, and for further processing. Its schema is defined via the **aggregation definition**.

**Purpose**

Incremental aggregation allows you to retrieve the aggregate value for different time durations. 
That is, it allows you to obtain aggregates such as `sum`, `count`, `avg`, `min`, `max`, and `count`
of stream attributes for durations such as `sec`, `min`, `hour`, etc.

This is of considerable importance in many Analytics scenarios because aggregate values are often needed for several time periods. 
Furthermore, this ensures that the aggregations are not lost due to unexpected system failures because aggregates can be stored in different persistence `stores`.

**Syntax**

```sql
@BufferSize("<positive integer>")
@IgnoreEventsOlderThanBuffer("<true or false>")
@store(type="<store type>", ...)
define aggregation <aggregator name>
from <input stream>
select <attribute name>, <aggregate function>(<attribute name>) as <attribute name>, ...
    group by <attribute name>
    aggregate by <timestamp attribute> every <time periods> ;
```
The above syntax includes the following:

|Item                          |Description
---------------                |---------
|`@BufferSize`                 |This annotation is optional. The default value is buffer size 0. <br/>It is used to identify the number of 'expired' events to retain <br/>in a buffer, to handle out of order event processing. It's an optional parameter <br/>which is applicable, only if aggregation is based on external timestamp (since events <br/>aggregated based on event arrival time cannot be out of order). An event is identified <br/>as 'expired' with relation to the latest event's timestamp and the most granular duration <br/>for which aggregation is calculated. For example, if aggregation is for sec...year, the <br/>most granular duration is seconds. Hence, if buffer size is 3 and events for 51st second, <br/>52nd second, 53rd second and 54th second arrive, all of the older aggregations (for <br/>seconds 51, 52 and 53) would be kept in the buffer (since latest event is for 54th second)
|`@IgnoreEventsOlderThanBuffer`|This annotation specifies whether or not to aggregate events older than the <br/>buffer. If this value is false (which is the default value as well), an event <br/>older than the buffer would be aggregated with the oldest event in buffer. If <br/>the value is true, an event older than the buffer would be dropped. This is an optional annotation.
|`@store`                      |This annotation is used to refer to the data store where the calculated <br/>aggregate results are stored. This annotation is optional. When <br/>no annotation is provided, the data is stored in the `in-memory` store.
|`<aggregator name>`           |This specifies a unique name for the aggregation so that it can be referred <br/>when accessing aggregate results.
|`<input stream>`              |The stream that feeds the aggregation. **Note! this stream should be <br/>already defined.**
|`group by <attribute name>`   |The `group by` clause is optional. If it is included in a Siddhi application, aggregate values <br/> are calculated per each `group by` attribute. If it is not used, all the<br/> events are aggregated together.
|`by <timestamp attribute>`    |This clause is optional. This defines the attribute that should be used as<br/> the timestamp. If this clause is not used, the event time is used by default.<br/> The timestamp could be given as either a `string` or a `long` value. If it is a `long` value,<br/> the unix timestamp in milliseconds is expected (e.g. `1496289950000`). If it is <br/>a `string` value, the supported formats are `<yyyy>-<MM>-<dd> <HH>:<mm>:<ss>` <br/>(if time is in GMT) and  `<yyyy>-<MM>-<dd> <HH>:<mm>:<ss> <Z>` (if time is <br/>not in GMT), here the ISO 8601 UTC offset must be provided for `<Z>` .<br/>(e.g., `+05:30`, `-11:00`).
|`<time periods>`              |The time periods can be given as a range separated by three dots, or as comma separated values. A range would be given as sec...year, where aggregation would be done per second, minute, hour, day, month and year. Comma separated values can be given as min, hour. However, skipping durations is not yet supported for comma separated values (e.g min, day is not a valid clause since hour duration has been skipped)

**Example**

This Siddhi Application defines an aggregation named `TradeAggregation` to calculate the average and sum for the `price` attribute of events arriving at the `TradeStream` stream. These aggregates are calculated per every time granularity in the range second-year.

```sql
define stream TradeStream (symbol string, price double, volume long, timestamp long);

define aggregation TradeAggregation
  from TradeStream
  select symbol, avg(price) as avgPrice, sum(price) as total
    group by symbol
    aggregate by timestamp every sec ... year;
```

### Join (Aggregation)

This allows a stream to retrieve calculated aggregate values from the aggregation. 

!!! Note
    A join can also be performed with [two streams](#join-stream), with a [table](#join-table) and a stream, or with a stream against externally [defined windows](#join-window).


**Syntax**

A join with aggregation is similer to the join with [table](#join-table), but with additional `within` and `per` clauses. 

```sql
from <input stream> join <aggrigation> 
  on <join condition> 
  within <time range> 
  per <time granularity>
select <attribute name>, <attribute name>, ...
insert into <output stream>;
```
Apart from constructs of [table join](#join-table) this includes the following. Please note that the 'on' condition is optional :

Item|Description
---------|---------
`within  <time range>`| This allows you to specify the time interval for which the aggregate values need to be retrieved. This can be specified by providing the start and end time separated by a comma as `string` or `long` values, or by using the wildcard `string` specifying the data range. For details refer examples.            
`per <time granularity>`|This specifies the time granularity by which the aggregate values must be grouped and returned. e.g., If you specify `days`, the retrieved aggregate values are grouped for each day within the selected time interval.

`within` and `par` clauses also accept attribute values from the stream.

**Example**

This query retrieves all aggregation per day within the time range `"2014-02-15 00:00:00 +05:30", "2014-03-16 00:00:00 +05:30"` (Please note that +05:30 can be omitted if timezone is GMT)

```sql
define stream StockStream (symbol string, value int);

from StockStream as S join TradeAggregation as T
  on S.symbol == T.symbol 
  within "2014-02-15 00:00:00 +05:30", "2014-03-16 00:00:00 +05:30" 
  per "days" 
select S.symbol, T.total, T.avgPrice 
insert into AggregateStockStream;
```

This query retrieves all aggregation per hour within the day `2014-02-15` .

```sql
define stream StockStream (symbol string, value int);

from StockStream as S join TradeAggregation as T
  on S.symbol == T.symbol 
  within "2014-02-15 **:**:** +05:30"
  per "hours" 
select S.symbol, T.total, T.avgPrice 
insert into AggregateStockStream;
```

This query retrieves all aggregation per value for an attribute of the stream  within the time period between timestamps `1496200000000` and `1596434876000`

```sql
define stream StockStream (symbol string, value int, perValue string);

from StockStream as S join TradeAggregation as T
  on S.symbol == T.symbol 
  within 1496200000000L, 1596434876000L
  per S.perValue
select S.symbol, T.total, T.avgPrice 
insert into AggregateStockStream;
```

## _(Defined)_ Window

A defined window is a window that can be shared across multiple queries. 
Events can be inserted to a defined window from one or more queries and it can produce output events based on the defined window type.
 
**Syntax**

The syntax for a defined window is as follows:

```sql
define window <window name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... ) <window type>(<parameter>, <parameter>, …) <output event type>;
```

The following parameters are configured in a table definition:

| Parameter     | Description |
| ------------- |-------------|
| `window name`      | The name of the window defined. (`PascalCase` is used for window names as a convention.) |
| `attribute name`   | The schema of the window is defined by its attributes with uniquely identifiable attribute names (`camalCase` is used for attribute names as a convention.)|    |
| `attribute type`   | The type of each attribute defined in the schema. <br/> This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOL` or `OBJECT`.     |
| `<window type>(<parameter>, ...)`   | The window type associated with the window and its parameters.     |
| `output <output event type>` | This is optional. Keywords such as `current events`, `expired events` and `all events` (the default) can be used to specify when the window output should be exposed. For more information, see [output event type](#output-event-types).
 

**Examples**
 
+ Returning all output when events arrive and when events expire from the window.

    In this query, the output event type is not specified. Therefore, it returns both current and expired events as the output.
    
    <pre>
    define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second); </pre>

+ Returning an output only when events expire from the window.

    In this query, the output event type of the window is `expired events`. Therefore, it only returns the events that have expired from the window as the output.
    
    <pre>
    define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second) output expired events; </pre>
     

**Operators on Defined Windows**

The following operators can be performed on defined windows.

### Insert

This allows events to be inserted into windows. This is similar to inserting events into streams. 

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
insert into <window>
```

To insert only events of a specific output event type, add the `current events`, `expired events` or the `all events` keyword between `insert` and `into` keywords (similar to how it is done for streams).

For more information, see [output event type](#output-event-types).

**Example**

This query inserts all events from the `TempStream` stream to the `OneMinTempWindow` window.

```sql
define stream TempStream(tempId string, temp double);
define window OneMinTempWindow(tempId string, temp double) time(1 min);

from TempStream
select *
insert into OneMinTempWindow;
```

### Join (Window)

To allow a stream to retrieve information from a window based on a condition.

!!! Note
    A join can also be performed with [two streams](#join-stream), [aggregation](#join-aggregation) or with tables [tables](#join-table).

**Syntax**

```sql
from <input stream> join <window>
    on <condition>
select (<input stream>|<window>).<attribute name>, (<input stream>|<window>).<attribute name>, ...
insert into <output stream>
```

**Example**

This Siddhi Application performs a join count the number of temperature events having more then 40 degrees 
 within the last 2 minutes. 

```sql
define window TwoMinTempWindow (roomNo int, temp double) time(2 min);
define stream CheckStream (requestId string);
   
from CheckStream as C join TwoMinTempWindow as T
    on T.temp > 40
select requestId, count(T.temp) as count
insert into HighTempCountStream;
```

### From

A window can be an input to a query, similar to streams. 

Note !!!
     When window is used as an input to a query, another window cannot be applied on top of this.

**Syntax**

```sql
from <window> 
select <attribute name>, <attribute name>, ...
insert into <output stream>
```

**Example**
This Siddhi Application calculates the maximum temperature within the last 5 minutes.

```sql
define window FiveMinTempWindow (roomNo int, temp double) time(5 min);


from FiveMinTempWindow
select max(temp) as maxValue, roomNo
insert into MaxSensorReadingStream;
```

## Trigger

Triggers allow events to be periodically generated. **Trigger definition** can be used to define a trigger. 
A trigger also works like a stream with a predefined schema.

**Purpose**

For some use cases the system should be able to periodically generate events based on a specified time interval to perform 
some periodic executions. 

A trigger can be performed for a `'start'` operation, for a given `<time interval>`, or for a given `'<cron expression>'`.


**Syntax**

The syntax for a trigger definition is as follows.

```sql
define trigger <trigger name> at ('start'| every <time interval>| '<cron expression>');
```

Similar to streams, triggers can be used as inputs. They adhere to the following stream definition and produce the `triggered_time` attribute of the `long` type.

```sql
define stream <trigger name> (triggered_time long);
```

The following types of triggeres are currently supported:

|Trigger type| Description|
|-------------|-----------|
|`'start'`| An event is triggered when Siddhi is started.|
|`every <time interval>`| An event is triggered periodically at the given time interval.
|`'<cron expression>'`| An event is triggered periodically based on the given cron expression. For configuration details, see <a target="_blank" href="http://www.quartz-scheduler.org/documentation/quartz-2.2.x/tutorials/tutorial-lesson-06">quartz-scheduler</a>.
 

**Examples**

+ Triggering events regularly at specific time intervals

    The following query triggers events every 5 minutes.
    
```sql
     define trigger FiveMinTriggerStream at every 5 min;
```

+ Triggering events at a specific time on specified days

    The following query triggers an event at 10.15 AM on every weekdays.
    
```sql
     define trigger FiveMinTriggerStream at '0 15 10 ? * MON-FRI';
```

## Script

Scripts allow you to write functions in other programming languages and execute them within Siddhi queries. 
Functions defined via scripts can be accessed in queries similar to any other inbuilt function. 
**Function definitions** can be used to define these scripts.

Function parameters are passed into the function logic as `Object[]` and with the name `data` . 

**Purpose**

Scripts allow you to define a function operation that is not provided in Siddhi core or its extension. It is not required to write an extension to define the function logic.

**Syntax**

The syntax for a Script definition is as follows.

```sql
define function <function name>[<language name>] return <return type> {
    <operation of the function>
};
```

The following parameters are configured when defining a script.

| Parameter     | Description |
| ------------- |-------------|
| `function name`| 	The name of the function (`camalCase` is used for the function name) as a convention.|
|`language name`| The name of the programming language used to define the script, such as `javascript`, `r` and `scala`.|
| `return type`| The attribute type of the function’s return. This can be `int`, `long`, `float`, `double`, `string`, `bool` or `object`. Here the function implementer should be responsible for returning the output attribute on the defined return type for proper functionality.
|`operation of the function`| Here, the execution logic of the function is added. This logic should be written in the language specified under the `language name`, and it should return the output in the data type specified via the `return type` parameter.

**Examples**

This query performs concatenation using JavaScript, and returns the output as a string.

```sql
define function concatFn[javascript] return string {
    var str1 = data[0];
    var str2 = data[1];
    var str3 = data[2];
    var responce = str1 + str2 + str3;
    return responce;
};
  
define stream TempStream(deviceID long, roomNo int, temp double);
  
from TempStream
select concatFn(roomNo,'-',deviceID) as id, temp 
insert into DeviceTempStream;
```

## Extensions

Siddhi supports an extension architecture to enhance its functionality by incorporating other libraries in a seamless manner. 

**Purpose**

Extensions are supported because, Siddhi core cannot have all the functionality that's needed for all the use cases, mostly use cases require 
different type of functionality, and for some cases there can be gaps and you need to write the functionality by yourself.

All extensions have a namespace. This is used to identify the relevant extensions together, and to let you specifically call the extension.

**Syntax**

Extensions follow the following syntax;

```sql
<namespace>:<function name>(<parameter>, <parameter>, ... )
```

The following parameters are configured when referring a script function.

| Parameter     | Description |
| ------------- |-------------|
|`namespace` | Allows Siddhi to identify the extension without conflict|
| `function name`| 	The name of the function referred.|
| `parameter`| 	The function input parameter for function execution.|

**Extension types**

Siddhi supports following extension types:

* **Function**

    For each event, it consumes zero or more parameters as input parameters and returns a single attribute. This can be used to manipulate existing event attributes to generate new attributes like any Function operation.
    
    This is implemented by extending `org.wso2.siddhi.core.executor.function.FunctionExecutor`.
    
    Example : 
    
    `math:sin(x)` 
    
    Here, the `sin` function of `math` extension returns the sin value for the `x` parameter.
    
* **Aggregate Function**

    For each event, it consumes zero or more parameters as input parameters and returns a single attribute with aggregated results. This can be used in conjunction with a window in order to find the aggregated results based on the given window like any Aggregate Function operation. 
    
     This is implemented by extending `org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator`.

    Example : 
    
    `custom:std(x)` 
    
    Here, the `std` aggregate function of `custom` extension returns the standard deviation of the `x` value based on its assigned window query. 

* **Window** 

    This allows events to be **collected, generated, dropped and expired anytime** **without altering** the event format based on the given input parameters, similar to any other Window operator. 
    
    This is implemented by extending `org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor`.

    Example : 
    
    `custom:unique(key)` 
    
    Here, the `unique` window of the `custom` extension retains one event for each unique `key` parameter.

* **Stream Function**

    This allows events to be  **generated or dropped only during event arrival** and **altered** by adding one or more attributes to it. 
    
    This is implemented by extending  `org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor`.
    
    Example :  
    
    `custom:pol2cart(theta,rho)` 
    
    Here, the `pol2cart` function of the `custom` extension returns all the events by calculating the cartesian coordinates `x` & `y` and adding them as new attributes to the events.

* **Stream Processor**
    
    This allows events to be **collected, generated, dropped and expired anytime** by **altering** the event format by adding one or more attributes to it based on the given input parameters. 
    
    Implemented by extending "org.wso2.siddhi.core.query.processor.stream.StreamProcessor".
    
    Example :  
    
    `custom:perMinResults(<parameter>, <parameter>, ...)` 
    
    Here, the `perMinResults` function of the `custom` extension returns all events by adding one or more attributes to the events based on the conversion logic. Altered events are output every minute regardless of event arrivals.

* **Sink**

Sinks provide a way to publish Siddhi events to external systems in the preferred data format. Sinks publish events from the streams via multiple transports to external endpoints in various data formats.

Implemented by extending "org.wso2.siddhi.core.stream.output.sink.Sink".

   Example : 

    `@sink(type='sink_type', static_option_key1='static_option_value1')`
    
To configure a stream to publish events via a sink, add the sink configuration to a stream definition by adding the @sink annotation with the required parameter values. The sink syntax is as above

* **Source**

Source allows Siddhi to consume events from external systems, and map the events to adhere to the associated stream. Sources receive events via multiple transports and in various data formats, and direct them into streams for processing.

Implemented by extending "org.wso2.siddhi.core.stream.input.source.Source".

   Example : 

    `@source(type='source_type', static.option.key1='static_option_value1')`
    
To configure a stream that consumes events via a source, add the source configuration to a stream definition by adding the @source annotation with the required parameter values. The source syntax is as above

* **Store**

You can use Store extension type to work with data/events stored in various data stores through the table abstraction. You can find more information about these extension types under the heading 'Extension types' in this document. 

Implemented by extending "org.wso2.siddhi.core.table.record.AbstractRecordTable".

* **Script**

Scripts allow you to define a function operation that is not provided in Siddhi core or its extension. It is not required to write an extension to define the function logic. Scripts allow you to write functions in other programming languages and execute them within Siddhi queries. Functions defined via scripts can be accessed in queries similar to any other inbuilt function.

Implemented by extending "org.wso2.siddhi.core.function.Script".

* **Source Mapper**

Each @source configuration has a mapping denoted by the @map annotation that converts the incoming messages format to Siddhi events.The type parameter of the @map defines the map type to be used to map the data. The other parameters to be configured depends on the mapper selected. Some of these parameters are optional. 

Implemented by extending "org.wso2.siddhi.core.stream.output.sink.SourceMapper".

   Example :
   
    `@map(type='map_type', static_option_key1='static_option_value1')`

* **Sink Mapper**

Each @source configuration has a mapping denoted by the @map annotation that converts the incoming messages format to Siddhi events.The type parameter of the @map defines the map type to be used to map the data. The other parameters to be configured depends on the mapper selected. Some of these parameters are optional. 

Implemented by extending "org.wso2.siddhi.core.stream.output.sink.SinkMapper".

   Example :
   
    `@map(type='map_type', static_option_key1='static_option_value1')`

**Example**

A window extension created with namespace `foo` and function name `unique` can be referred as follows:

```sql
from StockExchangeStream[price >= 20]#window.foo:unique(symbol)
select symbol, price
insert into StockQuote
```

**Available Extensions**

Siddhi currently has several pre written extensions that are available <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">here</a>
 
_We value your contribution on improving Siddhi and its extensions further._


### Writing Custom Extensions

Custom extensions can be written in order to cater use case specific logic that are not available in Siddhi out of the box or as an existing extension. 

There are five types of Siddhi extensions that you can write to cater your specific use cases. Please find each 
extension types and related maven archetypes below. You can use these archetypes to generate maven projects for each 
extension type.

* Follow one of the step below, based on your project :


**siddhi-execution**

Siddhi-execution provides following extension types,

* Function
* Aggregate Function
* Stream Function
* Stream Processor
* Window

You can use one or more from above mentioned extension types and implement according to your requirement. You can find more information about these extension types under the heading 'Extension types' in this document. 

* Run the following command
            
                mvn archetype:generate
                    -DarchetypeGroupId=org.wso2.siddhi.extension.archetype
                    -DarchetypeArtifactId=siddhi-archetype-execution
                    -DgroupId=org.wso2.extension.siddhi.execution
                    -Dversion=1.0.0-SNAPSHOT
            
* Then the system will pop-up the following message to enter the execution name
           
            eg:- Define value for property 'executionType': ML
            
* Finally confirm all property values are correct or not by typing Y or press Enter, else type N
                  
**siddhi-io**

Siddhi-io provides following extension types,

* Sink
* Source

You can use one or more from above mentioned extension types and implement according to your requirement. siddhi-io generaly uses to work with IO operations. If you want get inputs to your Siddhi app, you can use 'Source' extension type. If you want to get outputs from your Siddhi app, you can use 'Sink' extension and implement it. You can find more information about these extension types under the heading 'Extension types' in this document. 
    
* Run the following command
                
          
               mvn archetype:generate
                   -DarchetypeGroupId=org.wso2.siddhi.extension.archetype
                   -DarchetypeArtifactId=siddhi-archetype-io
                   -DgroupId=org.wso2.extension.siddhi.io
                   -Dversion=1.0.0-SNAPSHOT
            
* Then the system will pop-up the following message to enter the typeOf_IO
           
         eg:- Define value for property 'typeOf_IO': http

* Finally confirm all property values are correct or not by typing Y or press Enter, else type N
         
**siddhi-map**

Siddhi-map provides following extension types,

* Sink Mapper
* Source Mapper

You can use one or more from above mentioned extension types and implement according to your requirement. Source Mapper is used to map events to a predefined data format (such as XML, JSON, binary, etc), and publishes them to external endpoints (such as E-mail, TCP, Kafka, HTTP, etc). Sink Mapper is used for same usecase, but in the time of publishing events from Siddhi app.You can find more information about these extension types under the heading 'Extension types' in this document. 
        
* Run the following command
                    
            
                mvn archetype:generate
                    -DarchetypeGroupId=org.wso2.siddhi.extension.archetype
                    -DarchetypeArtifactId=siddhi-archetype-map
                    -DgroupId=org.wso2.extension.siddhi.map
                    -Dversion=1.0.0-SNAPSHOT
            
* Then the system will pop-up the following message to enter the mapType
       
            eg:- Define value for property 'mapType':CSV
    
* Finally confirm all property values are correct or not by typing Y or press Enter, else type N
                   
**siddhi-script**

Siddhi-script provides following extension types,

* Script

You can use script extension type to write functions in other programming languages and execute them within Siddhi queries. Functions defined via scripts can be accessed in queries similar to any other inbuilt function. You can find more information about these extension types under the heading 'Extension types' in this document. 

* Run the following command
                        
           
               mvn archetype:generate
                   -DarchetypeGroupId=org.wso2.siddhi.extension.archetype
                   -DarchetypeArtifactId=siddhi-archetype-script
                   -DgroupId=org.wso2.extension.siddhi.script
                   -Dversion=1.0.0-SNAPSHOT
           
* Then the system will pop-up the following message to enter the script type
       
         eg:- Define value for property 'typeOfScript':

* Finally confirm all property values are correct or not by typing Y or press Enter, else type N
       
**siddhi-store**

Siddhi-store provides following extension types,

* Store

You can use Store extension type to work with data/events stored in various data stores through the table abstraction. You can find more information about these extension types under the heading 'Extension types' in this document. 

* Run the following command
                            
   
               mvn archetype:generate
                  -DarchetypeGroupId=org.wso2.siddhi.extension.archetype
                  -DarchetypeArtifactId=siddhi-archetype-store
                  -DgroupId=org.wso2.extension.siddhi.store
                  -Dversion=1.0.0-SNAPSHOT
           
* Then the system will pop-up the following message to enter the store type
                          
          eg:- Define value for property 'storeType': RDBMS
    
* Finally confirm all property values are correct or not by typing Y or press Enter, else type N.

## Configuring and Monitoring Siddhi Applications

This section explains how to use the `@app` annotation to generate statistics for Siddhi applications as well as improve the performance of Siddhi applications.

### @app:statistics

To evaluate the performance of an application, you can enable the statistics of a Siddhi application to be published. This is done via the `@app:statistics` annotation that can be added to a Siddhi application as shown in the following example.

```sql
@app:statistics(reporter = 'console')
```
The following elements are configured with this annotation.

|Annotation| Description| Default Value|
| ------------- |-------------|-------------|
|`reporter`|The interface in which statistics for the Siddhi application are published. Possible values are as follows:<br/> `console`<br/> `jmx`|`console`|
|`interval`|The time interval (in seconds) at  which the statistics for the Siddhi application are reported.|`60`|
|`include`|If this parameter is added, only the types of metrics you specify are included in the reporting. The required metric types can be specified as a comma-separated list. It is also possible to use wild cards| All (*.*)|

The metrics are reported in the following format.
`org.wso2.siddhi.SiddhiApps.<SiddhiAppName>.Siddhi.<Component Type>.<Component Name>. <Metrics name>`

The following table lists the types of metrics supported for different Siddhi application component types.

|Component Type|Metrics Type|
| ------------- |-------------|
|Stream|Throughput<br/>The size of the buffer if parallel processing is enabled via the @async annotation.|
|Trigger|Throughput (Trigger and Stream)|
|Source|Throughput|
|Sink|Throughput|
|Mapper|Latency<br/>Input/output throughput<br/>
|Table|Memory<br/>Throughput (For all operations)<br/>Throughput (For all operations)|
|Query|Memory<br/>Latency|
|Window|Throughput (For all operations)<br/>Latency (For all operation)|
|Partition|Throughput (For all operations)<br/>Latency (For all operation)|



e.g., the following is a Siddhi application that includes the `@app` annotation to report performance statistics.

```sql
@App:name('TestMetrics')
@App:Statistics(reporter = 'console')

@Async(buffer.size='64')
define stream TestStream (message string);

@info(name='logQuery')
from TestSream#log("Message:")
insert into TempSream;
```

Statistics are reported for this Siddhi application as shown in the extract below.

<details>
  <summary>Click to view the extract</summary>
11/26/17 8:01:20 PM ============================================================

 -- Gauges ----------------------------------------------------------------------
 org.wso2.siddhi.SiddhiApps.TestMetrics.Siddhi.Queries.logQuery.memory
              value = 5760
 org.wso2.siddhi.SiddhiApps.TestMetrics.Siddhi.Streams.TestStream.size
              value = 0
 
 -- Meters ----------------------------------------------------------------------
 org.wso2.siddhi.SiddhiApps.TestMetrics.Siddhi.Sources.TestStream.http.throughput
              count = 0
          mean rate = 0.00 events/second
      1-minute rate = 0.00 events/second
      5-minute rate = 0.00 events/second
     15-minute rate = 0.00 events/second
 org.wso2.siddhi.SiddhiApps.TestMetrics.Siddhi.Streams.TempSream.throughput
              count = 2
          mean rate = 0.04 events/second
      1-minute rate = 0.03 events/second
      5-minute rate = 0.01 events/second
     15-minute rate = 0.00 events/second
 org.wso2.siddhi.SiddhiApps.TestMetrics.Siddhi.Streams.TestStream.throughput
              count = 2
          mean rate = 0.04 events/second
      1-minute rate = 0.03 events/second
      5-minute rate = 0.01 events/second
     15-minute rate = 0.00 events/second
 
 -- Timers ----------------------------------------------------------------------
 org.wso2.siddhi.SiddhiApps.TestMetrics.Siddhi.Queries.logQuery.latency
              count = 2
          mean rate = 0.11 calls/second
      1-minute rate = 0.34 calls/second
      5-minute rate = 0.39 calls/second
     15-minute rate = 0.40 calls/second
                min = 0.61 milliseconds
                max = 1.08 milliseconds
               mean = 0.84 milliseconds
             stddev = 0.23 milliseconds
             median = 0.61 milliseconds
               75% <= 1.08 milliseconds
               95% <= 1.08 milliseconds
               98% <= 1.08 milliseconds
               99% <= 1.08 milliseconds
             99.9% <= 1.08 milliseconds


</details>

### @app:playback

When this annotation is included, the timestamp of the event (specified via an attribute) is treated as the current time. This results in events being processed faster.
The following elements are configured with this annotation.

|Annotation| Description|
| ------------- |-------------|
|`idle.time`|If no events are received during a time interval specified (in milliseconds) via this element, the Siddhi system time is incremented by a number of seconds specified via the `increment` element.|
|`increment`|The number of seconds by which the Siddhi system time must be incremented if no events are received during the time interval specified via the `idle.time` element.|

e.g., In the following example, the Siddhi system time is incremented by two seconds if no events arrive for a time interval of 100 milliseconds.

`@app:playback(idle.time = '100 millisecond', increment = '2 sec') `