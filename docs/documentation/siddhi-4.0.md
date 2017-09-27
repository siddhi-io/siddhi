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


## Streams
Streams is a logical series of events ordered in time. It's schema is defined via the **stream definition**.
A stream definition contains a unique name and a set of attributes with specific types and uniquely identifiable names within the stream.
All events of a particular Stream will have the same schema (i.e. have the same attributes in the same order). 

**Purpose**

By defining a schema it unifies common types of events together. This enables them to be processed at queries using their defined attributes in a streaming manner, and let sinks and sources to map events to/from various data formats.

**Syntax**

The following is the syntax for defining a new stream.
```sql
define stream <stream name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );
```
The following parameters are configured in a stream definition.

| Parameter     | Description |
| ------------- |-------------|
| `stream name`      | The name of the stream created. (as a convention `PascalCase` is used for stream name) |
| `attribute name`   | The schema of an stream is defined by its attributes by uniquely identifiable attribute names (as a convention `camalCase` is used for attribute names)|    |
| `attribute type`   | The type of each attribute defined in the schema. <br/> This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOL` or `OBJECT`.     |

**Example**
```sql
define stream TempStream (deviceID long, roomNo int, temp double);
```
The above creates a stream named `TempStream` with the following attributes.

+ `deviceID` of type `long`
+ `roomNo` of type `int` 
+ `temp` of type `double` 

### Sources
Sources allow you to receive events via multiple transports and with various data formats into streams for processing.

Source let you define a mapping to convert the incoming event from its native data format (such as `JSON`, `TEXT`, `XML`, etc) 
to Siddhi Event, when customization to such mappings is not provided Siddhi assumes that the arriving event adhere to a 
predefined format based on the stream definition and the selected message format.

**Purpose**

Source provides a way to Siddhi consume events from external systems and map the events to adhere to the associated stream. 

**Syntax**

To configure a stream to consume events via a source, add the source configuration to a stream definition by adding the `@source` annotation with the required parameter values. 
The source syntax is as follows:
```sql
@source(type='source_type', static.option.key1='static_option_value1', static.option.keyN='static_option_valueN',
    @map(type='map_type', static.option_key1='static_option_value1', static.option.keyN='static_option_valueN',
        @attributes( attributeN='attribute_mapping_N', attribute1='attribute_mapping_1')
    )
)
define stream stream_name (attribute1 Type1, attributeN TypeN);
```

**Source**

The `type` parameter of `@source` defines the source type that receives events. The other parameters to be configured 
depends on the source selected, some of those can also be optional values. 
For detailed information about the parameters refer the appropriate source documentation.

Some of the supports source types are:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-tcp/">TCP</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/">Kafka</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-http/">HTTP</a> _(Only works in WSO2 Stream Processor)_ 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-mqtt/">MQTT</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-rabbitmq/">RabbitMQ</a> 
* In-Memory
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-file/">File</a> _(Only works in WSO2 Stream Processor)_ 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/">JMS</a> _(Only works in WSO2 Stream Processor)_  
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/">E-mail</a>  _(Only works in WSO2 Stream Processor)_ 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/">WSO2-Event</a> _(Only works in WSO2 Stream Processor)_ 

**Source Mapper**

Each `@source` will have a mapping denoted by `@map` that converts the incoming message format to Siddhi event.

The `type` parameter of `@map` defines the map type that's responsible of mapping the data. The other parameters to be 
configured depends on the mapper selected, some of those can also be optional values. 
For detailed information about the parameters refer the appropriate mapper documentation.

!!! tip 
    When `@map` is not provided `@map(type='passThrough')` will be used as default. This can be used when source consumes Siddhi events and when it does not need any mappings.
    
**Map Attributes**

`@attributes` is an optional parameter of `@map` to define custom mapping. When `@attributes` is not provided each mapper
assumes that the incoming events will be adhere to it's own expected default data format. By defining `@attributes` you 
can configure mappers to extract data from the incoming message selectively and assign then to attributes. 

There are two ways you can configure `@attributes`. 

1. Defining attributes as keys and mapping content as value in the following format: <br/>
```@attributes( attributeN='mapping_N', attribute1='mapping_1')``` 
2. Defining all attributes' mapping content in the same order as how the attributes are defined in stream definition: <br/>
```@attributes( 'mapping_1', 'mapping_N')``` 

Some of the supports source mappings are:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/">JSON</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-xml/">XML</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-binary/">Binary</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/">Text</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/">Key-Value</a>

**Example**

The following query receives events via the `HTTP` source on `JSON` data format, and passes them in the `InputStream` stream for processing. 
Here the HTTP source is configured to receive events on all network interfaces on port `8080` on the context `foo`, and 
its protected by basic authentication.

```sql
@source(type='http', receiver.url='http://0.0.0.0:8080/foo', is.basic.auth.enabled='true', 
  @map(type='json'))
define stream InputStream (name string, age int, country string);
```
### Sinks

Sinks allow you to publish events from the streams via multiple transports to external endpoints with various data formats.

Sink let you define a mapping to convert the Siddhi event to appropriate output data format (such as `JSON`, `TEXT`, `XML`, etc), 
when customization to such mappings is not provided Siddhi converts events to its default format based on the stream definition and 
the selected data format and publish the events.

**Purpose**

Sink provides a way to publish Siddhi events to external systems in their preferred data format. 

**Syntax**

To configure a stream to publish events via a Sink, add the sink configuration to a stream definition by adding the `@sink` 
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
    The Sink and Sink mapper properties that are categorised as `dynamic` have the ability to absorb attributes values 
    from their associated Streams. This can be done by using the attribute names in double curly braces as `{{...}}` when configuring the property value. 
    
    Some valid dynamic properties values are: 
    
    * `'{{attribute1}}'`
    * `'This is {{attribute1}}'` 
    * `{{attribute1}} > {{attributeN}}`  
    
    Here the attribute names in the double curly braces will be replaced with event values during execution. 


**Sink**

The `type` parameter of `@sink` defines the sink type that publishes the events. The other parameters to be configured 
depends on the sink selected, some of those can also be optional and some can be dynamic values. 
For detailed information about the parameters refer the appropriate sink documentation.

Some of the supports sink types are:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-tcp/">TCP</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-kafka/">Kafka</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/">E-mail</a>  _(Only works in WSO2 Stream Processor)_ 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-mqtt/">MQTT</a> 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-rabbitmq/">RabbitMQ</a> 
* In-Memory
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-file/">File</a> _(Only works in WSO2 Stream Processor)_ 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-http/">HTTP</a> _(Only works in WSO2 Stream Processor)_ 
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-jms/">JMS</a> _(Only works in WSO2 Stream Processor)_  
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-email/">WSO2-Event</a> _(Only works in WSO2 Stream Processor)_ 

**Sink Mapper**

Each `@sink` will have a mapping denoted by `@map` that converts the Siddhi event to an outgoing message format.

The `type` parameter of `@map` defines the map type that's responsible of mapping the event. The other parameters to be configured depends on the mapper selected, some of those can also be optional or dynamic values. 
For detailed information about the parameters refer the appropriate mapper documentation.

!!! tip 
    When `@map` is not provided `@map(type='passThrough')` will be used as default. This can be used when Sink can publish Siddhi events and when it does not need any mappings.

**Map Payload**

`@payload` is an optional parameter of `@map` to define a custom mapping. When `@payload` is not provided each mapper
maps the outgoing events to it's own default data format. By defining `@payload` you 
can configure mappers to produce the output payload as of your choice using dynamic properties by selectively assigning 
the attributes on your preferred format. 

There are two ways you can configure `@payload`. 

1. Some mappers such as `XML`, `JSON`, and `Test` accepts only one output payload using the following format: <br/>
```@payload( 'This is a test message from {{user}}.' )``` 
2. Some mappers such `key-value` accepts series of mapping values defined as: <br/>
```@payload( key1='mapping_1', key2='user : {{user}}')``` 

Some of the supports sink mappings are:

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-json/">JSON</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-xml/">XML</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-binary/">Binary</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/">Text</a>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-keyvalue/">Key-Value</a>

**Example**

The following query publishes events from `OutputStream` via the `HTTP` Sink. Here the events are mapped to default `JSON` payloads and sent to `http://localhost:8005/endpoint`
 using `POST` method, `Accept` header, and basic authentication having `admin` as both the username and the password.
```sql
@sink(type='http', publisher.url='http://localhost:8005/endpoint', method='POST', headers='Accept-Date:20/02/2017', 
  basic.auth.username='admin', basic.auth.password='admin', basic.auth.enabled='true',
  @map(type='json'))
define stream OutputStream (name string, ang int, country string);
```

## Query

Each Siddhi query can consume one/more streams and zero/one table, process the events in streaming manner and generate a
 output event to a stream or performs CRUD operation to a table.

**Purpose**

Query enables you to perform Complex Event Processing and Stream Processing operations by processing incoming events one by one in the arrival order. 

**Syntax**

All queries contain an input and output section, some also contain a projection section. A simple query with all three sections is as follows.

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
insert into <output stream/table>
```
**Example**

Following simple query in the Siddhi App consumes events from defined `TempStream` stream and output the room temperature and room number to `RoomTempStream` stream.

```sql
define stream TempStream (deviceID long, roomNo int, temp double);

from TempStream 
select roomNo, temp
insert into RoomTempStream;
```
!!! tip "Inferred Stream"
    Here the `RoomTempStream` is an inferred Stream, i.e. `RoomTempStream` can be used as any other defined stream 
    without explicitly defining its Stream Definition and the definition of the `RoomTempStream` will be inferred from the 
    first query that produces the Stream.  

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
            E.g., Use one of the following queries to select all the attributes in the `NewTempStream` stream.
            <pre>from TempStream<br>select *<br>insert into NewTempStream;</pre>
            or
            <pre>from TempStream<br>insert into NewTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Renaming attributes</td>
        <td>This selects attributes from the input streams and inserting them into the output stream with different names.
            <br><br>
            E.g., The following query renames `roomNo` to `roomNumber` and `temp` to `temperature`.
            <pre>from TempStream <br>select roomNo as roomNumber, temp as temperature<br>insert into RoomTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Introducing the constant value</td>
        <td>This adds constant values by assigning it to an attribute using `as`.
            <br></br>
            E.g., Using 'C' as the constant value for `scale` attribute. 
            <pre>from TempStream<br>select roomNo, temp, 'C' as scale<br>insert into RoomTempStream;</pre>
        </td>
    </tr>
    <tr>
        <td>Using mathematical and logical expressions</td>
        <td>This uses attributes with mathematical and logical expressions in the precedence order given below, and assign them to the output attribute using `as`.
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

###Functions

A function consumes zero, one or more parameters and always produces a result value. It can be used any location where
 an attribute can be used. 

**Purpose**

Functions encapsulates complex making the Siddhi application simple and easy to understand. 

**Function Parameters**

Functions parameters can be attributes, constant values, results of other functions, results of mathematical or logical expressions or time parameters. 
Function parameters depending on the function being called.

Time is a special parameter that can we defined using the integer time value followed by its unit as `<int> <unit>`. 
Following are the supported unit types. Upon execution time returns the value in the scale of milliseconds as a long value. 

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

### Filters

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

The following query filters all server rooms within the range of 100 and 210, and having temperature greater than 40 degrees 
from the `TempStream` stream, and inserts the results into the `HighTempStream` stream.

```sql
from TempStream[(roomNo >= 100 and roomNo < 210) and temp > 40]
select roomNo, temp
insert into HighTempStream;
```

### Windows

Windows allow you to capture a subset of events based on a specific criterion from an input stream for calculation. 
Each input stream can only have maximum of one window.

**Purpose**

To create subsets of events within a stream based on time duration, number of events, etc for processing. 
Window can operate on sliding or tumbling (batch) manner.

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

If you want to identify the maximum temperature out of last 10 events, you need to define a `length` window of 10 events.
 This window operates as a sliding mode where the following 3 subsets will be calculated when a list of 12 events are received in a sequential order.

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
 Where this window operates as a batch/tumbling mode where the following 3 subsets will be calculated when a list of 30 events are received in a sequential order.

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
    Similar operations can also be done time with `time` window and `timeBatch` window and for others. 
    Code segments such as `#window.time(10 min)` will consider events in last 10 minutes in a sliding manner and `#window.timeBatch(2 min)` will consider events every 2 minutes in a tumbling manner. 

Following are some inbuilt windows shipped with Siddhi, for more window types refer execution <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>. 

* time
* timeBatch
* length
* lengthBatch
* sort
* frequent
* lossyFrequent
* cron
* externalTime
* externalTimeBatch

**Output event type**<a id="output-event-types" class='anchor' aria-hidden='true'></a> 

Projection of the query depends on the output event types such as, `current` and `expired` event types.
 By default all queries produce `current` events and only queries with windows produce `expired` events 
 when events expire from the window.
 By controlling the event types outputted you will be able to manipulate the output of the query. 
 
 **Note!** Controlling the output event types will not alter the execution within the query and will not affect the correctness of the query execution.  
 
 The following keywords can be used with the output stream to manipulate output. 
 
| Output event types | Description |
|-------------------|-------------|
| `current events` | Output events when incoming events arrives to the query. </br> This is the default functionality of no output event types is specified.|
| `expired events` | Output events when events expires from the window. |
| `all events` | Output events when incoming events arrives to the query and </br> when events expires from the window. |

The output event type keyword can be used between `insert` and `into` as given in the following example.

**Example**

Delay all events in a stream by 1 minute.  

```sql
from TempStream#window.time(1 min)
select *
insert expired events into DelayedTempStream
```

### Aggregate functions

Aggregate functions perform aggregate calculations in the query. 
When a window is defined the aggregation will get restricted within that window and if no window is provided they will aggregate from the start of the Siddhi application.

**Syntax**

```sql
from <input stream>#window.<window name>(<parameter>, <parameter>, ... )
select <aggregate function>(<parameter>, <parameter>, ... ) as <attribute name>, <attribute2 name>, ...
insert into <output stream>;
```

**Aggregate Parameters**

Aggregate parameters can be attributes, constant values, results of other functions or aggregates, results of mathematical or logical expressions or time parameters. 
Aggregate parameters depending on the Aggregation being called.

**Example**

The following query calculates the average of `temp` attribute of `TempStream` stream for the last 
10 minutes and emit it as `avgTemp``.

```sql
from TempStream#window.time(10 min)
select avg(temp) as avgTemp, roomNo, deviceID
insert into AvgTempStream;
```
Following are some inbuilt aggregation functions shipped with Siddhi, for more aggregation functions refer execution <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>. 

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

```sql
from <input stream>#window.<window name>(...)
select <aggregate function>( <parameter>, <parameter>, ...) as <attribute1 name>, <attribute2 name>, ...
group by <attribute1 name>, <attribute2 name> ...
insert into <output stream>;
```

**Example**
The following query calculates the average temperature per `roomNo` and `deviceID` combination 
for sliding 10 minutes time from the `TempStream` stream.

```sql
from TempStream#window.time(10 min)
select avg(temp) as avgTemp, roomNo, deviceID
group by roomNo, deviceID
insert into AvgTempStream;
```

### Having

Having allows you to filter events after processing the `select` statement,
 this will be helpful to filter the aggregation output.

**Syntax**

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

### Joins (Stream) 

**Purpose**

Join let you get a combined results from two streams in real time based on a condition. 

As streams are stateless to join two streams they need to be connected to a window such that 
there will be a pool of events that can be used for joining. Join also accepts a condition to 
join appropriate events from each streams. 
 
During the joining process each incoming event of each stream is matched against all the events in the other 
stream's window on the given condition, and the output events are generated for all the matching event pairs.

!!! Note
    Join can also be performed with [sored data](#join-table), [aggregation](#join-aggregation) or externally [defined windows](#join-window).

**Syntax**

The syntax for a join is as follows:

```sql
from <input stream>#window.<window name>(<parameter>, ... ) {unidirectional} {as <reference>}
         join <input stream>#window.<window name>(<parameter>,  ... ) {unidirectional} {as <reference>}
    on <join condition>
select <attribute name>, <attribute name>, ...
insert into <output stream>
```
Here the `<join condition>` let you match the attributes from both the streams. 

**unidirectional join operation**

By default events arriving on either stream can trigger the joining process, but if you want to control the 
join execution you can add the `unidirectional` keyword next to a stream in the join definition as depicted in the 
syntax to only enable that stream to trigger the join operation. Here events arriving on other stream only updates its 
 window and it will not trigger the join operation.
 
!!! note
    We cannot use `unidirectional` keyword for both the input streams (as that's equal to the default behaviour, which 
    is not using the unidirectional keyword at all).

**Example**

Assuming that the temperature of regulators are updated every minute. 
Following is a Siddhi App the controls the temperature regulators if they are not already 'on' for all the rooms that 
have a room temperature greater than 30 degrees.  

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

Following are the supported operations of join clause.

 *  **Inner join (join)** 

    The default behaviour of join, use `join` as the keyword to join both the streams. The output is generated only if there is 
    a matching event in both the streams.

 *  **Left outer join** 

    Left outer join let you join two streams to be merged based on a condition. Use `left outer join` as the keyword to join both the streams.
    Here it returns all the events of left stream even if there are no matching events in the right stream by 
    having the right stream attribute values as nulls.

     **Example**

    The following query generates output events for all events from `StockStream` stream regardless of a matching 
    symbol being present in the `TwitterStream` stream.

    <pre>
    from StockStream#window.time(1 min) as S
      left outer join TwitterStream#window.length(1) as T
      on S.symbol== T.symbol
    select S.symbol as symbol, T.tweet, S.price
    insert into outputStream ;    </pre>

 *  **Right outer join** 

    This is similar to left outer join. Use `Right outer join` as the keyword to join both the streams.
    It returns all the events of the right stream even if there are no matching events in the left stream. 

 *  **Full outer join** 

    The full outer join combines the results of left outer join and right outer join. Use `full outer join` as the keyword to join both the streams.
    Here, output event are generated for each incoming event even if there are no matching events in the other stream.

    **Example**

    The following query generates output events for all the incoming events of each stream regardless of whether there is a 
    match for the `symbol` in the other stream or not.

    <pre>
    from StockStream#window.time(1 min) as S
      full outer join TwitterStream#window.length(1) as T
      on S.symbol== T.symbol
    select S.symbol as symbol, T.tweet, S.price
    insert into outputStream ;    </pre>



### Patterns

Patterns is a state machine implementation that allow you to detect event occurrence patterns over time. 
This can correlate events within a single stream or between multiple streams. 

**Purpose** 

Let you detect a specified event occurrence pattern over a time period. 

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
| Items | Description |
|-------------------|-------------|
| `->` | This represent followed by, i.e. After a event matching the first condition the next event matching the next condition arriving sometime after the first event (not necessarily need to be immediately after the previous event event) |
| `<event reference>` | Let you add a reference to the the matching event such that it can be accessed later for further processing. |
| `(within <time gap>)?` | `within` clause is optional, It defines the the duration within all the matching events should occur |
| `every` | `every` is an optional keyword. This defines weather the event matching should be triggered for every event arrival for the specified stream with the matching condition. <br/> When this keyword is not used the matching happens only ones. |

Siddhi also support pattern matching with counting events and matching events in a logical order such as (`and`, `or`, and `not`). Those are discussed in detail later.

**Example**

The following query sends an alert if the temperature of a room increases by 5 degrees within 10 min.

```sql
from every( e1=TempStream ) -> e2=TempStream[ e1.roomNo == roomNo and (e1.temp + 5) <= temp ]
    within 10 min
select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
```

Here for each events in `TempStream`, the matching process begins (because `every` is used with `e1=TempStream`), 
and if there is another event arrives within 10 minutes, having `temp` attribute greater then or equal to `e1.temp + 5` 
of the event e1, an output is generated via the `AlertStream`.

**Counting Pattern**

Counting pattern allows matching multiple events that may have been received for the same matching condition.
The number of events matched per condition can be limited via condition postfixes.

**Syntax**

Each matching condition can contain a collection of events with the minimum and maximum number of events to be matched as bellow. 

```sql
from (every)? <event reference>=<input stream>[<filter condition>] (<<min count>:<max count>>)? ->  
    ... 
    (within <time gap>)?     
select <event reference>([event index])?.<attribute name>, ...
insert into <output stream>
```

Postfix|Description|Example
---------|---------|---------|
`<n1:n2>`|This matches `n1` to `n2` events (including `n1` and excluding `n2`).|`1:4` matches 1 to 4 events.
`<n:>`|This matches `n` or more events (including `n`).|`<2:>` matches 2 or more events.
`<:n>`|This matches up to `n` events (excluding `n`).|`<:5>` matches up to 5 events.
`<n>`|This matches exactly `n` events.|`<5>` matches exactly 5 events.

Specific occurrences of the event in a collection can be retrieved by using event index along with its reference,
 square brackets can be used to indicate the event index where `1` can be used as the index of the first event and `last` can be used as the index
 for the `last` available event in the event collection. If you provided a index grater then the last event index
 the system returns `null`. Some valid examples are bellow.

+ `e1[3]` refers to the 3rd event.
+ `e1[last]` refers to the last event.
+ `e1[last - 1]` refers to the event before the last event.

**Example**

The following Siddhi App calculates temperature difference between two regulator events.

```sql
define stream TempStream (deviceID long, roomNo int, temp double);
define stream RegulatorStream (deviceID long, roomNo int, tempSet double, isOn bool);
  
from every( e1=RegulatorStream) -> e2=TempStream[e1.roomNo==roomNo]<1:> -> e3=RegulatorStream[e1.roomNo==roomNo]
select e1.roomNo, e2[0].temp - e2[last].temp as tempDiff
insert into TempDiffStream;
```
**Logical Patterns**

Logical pattern matches events that arrive in temporal order and correlates them with logical relationships such as `and`, 
`or` and `not`. 

**Syntax**

```sql
from (every)? (not)? <event reference>=<input stream>[<filter condition>] 
          ((and|or) <event reference>=<input stream>[<filter condition>])? (within <time gap>)? ->  
    ... 
select <event reference>([event index])?.<attribute name>, ...
insert into <output stream>
```

Keywords such as `and`, `or`, or `not` can used to illustrate the logical relationship.

Key Word|Description
---------|---------
`and`|This allows both conditions of `and` to be matched by two events in any order.
`or`|The state will succeed if either condition of `or` is satisfied. Here the event reference of the other condition will be `null`.
`not <condition1> and <condition2>`| When `not` with `and` is present, it let you identify an event matching `<condition2>` before any event match the `<condition1>`. 
`not <condition> for <time period>`| When `not` is present with `for`, it let you identify no event occurring that matches `<condition1>` for the given time  `<condition1>`.  is used to define a time period. E.g.`from not TemperatureStream[temp > 60] for 5 sec`. 

**Example**

Following Siddhi App, sends `stop` control action to regulator, when the key is removed from the hotel room. 
```sql
define stream RegulatorStateChangeStream(deviceID long, roomNo int, tempSet double, action string);
define stream RoomKeyStream(deviceID long, roomNo int, action string);

  
from every( e1=RegulatorStateChangeStream[ action == 'on' ] ) -> 
      e2=RoomKeyStream[ e1.roomNo == roomNo and action == 'removed' ] or e3=RegulatorStateChangeStream[ e1.roomNo == roomNo and action == 'off']
select e1.roomNo, ifThenElse( e2 is null, 'none', 'stop' ) as action
having action != 'none'
insert into RegulatorActionStream;
```

Following Siddhi App, alerts if we have switch off the regulator before temperature reaches 12 degrees.  

```sql
define stream RegulatorStateChangeStream(deviceID long, roomNo int, tempSet double, action string);
define stream TempStream (deviceID long, roomNo int, temp double);

from e1=RegulatorStateChangeStream[action == 'start'] -> not TempStream[e1.roomNo == roomNo and temp < 12] and e2=RegulatorStateChangeStream[action == 'off']
select e1.roomNo as roomNo
insert into AlertStream;
```

Following Siddhi App, alerts if the temperature did not reduce 12 degrees within 5 minutes of switching on the regulator.  

```sql
define stream RegulatorStateChangeStream(deviceID long, roomNo int, tempSet double, action string);
define stream TempStream (deviceID long, roomNo int, temp double);

from e1=RegulatorStateChangeStream[action == 'start'] -> not TempStream[e1.roomNo == roomNo and temp < 12] for '5 min'
select e1.roomNo as roomNo
insert into AlertStream;
```


### Sequences

Sequence is a state machine implementation that allow you to detect sequence of event occurrences over time. 
Here **all matching events need to arrive conservatively** to match the sequence condition,
 and there cannot be any non matching events arrived withing a matching sequence of events.
This can correlate events within a single stream or between multiple streams. 

**Purpose** 

Let you detect a specified event sequence over a time period. 

**Syntax**

The following is the syntax for a sequence query:

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
| `,` | This represent immediate next event, i.e. After a event matching the first condition the immediate next conservative event must match the next condition. |
| `<event reference>` | Let you add a reference to the the matching event such that it can be accessed later for further processing. |
| `(within <time gap>)?` | `within` clause is optional, It defines the the duration within all the matching events should occur |
| `every` | `every` is an optional keyword. This defines weather the event matching should be triggered for every event arrival for the specified stream with the matching condition. <br/> When this keyword is not used the matching happens only ones. |


**Example**

The following query sends an alert if there is more than 1 degree increase in the temperature between two consecutive temperature events.

```sql
from every e1=TempStream, e2=TempStream[e1.temp + 1 < temp]
select e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
```

**Counting Sequence**

Counting Sequence allows matching multiple events for the same matching condition.
The number of events matched per condition can be limited via condition postfixes as **Counting Patterns** or by using the 
`*`, `+` and `?` operators.

The matching events can also be retrieved using event indexes like in  **Counting Patterns**

**Syntax**

Each matching condition in a sequence can contain a collection of events as mentioned bellow. 

```sql
from (every)? <event reference>=<input stream>[<filter condition>](+|*|?)?, 
    <event reference>=<input stream [<filter condition>](+|*|?)?, 
    ... 
    (within <time gap>)?     
select <event reference>.<attribute name>, <event reference>.<attribute name>, ...
insert into <output stream>
```

|Postfix symbol|Description|
|---------|---------|
| `+` | Its optional, that machines **one or more** event occurrences of the for the given condition. |
| `*` | Its optional, that machines **zero or more** event occurrences of the for the given condition. |
| `?` | Its optional, that machines **zero or one** event occurrences of the for the given condition. |


**Example**

The following Siddhi App identifies temperature peeks.

```sql
define stream TempStream(deviceID long, roomNo int, temp double);
  
from every e1=TempStream, e2=TempStream[e1.temp <= temp]+, e3=TempStream[e2[last].temp > temp]
select e1.temp as initialTemp, e2[last].temp as peakTemp
insert into PeekTempStream;
```

**Logical Sequence**

Logical sequence identifies logical relationships using `and`, `or` and `not` on consecutively arriving events.

**Syntax**

```sql
from (every)? (not)? <event reference>=<input stream>[<filter condition>] 
          ((and|or) <event reference>=<input stream>[<filter condition>])? (within <time gap>)?, 
    ... 
select <event reference>([event index])?.<attribute name>, ...
insert into <output stream>
```

Keywords such as `and`, `or`, or `not` can used to illustrate the logical relationship same as in **Logical Patterns**. 

**Example**

Following Siddhi App notifies the state when a regulator event is immediately followed by both the temperature and humidity events. 

```sql
define stream TempStream(deviceID long, temp double);
define stream HumidStream(deviceID long, humid double);
define stream RegulatorStream(deviceID long, isOn bool);
  
from every e1=RegulatorStream, e2=TempStream and e3=HumidStream
select e2.temp, e3.humid
insert into StateNotificationStream;
```

### Output rate limiting

Output rate limiting allows queries to emit events periodically based on the condition specified.

**Purpose**

This allows you to limit the output to avoid overloading following executions and to remove unnecessary information.

**Syntax**

The following is the syntax of an output rate limiting configuration.

```sql
from <input stream> ...
select <attribute name>, <attribute name>, ...
output <rate limiting configuration>
insert into <output stream>
```
Three type of output rate limiting configuration are available, such as 

Rate limiting configuration|Syntax| Description
---------|---------|--------
Based on time | `<output event> every <time interval>` | Output `<output event>` for every given `<time interval>` time interval.
Based on number of events | `<output event> every <event interval> events` | Output `<output event>` for every `<event interval>` number of events.
Snapshot based output | `snapshot every <time interval>`| Output all events in the window (or the last event if no window is defined in the query) for every given `<time interval>` time interval.

Here the `<output event>` specifies which event(s) should be emitted as the output of the query. 
The possible values are as follows:
* `first` : Only the first event processed by the query in the specified time interval/sliding window is emitted.
* `last` : Only the last event processed by the query in the specified time interval/sliding window is emitted.
* `all` : All the events processed by the query in the specified time interval/sliding window are emitted. **When no `<output event>` is defined `all` will be used as default.**

**Examples**

+ Emitting events based on number of events

    Here the events are emitted every time the specified number of events arrive. You can also specify whether to emit only the first event, last event, or all events out of the events that arrived.
    
    In this example, the last temperature per sensor is emitted for every 10 events.
    
    <pre>
    from TempStreamselect 
    select temp, deviceID
    group by deviceID
    output last every 10 events
    insert into LowRateTempStream;    </pre>

+ Emitting events based on time

    Here events are emitted for every predefined time interval. You can also specify whether to to emit only the first event, last event, or all events out of the events that arrived during the specified time interval.

    In this example, emits all temperature events every 10 seconds  
      
    <pre>
    from TempStreamoutput 
    output every 10 sec
    insert into LowRateTempStream;    </pre>

+ Emitting a periodic snapshot of events

    This method works best with windows. When an input stream is connected to a window, snapshot rate limiting emits all the current events that have arrived and do not have corresponding expired events for every predefined time interval. 
    If the input stream is not connected to a window, only the last current event for each predefined time interval is emitted.
    
    The following query emits snapshot of the events in a time window of 5 seconds every 1 second. 

    <pre>
    from TempStream#window.time(5 sec)
    output snapshot every 1 sec
    insert into SnapshotTempStream;    </pre>
    

## Partitions

Partitions divides streams and queries into isolated groups to process them in parallel and in isolation. 
A partition can contain one or more queries and there will be multiple instances of the same queries and streams replicated for each partition. 
Each partition is tagged with a partition key those partitions only process the events that matches the corresponding partition key. 

**Purpose** 

Partition let you process the events groups in isolation such that event processing can be performed using the same set of queries for each group. 

**Partition key generation**

Partition key can be generation in two ways. 

* Partition by value
  
    This is created by using an attribute value of the input stream.

    **Syntax**
    
    <pre>
    partition with ( &lt;attribute name> of &lt;stream name>, &lt;attribute name> of &lt;stream name>, ... )
    begin
        &lt;query>
        &lt;query>
        ...
    end; </pre>
    
    **Example**
    
    The following query calculates the maximum temperature recorded within the last 10 events per `deviceID`.
    
    <pre>
    partition with ( deviceID of TempStream )
    begin
        from TempStream#window.length(10)
        select roomNo, deviceID, max(temp) as maxTemp
        insert into DeviceTempStream;
    end;
    </pre>

* Partition by range

    This is created by mapping each partition key to a range condition of the input stream's numerical attribute.

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
    
    The following query calculates the average temperature for the last 10 minutes per office area.
    
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

### Inner Streams 

Queries inside a partition block can use inner streams to communicate between each other while preserving partition isolation.
Inner streams are denoted by a "#" in front of them, and these streams cannot be accessed outside of the partition block. 

**Purpose**
  
Inner streams let you connect queries within the partition block such that output of a query will only feed to a query 
that's in the same partition. With this you don't need to repartition the streams if they are communicating within the partition.

**Example**

For each sensor, following partition calculates the average temperature over every 10 events
and produces output to `DeviceTempIncreasingStream` is the consecutive average temperature values are increasing more then 
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

## Tables

A table is a stored version of an stream or a table of events. It's schema is defined via the **table definition** thats
quite similar to a stream definition. These events are by default stored 
`in-memory` but Siddhi also provides store extension to work with data/events stored in various data stores through the 
table abstraction.

**Purpose**

Tables allow Siddhi to work with stored events. By defining a schema to table Siddhi enables them to be processed at queries using their defined attributes with the streaming data, and also interactively query the state of the stored events in the table.

**Syntax**

The following is the syntax for defining a new table.

```sql
define stream <stream name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );
```
The following parameters are configured in a table definition.

| Parameter     | Description |
| ------------- |-------------|
| `table name`      | The name of the table defined. (as a convention `PascalCase` is used for table name) |
| `attribute name`   | The schema of the table is defined by its attributes by uniquely identifiable attribute names (as a convention `camalCase` is used for attribute names)|    |
| `attribute type`   | The type of each attribute defined in the schema. <br/> This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOL` or `OBJECT`.     |


**Example**

Defining a table named `RoomTypeTable` with the attributes `roomNo` with type `int`, and `type` with type `string`.

```sql
define table RoomTypeTable ( roomNo int, type string );
```

**Primary Keys**

Tables can be configured with primary keys to avoid the duplication of data. 

Primary keys are configured by including the `@PrimaryKey( 'key1', 'key2' )` annotation to the table definition. 
Each event table configuration can have only one `@PrimaryKey` annotation. 
The number of attributes supported differ based on the table implementations and when more then one attribute 
 is used for primary key the combination of the keys will be used to grantee uniqueness among the events saved in the table.

**Examples**

The following query creates an event table with the `symbol` attribute as the primary key. 
Therefore each entry in this table will have a unique value for `symbol` attribute.

```sql
@PrimaryKey('symbol')
define table StockTable (symbol string, price float, volume long);
```

**Indexes**
 
Indexes let tables to searched/modified much faster. 

Indexes are configured by including the `@Index( 'key1', 'key2' )` annotation to the table definition.
 Each event table configuration can have at most only one `@Index` annotation. 
 Support for `@Index` annotation and the number of attributes supported differ based on the table implementations. 
 When more then one attribute used for index each of them will be used to index the table for fast access of the data. 
 Indexes can be configured together with primary keys. 

**Examples**

The following query creates an indexed event table named `RoomTypeTable` using attribute `roomNo` as the index key.

```sql
@Index('roomNo')
define table RoomTypeTable (roomNo int, type string);
```

**Operators on Table**

The following operators can be performed on tables.


### Insert

This allows events to be inserted in to tables. This is similar to inserting events into streams. 

!!! warning
    If the table is defined with primary keys, and if you insert duplicate data primary key constrain violation can occur. 
    In such cases use the `update or insert` operation. 
    
**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
insert into <table>
```

Like in streams to insert only the specific output event types, use the `current events`, `expired events` or the `all events` keyword between `insert` and `into` keywords. 
For more information refer [output event type](http://127.0.0.1:8000/documentation/siddhi-4.0/#output-event-types) section.

**Example**

The following query inserts all events from the `TempStream` stream to the `TempTable` table.

```sql
from TempStream
select *
insert into TempTable;
```

### Join (Table)

To allow a stream to retrieve information from a table in a streaming manner.

!!! Note
    Join can also be performed with [two streams](#join-stream), [aggregation](#join-aggregation) or against externally [defined windows](#join-window).

**Syntax**

```sql
from <input stream> join <table>
    on <condition>
select (<input stream>|<table>).<attribute name>, (<input stream>|<table>).<attribute name>, ...
insert into <output stream>
```

!!! Note 
    Table can only be joint with a stream, and two tables cannot be joined because there should be at least one active 
    entity to trigger the join operation.

**Example**

The following Siddhi App performs a join to retrieve the room type from `RoomTypeTable` table based on the room number, such that it can filter the events related to `server-room`s.

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
 
To execute delete for specific output event types use the `current events`, `expired events` or the `all events` keyword with `for` as shown
in the syntax. To understand more refer [output event type](http://127.0.0.1:8000/documentation/siddhi-4.0/#output-event-types) section.

!!! note 
    Table attributes should be always referred to with the table name as `<table name>.<attibute name>`.
    
**Example**

The following script deletes entries from the `RoomTypeTable` table, 
that have room number equal to the `roomNumber` attribute of the events in `DeleteStream` stream.

```sql
define table RoomTypeTable (roomNo int, type string);

define stream DeleteStream (roomNumber int);
  
from DeleteStream
delete RoomTypeTable
    on RoomTypeTable.roomNo == roomNumber;
```

### Update

Update selected event attributes stored in a table based on a condition. 

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
update <table> (for <output event type>)? 
    set <table>.<attribute name> = <attribute name>, <table>.<attribute name> = <attribute name>, ...
    on <condition>
```

The `condition` element specifies the basis on which events are selected for update.
When specifying the `condition`, table attributes should be referred to with the table name.

Use `set` keyword to update selected attributes from the table. Here for each assignment the left hand side 
 attribute should be a table attribute and the right hand side attribute can be stream/table attribute, mathematical 
 operations or other. When `set` clause is not provided all attributes in the table will be updated.  
 
To execute update upon specific output event types use the `current events`, `expired events` or the `all events` keyword with `for` as shown
in the syntax. To understand more refer [output event type](http://127.0.0.1:8000/documentation/siddhi-4.0/#output-event-types) section.

!!! note 
    Table attributes should be always referred to with the table name as `<table name>.<attibute name>`.

**Example**

Siddhi app updating room occupancy in the `RoomOccupancyTable` table for each room number based on new arrival and exit from the `UpdateStream` stream.

```sql
define table RoomOccupancyTable (roomNo int, people int);
define stream UpdateStream (roomNumber int, arrival int, exit int);
  
from UpdateStream
select *
update RoomTypeTable
    set RoomTypeTable.people = RoomTypeTable.people + arrival - exit
    on RoomTypeTable.roomNo == roomNumber;
```

### Update or Insert

Let you update if the event attributes already exist in the table based on a condition, or 
else insert the entry as a new attribute.

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
update or insert <table> (for <output event type>)? 
    set <table>.<attribute name> = <attribute name>, <table>.<attribute name> = <attribute name>, ...
    on <condition>
```
The `condition` element specifies the basis on which events are selected for update.
When specifying the `condition`, table attributes should be referred to with the table name. 
Here if no stored event matches for the given condition then the arriving event will be inserting into 
the table.

The `set` clause is only used when the update operation is performed and it will during the insert operation. 
When ing the `set` clause assignments their the left hand side attribute should be always a table attribute and 
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
update or insert RoomAssigneeTable
    set RoomAssigneeTable.assignee = assignee
    on RoomAssigneeTable.roomNo == roomNo;
```

### In
 
Let the stream check if the expected value exist in the table as part of a conditional operation.

**Syntax**

```sql
from <input stream>[<condition> in <table>]
select <attribute name>, <attribute name>, ...
insert into <output stream>
```

The `condition` element specifies the basis on which events are selected to be compared. 
When constructing the `condition`, the table attribute should be always referred to with the table name as 
`<table>.<attibute name>`.

**Example**

The Siddhi App to filter only room numbers that are listed in the `ServerRoomTable` table.

```sql
define table ServerRoomTable (roomNo int);
define stream TempStream (deviceID long, roomNo int, temp double);
   
from TempStream[ServerRoomTable.roomNo == roomNo in ServerRoomTable]
insert into ServerRoomTempStream;
```

## Incremental Aggregations

Incremental aggregation let you obtaining aggregates in an incremental manner for a specified set of time periods.

This not only let you calculate aggregations with varies time granularity but also let you access them in an interactive
 manner for reports, dashboards, and for further processing. It's schema is defined via the **aggregation definition**.

**Purpose**

Incremental aggregation allows you to retrieve the aggregate value for different time durations. 
That is, it allows you to obtain aggregates such as `sum`, `count`, `avg`, `min`, `max`, and `count`) 
of stream attributes for durations such as `sec`, `min`, `hour`, etc. 

This is of considerable importance in many analytics scenarios since aggregate values are often needed for several time periods. 
Furthermore, this ensures that the aggregations are not lost due to unexpected system failures, as the aggregates can be stored in different persistence `stores`.

**Syntax**

```sql
@store(type="<store type>", ...)
define aggregation <aggregator name>
from <input stream>
select <attribute name>, <aggregate function>(<attribute name>) as <attribute name>, ...
    group by <attribute name>
    aggregate by <timestamp attribute> every <time periods> ;
```
The above syntax includes the following:

|Item                |Description
---------------      |---------
|`@store`            |This annotation is used to refer to the data store where the calculated <br/>aggregate results will be stored. This annotation is optional and when <br/>no annotation is provided the data will be sored in the `in-memory` store.
|`<aggregator name>` | Specifies a unique name for the aggregation such that it can be referred <br/>when accessing aggregate results. 
|`<input stream>`    |The stream that feeds the aggregation. **Note! this stream should be <br/>already defined.**
|`group by <attribute name>`|The `group by` clause is optional. If it's given the aggregate values <br/>would be calculated, per each `group by` attribute, and otherwise all the<br/> events would be aggregated together. 
|`by <timestamp attribute>`| This clause is optional. This defined the attribute that should be used as<br/> the timestamp if this is not provided the event time will be used by default.<br/> The timestamp could be given as either a string or long value. If it's a `long`,<br/> the unix timestamp in milliseconds is expected (e.g. `1496289950000`). If it's <br/>a `string` the supported formats are `<yyyy>-<MM>-<dd> <HH>:<mm>:<ss>` <br/>(if time is in GMT) and  `<yyyy>-<MM>-<dd> <HH>:<mm>:<ss> <Z>` (if time is <br/>not in GMT), here the ISO 8601 UTC offset must be provided for `<Z>` <br/>(ex. `+05:30`, `-11:00`).
|`<time periods>`    |This depicts the aggregation period. This can be defined either as a range <br/>such as `evey sec ... year` defining that it should process all time <br/>granularities form second to year, or as comma separated values such as <br/> `every sec, hour, month`. Aggregation supports `second`, `minute`, `hour`, <br/>`day`, `month` and `year` as its time granularity levels. 

**Example**

Siddhi App defining `TradeAggregation` to calculate  average and sum of `price` from `TradeStream` stream every second to year time granularities.

```sql
define stream TradeStream (symbol string, price double, volume long, timestamp long);

define aggregation TradeAggregation
  from TradeStream
  select symbol, avg(price) as avgPrice, sum(price) as total
    group by symbol
    aggregate by timestamp every sec ... year;
```

### Join (Aggregation)

Join allow a stream to retrieve calculated aggregate values from the aggregation. 

!!! Note
    Join can also be performed with [two streams](#join-stream), with [table](#join-table) or against externally [defined windows](#join-window).


**Syntax**

Join with aggregation is similer to the join with [table](#join-table), but with additional `within` and `per` clauses. 

```sql
from <input stream> join <aggrigation> 
  on <join condition> 
  within <time range> 
  per <time granularity>
select <attribute name>, <attribute name>, ...
insert into <output stream>;
```
Apart for constructs of [table](#join-table) this includes the following :

Item|Description
---------|---------
`within  <time range>`| This allows you to specify the time interval for which the aggregate values need to be retrieved. This can be specified by providing the start and end time separated by comma as `string` or `long` values or by using wildcard `string` specifying the data range. For details refer examples.            
`per <time granularity>`|This specifies the time granularity by which the aggregate values must be grouped and returned. E.g., If you specify `days`, the retrieved aggregate values are grouped for each day within the selected time interval.

`within` and `par` clauses also accept attribute values from the stream.

**Example**

Retrieving all aggregation per day within the time range `"2014-02-15 00:00:00 +05:30", "2014-03-16 00:00:00 +05:30"` 

```sql
define stream StockStream (symbol string, value int);

from StockStream as S join TradeAggregation as T
  on S.symbol == T.symbol 
  within "2014-02-15 00:00:00 +05:30", "2014-03-16 00:00:00 +05:30" 
  per "days" 
select S.symbol, T.total, T.avgPrice 
insert into AggregateStockStream;
```

Retrieving all aggregation per hour within the day `2014-02-15`  

```sql
define stream StockStream (symbol string, value int);

from StockStream as S join TradeAggregation as T
  on S.symbol == T.symbol 
  within "2014-02-15 **:**:** +05:30"
  per "hours" 
select S.symbol, T.total, T.avgPrice 
insert into AggregateStockStream;
```

Retrieving all aggregation per an attribute value from the stream and within timestamps `1490918400` and `1490922000`  

```sql
define stream StockStream (symbol string, value int, perValue string);

from StockStream as S join TradeAggregation as T
  on S.symbol == T.symbol 
  within 1490918400, 1490922000  
  per S.perValue
select S.symbol, T.total, T.avgPrice 
insert into AggregateStockStream;
```

## _(Defined)_ Windows 

A defined window is a window that can be shared across multiple queries. 
Events can be inserted to a defined window from one or more queries and it can produce output events based on the defined window type.
 
**Syntax**

The following is the syntax for a defined window.

```sql
define window <window name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... ) <window type>(<parameter>, <parameter>, …) <output event type>;
```

The following parameters are configured in a table definition.

| Parameter     | Description |
| ------------- |-------------|
| `window name`      | The name of the window defined. (as a convention `PascalCase` is used for window name) |
| `attribute name`   | The schema of the window is defined by its attributes by uniquely identifiable attribute names (as a convention `camalCase` is used for attribute names)|    |
| `attribute type`   | The type of each attribute defined in the schema. <br/> This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT`, `BOOL` or `OBJECT`.     |
| `<window type>(<parameter>, ...)`   | The window type associated with the window and its parameters.     |
| `output <output event type>` | This is optional, Keywords like `current events`, `expired events` and `all events` (the default) can be used to manipulate when the window output should be exposed. For information refer section Output event types. 

**Examples**
 
+ Returning all output when event arrives and when events expire from the window.

    In the following query, output event type is not specified therefore, it emits both current and expired events as the output.
    
    <pre>
    define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second); </pre>

+ Returning a only when events  expire from the window.

    In the following query, the window's output event type is `all events`. Therefore, it emits only the window expiry events as output.
    
    <pre>
    define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second) output expired events; </pre>
     

**Operators on Defined Windows**

The following operators can be performed on defined windows.

### Insert

This allows events to be inserted in to windows. This is similar to inserting events into streams. 

**Syntax**

```sql
from <input stream> 
select <attribute name>, <attribute name>, ...
insert into <window>
```

Like in streams to insert only the specific output event types, use the `current events`, `expired events` or the `all events` keyword between `insert` and `into` keywords. 
For more information refer [output event type](#output-event-types) section.

**Example**

The following query inserts all events from the `TempStream` stream to the `OneMinTempWindow` window.

```sql
from TempStream
select *
insert into OneMinTempWindow;
```

### Join (Window)

To allow a stream to retrieve information from a window based on a condition.

!!! Note
    Join can also be performed with [two streams](#join-stream), [aggregation](#join-aggregation) or with tables [tables](#join-table).

**Syntax**

```sql
from <input stream> join <window>
    on <condition>
select (<input stream>|<window>).<attribute name>, (<input stream>|<window>).<attribute name>, ...
insert into <output stream>
```

**Example**

The following Siddhi App performs a join count the number of temperature events having more then 40 degrees 
 within the last 2 minutes. 

```sql
define window TwoMinTempWindow (roomNo int, temp double) time('2 min');
define stream CheckStream (requestId string);
   
from CheckStream as C join TwoMinTempWindow as T
    on T.temp > 40
select requestId, count(T.temp) as count
insert into HighTempCountStream;
```

### From

A window can be also be used as input to any query like streams. 

Note !!!
     When window is used as input to a query, another window cannot be applied on top of this.

**Syntax**

```sql
from <window> 
select <attribute name>, <attribute name>, ...
insert into <output stream>
```

**Example**
The following Siddhi App calculates the maximum temperature within last 5 minutes.

```sql
define window FiveMinTempWindow (roomNo int, temp double) time('5 min');


from FiveMinTempWindow
select name, max(value) as maxValue, roomNo
insert into MaxSensorReadingStream;
```

## Triggers

Triggers allow events to be periodically generated. **Trigger definition** can be used to define a trigger. 
Trigger also works like a stream with a predefined schema.

**Purpose**

For some use cases the system should be able to periodically generated events based on specified time interval to perform 
some periodic executions. 

Trigger can be performed during three times such as `'start'`, for a given `<time interval>` or for a given `'<cron expression>'`, 


**Syntax**

The following is the syntax for an trigger definition.

```sql
define trigger <trigger name> at ('start'| every <time interval>| '<cron expression>');
```

Triggers can be used as inputs like streams and they adhere to the following stream definition, and produces `triggered_time` attribute with type `long`

```sql
define stream <trigger name> (triggered_time long);
```

Types of triggers supported as following

|Trigger type| Description|
|-------------|-----------|
|`'start'`| An event will be trigger at Siddhi start.|
|`every <time interval>`| an event will be triggered periodically on the given time interval.
|`'<cron expression>'`| an event will be triggered periodically based on the given cron expression, refer  <a target="_blank" href="http://www.quartz-scheduler.org/documentation/quartz-2.2.x/tutorials/tutorial-lesson-06">quartz-scheduler</a> for config details.
 

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

## Scripts

Script allows you to write functions in other programming languages and execute them within queries. 
Functions defined via script can be accessed in queries just like any other inbuilt functions. 
**Function definitions** can be used to define these scripts.

Function parameters are passed as `Object[]` with the name `data` into the function logic. 

**Purpose**

Scripts let you define a function operation that not provided in Siddhi core or it's extension and let you define 
the function logic without focusing you to write an extension.

**Syntax**

The following is the syntax for a Script definition.

```sql
define function <function name>[<language name>] return <return type> {
    <operation of the function>
};
```

The following parameters are configured when defining a script.

| Parameter     | Description |
| ------------- |-------------|
| `function name`| 	The name of the function (as a convention `camalCase` is used for function name).|
|`language name`| The name of the programming language used to define the script, such as `javascript`, `r` and `scala`.|
| `return type`| The return attribute type of the the function. It can be `int`, `long`, `float`, `double`, `string`, `bool` or `object`. Here the function implementer should be responsible for returning the output attribute on the defined return type for proper functionality. 
|`operation of the function`| Here, the execution logic of the function is added. This logic should be written in the language specified under the `language name`, and it should return the output on the specified `return type` parameter.

**Examples**

The following query performs concatenation using JavaScript and returns the output as a string.

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
different type of functionality, and for come cases there can be gaps and you need to write the functionality by yourself.

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

    For each event, it consumes zero or more parameters as input parameters, and outputs a single attribute. This could be used to manipulate existing event attributes to generate new attributes like any Function operation.
    
    This is implemented by extending `org.wso2.siddhi.core.executor.function.FunctionExecutor`.
    
    Example : 
    
    `math:sin(x)` here the `sin` function of `math` extension will return the sin value for parameter `x`.
    
* **Aggregate Function**

    For each event, it consumes zero or more parameters as input parameters, and outputs a single attribute having an aggregated results. This could be used with conjunction with a window in order to find the aggregated results based on the given window like any Aggregate Function operation. 
    
     This is implemented by extending `org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator`.

    Example : 
    
    `custom:std(x)` here the `std` aggregate function of `custom` extension will return the standard deviation of value `x` based on it's assigned window query. 

* **Window** 

    Allows events to be **collected, generated, dropped and expired anytime** **without altering** the event format based on the given input parameters, like any other Window operator. 
    
    This is implemented by extending `org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor`.

    Example : 
    
    `custom:unique(key)` here the `unique` window of `custom` extension will retain one event for each unique `key` parameter.

* **Stream Function**

    Allows events to be  **generated or dropped only during event arrival** and **altered** by adding one or more attributes to it. 
    
    This is implemented by extending  `org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor`.
    
    Example :  
    
    `custom:pol2cart(theta,rho)` here the `pol2cart` function of `custom` extension will return all events by calculating the cartesian coordinates `x` & `y` and adding them as new attributes to the events.

* **Stream Processor**
    
    Allows events to be **collected, generated, dropped and expired anytime** with **altering** the event format by adding one or more attributes to it based on the given input parameters. 
    
    Implemented by extending "oorg.wso2.siddhi.core.query.processor.stream.StreamProcessor".
    
    Example :  
    
    `custom:perMinResults(<parameter>, <parameter>, ...)` here the `perMinResults` function of `custom` extension will return all events by adding one or more attributes the events based on the conversion logic and emit output every minute despite of event arrivals.

* **Sink**

* **Source**

* **Store**

* **Script**

**Example**

A window extension created with namespace `foo` and function name `unique` can be referred as follows:

```sql
from StockExchangeStream[price >= 20]#window.foo:unique(symbol)
select symbol, price
insert into StockQuote
```

**Available Extensions**

Siddhi currently have several pre written extensions <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">here</a>
 
_We value your contribution on improving Siddhi and its extensions further._


**Writing Custom Extensions**

Custom extensions can be written in order to cater use case specific logic that are not out of the box available in Siddhi or as an existing extension. 

More information on this will be available soon.