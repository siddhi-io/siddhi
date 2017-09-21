# Siddhi Streaming SQL Guide 4.0

## Introduction

Siddhi Streaming SQL is designed to process event streams in streaming manner, detect complex event occurrences, 
and notify them in real-time. 

## Siddhi Application
Streaming processing and Complex Event Processing rules can be written is Siddhi Streaming SQL and they can be put 
together as `SiddhiApp` in one file. 

**Purpose**

Each Siddhi Application is an isolated processing unit that let you deploy and 
execute queries independent of other SiddhiApps in the system.

The flowing diagram depicts how **event flows** within some of the key Siddhi Streaming SQL elements 
of the Siddhi Application.

![Event Flow](../images/event-flow.png?raw=true "Event Flow")

Below table provides brief description of a few key elements in the Siddhi Streaming SQL Language.

| Elements     | Description |
| ------------- |-------------|
| Stream    | A logical series of events ordered in time with a uniquely identifiable name and set of defined typed attributes defining it's schema |
| Event     | An event is associated with only one stream, and all events of that stream have an identical set of attributes assigned specific types (or the same schema). An event contains a timestamp and set of attribute values according to the schema.|
| Table     | A structured representation of stored data with a defined schema. Stored data can be backed by `In-Memory`, `RDBMs`, `MongoDB`, etc to be accessed and manipulated at runtime.
| Query	    | A logical construct that process events in streaming manner by combining existing streams and/or tables, and generates events to output stream or table. A query consumes one or more input streams and zero or one table, process those events in a streaming manner, publishes the output events to stream or tables for further processing or notifications. 
| Source    | A contract that consumes data from external sources (such as `TCP`, `Kafka`, `HTTP`, etc), converts it's data format (such as `XML`, `JSON`, `binary`, etc) to Siddhi event, and passes that to a Stream for processing.
| Sink      | A contract that takes events arriving at a Stream, map them to a predefined data format (such as `XML`, `JSON`, `binary`, etc), and publish them to external endpoints (such as `E-mail`, `TCP`, `Kafka`, `HTTP`, etc).
| Input Handler | A mechanism to programmatically inject events into Streams. |
| Stream/Query Callback | A mechanism to programmatically consumes output events from Streams and Queries. |
| Partition	| A logical container that isolates processing of queries based on partition keys. Here separate instance of queries will be generated for each partition key to achieve isolation. 
| Inner Stream | A positionable stream that connects portioned queries within their partitions preserving isolation.  

**Grammar**

The element of Siddhi SQL can be composed together as a script in Siddhi App, Here each construct should be separated 
by a semicolon ( ; ). 

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

**Output event type**

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
    
```sql
from TempStreamselect 
select temp, deviceID
group by deviceID
output last every 10 events
insert into LowRateTempStream;
```

+ Emitting events based on time

    Here events are emitted for every predefined time interval. You can also specify whether to to emit only the first event, last event, or all events out of the events that arrived during the specified time interval.

    In this example, emits all temperature events every 10 seconds    
```sql
from TempStreamoutput 
output every 10 sec
insert into LowRateTempStream;
```

+ Emitting a periodic snapshot of events

    This method works best with windows. When an input stream is connected to a window, snapshot rate limiting emits all the current events that have arrived and do not have corresponding expired events for every predefined time interval. 
    If the input stream is not connected to a window, only the last current event for each predefined time interval is emitted.
    
    The following query emits snapshot of the events in a time window of 5 seconds every 1 second. 

```sql
from TempStream#window.time(5 sec)
output snapshot every 1 sec
insert into SnapshotTempStream;
```

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

**Example**

The following creates a table named `RoomTypeTable` with the attributes `roomNo` with `int` type, `type` with `string` type.

```sql
define table RoomTypeTable ( roomNo int, type string );
```

**Primary Keys and Indexes**

Event tables can be configured with primary keys to avoid the duplication of data, and indexes for fast event access.

Primary keys are configured by including the `@PrimaryKey` annotation to the table configuration. The combined value of the primary key attribute should be unique for each entry saved in the table. This ensures that entries in the table are not duplicated.

Indexes are configured by including the `@Index` annotation within the event table configuration. Each event table configuration can have only one `@Index` annotation. However, multiple attributes can be specified as index attributes via a single annotation. When the `@Index` annotation is defined, multiple entries can be stored for a given key in the table. Indexes can be configured together with primary keys. 


**Examples**

+ Configuring primary keys
The following query creates an event table with the `symbol` attribute defined as the primary key. Therefore, each entry in this table should have a unique value for the `symbol` attribute.

```sql
@PrimaryKey('symbol')
define table StockTable (symbol string, price float, volume long);
```

+ Configuring indexes

The following query creates an indexed event table named `RoomTypeTable` with the attributes `roomNo` (as an `INT` attribute) and `type` (as a `STRIN`G attribute). All entries in the table are to be indexed by the `roomNo` attribute.

```sql
@Index('roomNo')
define table RoomTypeTable (roomNo int, type string);
```



#### Join



**Syntax**

```sql
from <input stream name>#window.length(1) join <table_name>
    on <input stream name>.<attribute name> <condition> <table_name>.<table attribute name>
select <input stream name>.<attribute name>, <table_name>.<table attribute name>, ...
insert into <output stream name>
```

At the time of joining, the event table should not be associated with window operations because an event table is not an active construct. Two event tables cannot be joined with each other due to the same reason.
 
**Purpose**

To allow a stream to retrieve information from an event table.
 
**Parameters**

**Example**

The following query performs a join to update the room number of the events in the `TempStream` stream with that of the corresponding events in the `RoomTypeTable` event table, and then inserts the updated events into the `EnhancedTempStream` stream.
```sql
define table RoomTypeTable (roomNo int, type string);
define stream TempStream (deviceID long, roomNo int, temp double);
   
from TempStream join RoomTypeTable
    on RoomTypeTable.roomNo == TempStream.roomNo
select deviceID, RoomTypeTable.roomNo as roomNo, type, temp
insert into EnhancedTempStream;
```

### Supported Event Table Operators

The following event table operators are supported for Siddhi.

#### Insert into

**Syntax**

```sql
from <input stream name> 
select <attribute name>, <attribute name>, ...
insert into <table name>
```
To insert only the specified output event category, use the `current events`, `expired events` or the `all events` keyword between `insert` and `into` keywords. For more information, see Output Event Categories.

**Purpose**

To store filtered events in a specific event table.

**Parameters**

+ `input stream name`: The input stream from which the events are taken to be stored in the event table.

+ `attribute name`: Attributes of the chosen events that are selected to be saved in the event table.

+ `table name`: The name of the event table in which the events should be saved.

**Example**

The following query inserts all the temperature events from the `TempStream` event stream to the `TempTable` event table.

```sql
from TempStream
select *
insert into TempTable;
```
#### Delete

**Syntax**

```sql
from <input stream name> 
select <attribute name>, <attribute name>, ...
delete <table name>
    on <condition>
```

The `condition` element specifies the basis on which events are selected to be deleted. When specifying this condition, the attribute names should be referred to with the table name.
To delete only the specified output category, use the `current events`, `expired events` or the `all events` keyword. For more information, see Output Event Categories.

**Purpose**

To delete selected events that are stored in a specific event table.

**Parameters**

+ `input stream name`: The input stream that is the source of the events stored in the event table.

+ `attribute name`: Attributes to which the given condition is applied in order to filter the events to be deleted.

+ `table name`: The name of the event table from which the filtered events are deleted.

+ `condition`: The condition based on which the events to be deleted are selected.

**Example**

The following query deletes all the entries in the `RoomTypeTable` event table that have a room number that matches the room number in any event in the `DeleteStream` event stream.

```sql
define table RoomTypeTable (roomNo int, type string);
define stream DeleteStream (roomNumber int);
  
from DeleteStream
delete RoomTypeTable
    on RoomTypeTable.roomNo == roomNumber;
```

#### Update

**Syntax**

```sql
from <input stream name> 
select <attribute name> as <table attribute name>, <attribute name> as <table attribute name>, ...
update <table name>
    on <condition>
```

The `condition` element specifies the basis on which events are selected to be updated. When specifying this condition, the attribute names should be referred to with the table name.
To update only the specified output category, use the `current events`, `expired events` or the `all events` keyword. For more information, see Output Event Categories.

**Purpose**

To update selected events in an event table.

**Parameters**

+ `input stream name`: The input stream that is the source of the events stored in the event table.

+ `attribute name`: Attributes to which the given `condition` is applied in order to filter the events to be updated.

+ `table name`: The name of the event table in which the filtered events should be updated.

+ `condition`: The condition based on which the events to be updated are selected.

**Example**

The following query updates room type of all the events in the `RoomTypeTable` event table that have a room number that matches the room number in any event in the `UpdateStream` event stream.

```sql
define table RoomTypeTable (roomNo int, type string);
define stream UpdateStream (roomNumber int, roomType string);
  
from UpdateStream
select roomType as type
update RoomTypeTable
    on RoomTypeTable.roomNo == roomNumber;
```

#### Insert Overwrite

**Syntax**

```sql
from <input stream name> 
select <attribute name> as <table attribute name>, <attribute name> as <table attribute name>, ...
insert overwrite <table name>
    on <condition>
```

The `condition` element specifies the basis on which events are selected to be inserted or overwritten. When specifying this condition, the attribute names should be referred to with the table name.
When specifying the `table attribute` name, the attributes should be specified with the same name specified in the event table, allowing Siddhi to identify the attributes that need to be updated/inserted in the event table.

**Purpose**

**Parameters**

+ `input stream name`: The input stream that is the source of the events stored in the event table.

+ `attribute name`: Attributes to which the given `condition` is applied in order to filter the events to be inserted or over-written.

+ `table name`: The name of the event table in which the filtered events should be inserted or over-written.

+ `condition` : The condition based on which the events to be inserted or over-written are selected.

**Example**

The following query searches for events in the `UpdateTable` event table that have room numbers that match the same in the `UpdateStream` stream. When such events are founding the event table, they are updated. When a room number available in the stream is not found in the event table, it is inserted from the stream.
 
 ```sql
 define table RoomTypeTable (roomNo int, type string);
 define stream UpdateStream (roomNumber int, roomType string);
   
 from UpdateStream
 select roomNumber as roomNo, roomType as type
 insert overwrite RoomTypeTable
     on RoomTypeTable.roomNo == roomNo;
 ```
#### In
 
**Syntax**

```sql
<condition> in <table name>
```

The `condition` element specifies the basis on which events are selected to be inserted or overwritten. When specifying this condition, the attribute names should be referred to with the table name.

**Purpose**
 
**Parameters**

**Example**

```sql
define table ServerRoomTable (roomNo int);
define stream TempStream (deviceID long, roomNo int, temp double);
   
from TempStream[ServerRoomTable.roomNo == roomNo in ServerRoomTable]
insert into ServerTempStream;
```

## Time based aggregation
Time-based aggregation involves obtaining aggregate attribute values (i.e., sum, average, min, max etc.) for a specified time period.

### Calculating and storing time-based aggregated values
This section explains how to write Siddhi queries to calculate aggregate values for specific time periods as required.

**Syntax**
```sql
@store(type="<DATABASE_TYPE>")
define aggregation <aggregatorName>
from <InputStreamName>
select <attributeName>, <aggregate_function>(attributeName) as <attributeName>, <aggregate_function>(attributeName) as <attributeName> ...
    group by <attributeName>
    aggregate by timestamp every <time_period>;
```
The above syntax includes the following:
Item|Description
---------|---------
`@store`|This annotation is used to refer to the data source where the events for which aggregate values are to be calculated are stored.
`define aggregation`|This specifies a unique name for the aggregation
`group by`|The attribute by which the calculated aggregate values are grouped. Specifying an attribute to group by is optional. When an attribute is specified, the aggregate values are calculated for the required time periods per value for the specified attribute. If no attribute is specified, all the events are aggregated together.
`aggregate by timestamp`|The time period for which the aggregate values are calculated. This is an optional parameter. If the time period is determined by an external timestamp (i.e., the timestamp specified as the value for the `_timestamp` attribute in the event), specific timestamps must be specified in the query with the `within` operator using supported formats (i.e., `<yyyy>-<MM>-<dd>`, `<HH>:<mm>:<ss>`, `<Z>` (if time is not in GMT), and `<yyyy>-<MM>-<dd> <HH>:<mm>:<ss>` (if time is in GMT). If the time period is to be determined based on the system time, you can specify the time duration for which the aggregate values should be calculated (e.g., `aggregate every sec...year` calculates aggregate values for the last second, minute, hour, day, month and year in a sliding manner.).


**Example**
```sql
@store(type="rdbms")
define aggregation testAggregator
from tradesStream
select symbol, avg(price) as avgPrice, sum(price) as total
    group by symbol
    aggregate by timestamp every sec...year;
```
In this query, an aggregator named `testAggregator`calculates the average price and the sum of prices of the events that arrive at the `tradesStream` stream every second. These average and total are calculated per symbol, and in each second, the average and sum relevant for the last second, minute, hour, day, month, and year are output, and stored in the RDBMS database.

### Retrieving aggregate values
This section explain how to retrieve aggregate values that are already calculated and persisted in the system.
**Syntax**
```sql
define stream `InputStreamName` (<attributeName> <ATTRIBUTE_TYPE>, <attributeName> <ATTRIBUTE_TYPE> ...);

from InputStreamName as b join <AGGREGATION_NAME> as a
on a.symbol == b.symbol 
within "<START_TIME>", "<END_TIME>" 
per "<TIME_PERIOD>" 
select a.<attributeName>, a.<attributeName>, a.<attributeName> 
insert into fooBar;
```
The above syntax includes the following:
Item|Description
---------|---------
`within "<START_TIME>", "<END_TIME>"`|This allows you to specify the time interval for which the aggregate values need to be retrieved by specifying the timestamps for the start time and the end time.
`per "<TIME_PERIOD>"`|This specifies the time period by which the aggregate values must be grouped. e.g., If you specify `days`, the retrieved aggregate vlaues are displayed for each day within the selected time interval.

**Example**
```sql
define stream barStream (symbol string, value int);

from barStream as b join testAggregator as a
on a.symbol == b.symbol 
within "2014-02-15 00:00:00 +05:30", "2014-03-16 00:00:00 +05:30" 
per "days" 
select a.symbol, a.total, a.avgPrice 
insert into fooBar;
```
This query performs a join to match events arriving at the `barStream` stream with the events calculated and persisted by the `testAggregator` aggregator. If the value for the `symbol` attribute of an event that arrives in an input stream is the same as that of an event persisted by the aggregator, the aggregated values already calculated for it for the time period between `2014-02-15 00:00:00 +05:30` and `2014-03-16 00:00:00 +05:30` are retrieved. The aggregate values (i.e., average and the total in this scenario) for the last day is retrieved. The output events are inserted into the `FooBar` output stream.

## Joins
Join allows two event streams to be merged based on a condition. In order to carry out a join, each stream should be connected to a window. If no window is specified, a window of zero length (`#window.length(0)`) is assigned to the input event stream by default. During the joining process each incoming event on each stream is matched against all the events in the other input event stream window based on the given condition. An output event is generated for all the matching event pairs.

**Syntax**
The syntax for a join is as follows:
```sql
from <input stream name>[<filter condition>]#window.<window name>(<parameter>, ... ) {unidirectional} {as <reference>}
         join <input stream name>#window.<window name>(<parameter>,  ... ) {unidirectional} {as <reference>}
    on <join condition>
    within <time gap>
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```
**Example**
```sql
define stream TempStream(deviceID long, roomNo int, temp double);
define stream RegulatorStream(deviceID long, roomNo int, isOn bool);
  
from TempStream[temp > 30.0]#window.time(1 min) as T
  join RegulatorStream[isOn == false]#window.length(1) as R
  on T.roomNo == R.roomNo
select T.roomNo, R.deviceID, 'start' as action
insert into RegulatorActionStream;
```
WSO2 Siddhi currently supports the following types of joins.

### Left Outer Join
Outer join allows two event streams to be merged based on a condition. However, it returns all the events of left stream even if there are no matching events in the right stream. Here each stream should be associated with a window. During the joining process, each incoming event of each stream is matched against all the events in the other input event stream window based on the given condition. Incoming events of the right stream are matched against all events in the left event stream window based on the given condition. An output event is generated for all the matching event pairs. An output event is generated for incoming events of the left stream even if there are no matching events in right stream.

**Example**
The following query generates output events for all the events in the `stockStream` stream whether there is a match for the symbol in the `twitterStream` stream or not.
```sql
from stockStream#window.length(2) 
left outer join twitterStream#window.length(1)
on stockStream.symbol== twitterStream.symbol
select stockStream.symbol as symbol, twitterStream.tweet, stockStream.price
insert all events into outputStream ;
```

### Right Outer Join
This is similar to left outer join. It returns all the events of the right stream even if there are no matching events in the left stream. Incoming events of the left stream are matched against all events in the right event stream window based on the given condition. An output event is generated for all the matching event pairs. An output event is generated for incoming events of the right stream even if there are no matching events in left stream.

e.g., The following generates output events for all the incoming events of each stream whether there is a match for the symbol in the other stream or not.
### Full Outer Join

The full outer join combines the results of left outer join and right outer join. An output event is generated for each incoming event even if there are no matching events in the other stream.<br>
e.g., The following generates output events for all the incoming events of each stream whether there is a match for the symbol in the other stream or not.
```sql
from stockStream#window.length(2)
full outer join twitterStream#window.length(1)
on stockStream.symbol== twitterStream.symbol
select stockStream.symbol as symbol, twitterStream.tweet, stockStream.price
insert all events into outputStream ;
```

### Full Outer Join

The full outer join combines the results of left outer join and right outer join. An output event is generated for each incoming event even if there are no matching events in the other stream.

e.g., The following generates output events for all the incoming events of each stream whether there is a match for the symbol in the other stream or not.

<pre>from stockStream#window.length(2)<br>full outer join twitterStream#window.length(1)<br>on stockStream.symbol== twitterStream.symbol<br>select stockStream.symbol as symbol, twitterStream.tweet, stockStream.price<br>insert all events into outputStream ;</pre>

## Patterns and Sequences

Patterns and sequences allow event streams to be correlated over time and detect event patterns based on the order of event arrival.

### Patterns

Pattern allows event streams to be correlated over time and detect event patterns based on the order of event arrival. With pattern there can be other events in between the events that match the pattern condition. It creates state machines to track the states of the matching process internally. Pattern can correlate events over multiple input streams or over the same input stream. Therefore, each matched input event need to be referenced so that that it can be accessed for future processing and output generation.

**Syntax**
The following is the syntax for a pattern configuration:
```sql
from {every} <input event reference>=<input stream name>[<filter condition>] -> {every} <input event reference>=<input stream name>[<filter condition>] -> ...        within <time gap>
select <input event reference>.<attribute name>, <input event reference>.<attribute name>, ...
insert into <output stream name>
```

**Example**

The following query sends an alert if the temperature of a room increases by 5 degrees within 10 min.

```sql
from every( e1=TempStream ) -> e2=TempStream[e1.roomNo==roomNo and (e1.temp + 5) <= temp ]
    within 10 min
select e1.roomNo, e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
```

WSO2 Siddhi supports the following types of patterns:

+ Counting patterns
+ Logical patterns

#### Counting Patterns

Counting patterns allow multiple events that may or may not have been received in a sequential order based on the same matching condition.

**Syntax**

The number of events matched can be limited via postfixes as explained below.

Postfix|Description|Example
---------|---------|---------|
`n1:n2`|This matches `n1` to `n2` events.|`1:4` matches 1 to 4 events.
`<n:>`|This matches `n` or more events.|`<2:>` matches 2 or more events.
`<:n>`|This matches up to `n` events.|`<:5>` matches up to 5 events.
`<n>`|This matches exactly `n` events.|`<5>` matches exactly 5 events.

Specific occurrences of the events that should be matched based on count limits are specified via key words and numeric values within square brackets as explained with the examples given below.

+ `e1[3]` refers to the 3rd event.
+ `e1[last]` refers to the last event.
+ `e1[last - 1]` refers to the event before the last event.

**Example**
The following query calculates the temperature difference between two regulator events.

```sql
define stream TempStream(deviceID long, roomNo int, temp double);
define stream RegulatorStream(deviceID long, roomNo int, tempSet double, isOn bool);
  
from every( e1=RegulatorStream) -> e2=TempStream[e1.roomNo==roomNo]<1:> -> e3=RegulatorStream[e1.roomNo==roomNo]
select e1.roomNo, e2[0].temp - e2[last].temp as tempDiff
insert into TempDiffStream;
```
#### Logical Patterns

Logical pattern matches events that arrive in temporal order and correlates events with logical relationships.

**Syntax**

Keywords such as and and or can be used instead of -> to illustrate the logical relationship.

Key Word|Description
---------|---------
`and`|This allows two events received in any order to be matched.
`or`|One event from either event stream can be matched regardless of the order in which the events were received.





**Example: Identifying the occurence of an expected event**
```sql
define stream TempStream(deviceID long, roomNo int, temp double);
define stream RegulatorStream(deviceID long, roomNo int, tempSet double);
  
from every( e1=RegulatorStream ) -> e2=TempStream[e1.roomNo==roomNo and e1.tempSet <= temp ] or e3=RegulatorStream[e1.roomNo==roomNo]
select e1.roomNo, e2.temp as roomTemp
having e3 is null
insert into AlertStream;
```
This query sends an alert when the room temperature reaches the temperature set on the regulator. The pattern matching is reset every time the temperature set on the regulator changes.


### Sequences

Sequence allows event streams to be correlated over time and detect event sequences based on the order of event arrival. With sequence there cannot be other events in between the events that match the sequence condition. It creates state machines to track the states of the matching process internally. Sequence can correlate events over multiple input streams or over the same input stream. Therefore, each matched input event needs to be referenced so that it can be accessed for future processing and output generation.
**Syntax**

The following is the syntax for a sequence configuration.

```sql
from {every} <input event reference>=<input stream name>[<filter condition>], <input event reference>=<input stream name>[<filter condition>]{+|*|?}, ...       within <time gap>
select <input event reference>.<attribute name>, <input event reference>.<attribute name>, ...
insert into <output stream name>
```
**Example**

The following query sends an alert if there is more than 1 degree increase in the temperature between two consecutive temperature events.

```sql
from every e1=TempStream, e2=TempStream[e1.temp + 1 < temp ]
select e1.temp as initialTemp, e2.temp as finalTemp
insert into AlertStream;
```

WSO2 Siddhi supports the following types of sequences:

+ Counting sequences
+ Logical sequences

#### Counting sequences

Counting sequence allows us to match multiple consecutive events based on the same matching condition.

**Syntax**

The number of events matched can be limited via postfixes as explained below.

Postfix|Description
---------|---------
*|This matches zero or more events.
+|This matches 1 or more events.
?|This matches zero events or one event.

**Example**

The following query identifies peak temperatures.

```sql
define stream TempStream(deviceID long, roomNo int, temp double);
define stream RegulatorStream(deviceID long, roomNo int, tempSet double, isOn bool);
  
from every e1=TempStream, e2=TempStream[e1.temp <= temp]+, e3=TempStream[e2[last].temp > temp]
select e1.temp as initialTemp, e2[last].temp as peakTemp
insert into TempDiffStream;
```

#### Logical Sequences

Logical sequence matches events that arrive in temporal order and correlates events with logical relationships.

**Syntax**

Keywords such as and and or can be used instead of -> to illustrate the logical relationship.

Keyword|Description
---------|---------
`and`|This allows two events received in any order to be matched.
`or`|One event from either event stream can be matched regardless of the order in which the events were received.
`not`|When this precedes a condition in a Siddhi query, it indicates that the condition is not met.
`for`| This is used to define a time period within which an event should arrive. e.g., `from not TemperatureStream[temp > 60] for 5 sec -> e1=FireAlarmStream` defines a condition for an event to arrive at the `FireAlarmStream` stream within 5 seconds after an event with a value greater than 60 for temperature arrives in the `TemperatureStream` stream. 

**Example 1: Identifying the occurence of an event**


```sql
define stream TempStream(deviceID long, temp double);define stream HumidStream(deviceID long, humid double);
define stream RegulatorStream(deviceID long, isOn bool);
  
from every e1=RegulatorStream, e2=TempStream and e3=HumidStream
select e2.temp, e3.humid
insert into StateNotificationStream;
```
This query creates a notification when a regulator event is followed by both temperature and humidity events.

**Example 2: Identifying the non-occurence of an expected event**
 ```sql
 define stream CustomerStream (customerId string, timestamp long);
 
 from every not CustomerStream for 7 days
 select *
 insert into OutputStream;
 ```
This query receives information about existing customers of the store from the `CustomerStream` stream. It identifies customers that have not visited the store for the last seven days, and outputs that information to the `OutputStream` stream. A message is generated from the `OutputStream` stream with information for those customers about the discounts that are currently offered at the store.

**Example 3: Detecting the non-occurence of an expected event following another event**
```sql
define stream LocationStream (username string, latitude double, longitude double);
define stream SpeedStream (username string, speed double);
from not LocationStream[latitude == 43.0096 and longitude == 81.2737] for 15 minutes and e1=SpeedStream[speed >= 60.0]
select e1.username as username
insert into AlertStream;
```
This query receives information about the location of taxis from the `LocationStream` stream, and information about the average speed of taxis from the `SpeedStream` stream. If a taxi (i.e., a username) with an average speed greater than 60 that has not reached location at `latitude == 43.0096 and longitude == 81.2737` in 15 minutes is identified, an event is output to the `AlertStream` in order send an alert that indicates that the taxi has taken the wrong route.

**Example 4: Detecting the non-occurence of multiple events**
```sql
define stream LocationStream (username string, latitude double, longitude double);
define stream StateStream (username string, state string);
from not LocationStream[latitude == 43.0096 and longitude == 81.2737] for 30 minutes and not StateStream[state == ‘finished’] for 30 minutes
select ‘Danger’ as message
insert into AlertStream;
```
This query receives information about the location of taxis from the `LocationStream` stream, and information about the status of the passenger from the `StateStream` stream. If the passenger (i.e., username) does not arrive at the location at `latitude == 43.0096 and longitude == 81.2737` in 30 minutes, and at the same time, if he/she has not marked the journey as `finished`, an event is output to the `AlertStream` stream to generate an alert with `Danger` as the message.

**Example 5: Detecting the non-occurence of either of two mutually exclusive events**
```sql
define stream LocationStream (username string, latitude double, longitude double);

from not LocationStream[latitude == 43.0096 and longitude == 81.2737] for 15 minutes or not LocationStream[latitude == 44.0096 and longitude == 81.2735] for 15 minutes
select ‘Unexpected Delay’ as message
insert into AlertStream;
```
This query receives information about the location of taxis from the `LocationStream` stream. If a taxi has not reached either the location at `latitude == 43.0096 and longitude == 81.2737`, or the one at `latitude == 44.0096 and longitude == 81.2735` in 15 minutes, an event is output to the `AlertStream` stream to generate an alert with `Unexpected Delay` as the message.

**Example 6: Detecting the non-occurence of one event or the occurence of another**
```sql
define stream LocationStream (username string, latitude double, longitude double);
define stream DangerStream (username string);
from not LocationStream[latitude == 43.0096 and longitude == 81.2737] for 30 minutes or e1=DangerStream
select e1.username as username
insert into AlertStream;
```
This query receives information about the location of taxis from the `LocationStream` stream, and information about whether the passenger is in danger from the `DangerStream` stream. After 30 minutes, it checkes whether the passenger has reached the location at `latitude == 43.0096 and longitude == 81.2737`, or marked to indicate that he/she is in danger. If the passenger has not reached the location, or if he/she has indicated that he/she is in danger, an event is output to the `AlertStream` stream in order to generate an alert to indicate that the passenger is in danger.

**Example 7: Identifying the occurence of an unexpected event within a specified time interval**
```sql
define stream TemperatureStream (temp float, timestamp long);
define stream FireAlarmStream (active boolean);
from not TemperatureStream[temp > 60] for 5 sec -> e1=FireAlarmStream
select e1.id as alarmId
insert into AlertStream;
```
This query receives information about the temperature from the `TemperatureStream` stream, and information about the state of the fire alarm from the `FireAlarmStream` stream. If the state of the fire alarm is `active` within a period of 5 seconds during which the temperature is less than 60 degrees, an event is output to the `AlertStream` stream in order to indicate that the fire alarm generates false alerts.

**Example 8: Identifying the non-occurence of an expected event within a specified time period**
```sql
define stream TemperatureStream (temp float, timestamp long);
define stream FireAlarmStream (active boolean);
from TemperatureStream[temp > 60] -> not FireAlarmStream[active == true] for 5 sec
select 'Fire alarm not working' as message
insert into AlertStream;
```
This query receives information about the temperature from the `TemperatureStream` stream, and information about the state of the fire alarm from the `FireAlarmStream` stream. If an event where the state of the fire alarm is `active` does not arrive within five seconds after an event that indicates that the temperature has risen above 60 degrees, an event is output to the `AlertStream` stream with `Fire alarm not working` as the message.

**Example 9: Identifying the occurence of an even that is not preceded by another expected event**
```sql
define stream LocationStream (locationId string, customerId string);

from not LocationStream[locationId == 'zoneA'] and e1=LocationStream[locationId ==  'billingCounter']
select e1.customerId as customerId, 'Great deals are waiting for you at zone A' as message
insert into NotificationStream;
```
This query receives information about the location of customers from the `LocationStream` stream. If an event indicates that a customer has reached the `bilingCounter` location, and it is not preceded by an event that indicates that the same customer has been to the `zoneA` location, an event is output to the `NotificationStream` stream in order to generate a notification with `Great deals are waiting for you at zone A` as the message.

## Partitions

Partitions allow events and queries to be divided in order to process them in parallel and in isolation. Each partition is tagged with a partition key. Only events corresponding to this key are processed for each partition. A partition can contain one or more Siddhi queries.
Siddhi supports both variable partitions and well as range partitions.

### Variable Partitions

A variable partition is created by defining the partition key using the categorical (string) attribute of the input event stream.

**Syntax**

```sql
partition with ( <attribute name> of <stream name>, <attribute name> of <stream name>, ... )begin
    <query>
    <query>
    ...
end;
```
**Example**

The following query calculates the maximum temperature recorded for the last 10 events emitted per sensor.

```sql
partition with ( deviceID of TempStream )begin
    from TempStream#window.length(10)
    select roomNo, deviceID, max(temp) as maxTemp
    insert into DeviceTempStream
end;
```

### Range Partitions

A range partition is created by defining the partition key using the numerical attribute of the input event stream.

**Syntax**

```sql
partition with ( <condition> as <partition key> or <condition> as <partition key> or ... of <stream name>, ... )begin
    <query>
    <query>
    ...
end;
```

**Example**

The following query calculates the average temperature for the last 10 minutes per office area.

```sql
partition with ( roomNo>=1030 as 'serverRoom' or roomNo<1030 and roomNo>=330 as 'officeRoom' or roomNo<330 as 'lobby' of TempStream) )begin
    from TempStream#window.time(10 min)
    select roomNo, deviceID, avg(temp) as avgTemp
    insert into AreaTempStream
end;
```
###Inner Streams 

Inner streams can be used for query instances of a partition to communicate between other query instances of the same partition. Inner Streams are denoted by a "#" in front of them, and these streams cannot be accessed outside of the partition block. 

**Example**

Per sensor, calculate the maximum temperature over last 10 temperature events when the sensor is having an average temperature greater than 20 over the last minute.

<pre>
partition with ( deviceID of TempStream )
begin
    from TempStream#window.time(1 min)
    select roomNo, deviceID, temp, avg(temp) as avgTemp
    insert into #AvgTempStream
  
    from #AvgTempStream[avgTemp > 20]#window.length(10)
    select roomNo, deviceID, max(temp) as maxTemp
    insert into deviceTempStream
end;
</pre>

## Inner streams 
Inner streams can be used for query instances of a partition to communicate between other query instances of the same partition. Inner Streams are denoted by a "#" in front of them, and these streams cannot be accessed outside of the partition block. 
Inner streams can be used for query instances of a partition to communicate between other query instances of the same partition. Inner Streams are denoted by a "#" in front of them, and these streams cannot be accessed outside of the partition block. <br>
E.g. Per sensor, calculate the maximum temperature over last 10 temperature events when the sensor is having an average temperature greater than 20 over the last minute.<br>

```sql
define table RoomTypeTable (roomNo int, type string);
```

## (Defined) Windows 

An event window is a window that can be shared across multiple queries. Events are inserted from one or more streams. The event window publishes current and/or expired events as the output. The time at which these events are published depends on the window type.
 
**Syntax**

The following is the syntax for an event window.
```sql
define window <event window name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... ) <window type>(<parameter>, <parameter>, …) <output event type>;
```
**Examples**
 
+ Returning all output categories

In the following query, the window type is not specified in the window definition. Therefore, it emits both current and expired events as the output.
```sql
define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second);
```

+ Returning a specified output category

In the following query, the window type is `output all events`. Therefore, it emits both current and expired events as the output.

```sql
define window SensorWindow (name string, value float, roomNo int, deviceID string) timeBatch(1 second) output all events;
```
 
### Supported Event Window Operators
The following operators are supported for event windows.

#### Insert Into

**Syntax**

```sql
from <input stream name> 
select <attribute name>, <attribute name>, ...
insert into <window name>
```

To insert only the specified output event category, use the `current events`, `expired events` or the `all events` keyword between `insert` and `into` keywords. For more information, see Output Event Categories.
 
**Purpose**

To insert events from an event stream to a window.
 
**Parameters**

+ `input stream name`:  The event stream from which events are inserted into the event window.

+ `attribute name`: The name of the attributes with which the events are inserted from the event stream to the event window. Multiple attributes can be specified as a comma separated list.

+ `window name`: The event window to which events are inserted from the event stream.
 
**Example**

The following query inserts both current and expired events from an event stream named `sensorStream` to an event window named `sensorWindow`.

```sql
from SensorStream
insert into SensorWindow;
```
 
#### Output
An event window can be used as a stream in any query. However, an ordinary window cannot be applied to the output of an event window.

**Syntax**

```sql
from <window name> 
select <attribute name>, <attribute name>, ...
insert into <event stream name>
```
**Purpose**

To inject the output of an event window into an event stream.

**Parameters**

+ `window name`: The event window of which the output is injected into the specified stream.

+ `attribute name`: The name of the attributes with which the events are inserted from the event stream to the event window. Multiple attributes can be specified as a comma separated list.

+ `event stream name`: The event stream to which the output of the specified event window is injected.
 
**Example**
The following query selects the name and the maximum values for the `value` and `roomNo` attributes from an event window named `SensorWindow`, and inserts them into an event stream named `MaxSensorReadingStream`.

```sql
from SensorWindow
select name, max(value) as maxValue, roomNo
insert into MaxSensorReadingStream;
```

#### Join

**Example**

```sql
 define stream TempStream(deviceID long, roomNo int, temp double);
 define stream RegulatorStream(deviceID long, roomNo int, isOn bool);
 define window TempWindow(deviceID long, roomNo int, temp double) time(1 min);
   
 from TempStream[temp > 30.0]
 insert into TempWindow;
   
 from TempWindow
 join RegulatorStream[isOn == false]#window.length(1) as R
 on TempWindow.roomNo == R.roomNo
 select TempWindow.roomNo, R.deviceID, 'start' as action
 insert into RegulatorActionStream;
```
## Triggers
Event triggers allow events to be created periodically based on a specified time interval.

**Syntax**

The following is the syntax for an event trigger definition.
```sql
define trigger <trigger name> at {'start'| every <time interval>| '<cron expression>'};
```

**Examples**

+ Triggering events regularly at specific time intervals

The following query triggers events every 5 minutes.
```sql
 define trigger FiveMinTriggerStream at every 5 min;
```

+ Triggering events at a specific time on specified days
The following query triggers an event at 10.15 AM every Monday, Tuesday, Wednesday, Thursday and Friday.
```sql
 define trigger FiveMinTriggerStream at '0 15 10 ? * MON-FRI';
 
```
## Siddhi Logger

The Siddhi Logger logs events that arrive in different logger priorities such as `INFO`, `DEBUG`, `WARN`, `FATAL`, `ERROR`, `OFF`, and `TRACE`.

**Syntax**

The following is the syntax for a query with a Siddhi logger.

```sql
<void> log(<string> priority, <string> logMessage, <bool> isEventLogged)
```

The parameters configured are as follows.

+ `prioroty`: The logging priority. Possible values are `INFO`, `DEBUG`, `WARN`, `FATAL`, `ERROR`, `OFF`, and `TRACE`. If no value is specified for this parameter, `INFO` is printed as the priority by default.

+ `logMessage`: This parameter allows you to specify a message to be printed in the log.

+ `isEventLogged`: This parameter specifies whether the event body should be included in the log. Possible values are `true` and `false`. If no value is specified, the event body is not printed in the log by default.

**Examples**

+ The following query logs the event with the `INFO` logging priority. This is because the priority is not specified.
```sql
from StockStream#log()
select *
insert into OutStream;
```

+ The following query logs the event with the `INFO` logging priority (because the priority is not specified) and the `test message` text.
```sql
from StockStream#log('test message')
select *
insert into OutStream;
```

+ The following query logs the event with the `INFO` logging priority because a priority is not specified. The event itself is printed in the log.
```sql
from StockStream#log(true)
select *
insert into OutStream;
```

+ The following query logs the event with the `INFO` logging priority (because the priority is not specified) and the `test message` text. The event itself is printed in the log.
```sql
from StockStream#log('test message', true)
select *
insert into OutStream;
```

+ The following query logs the event with the `WARN` logging priority and the `test message` text.
```sql
from StockStream#log('warn','test message')
select *
insert into OutStream;
```

+ The following query logs the event with the `WARN` logging priority and the `test message` text.  The event itself is printed in the log.
```sql
from StockStream#log('warn','test message',true)
select *
insert into OutStream;
```

## Scripts

Eval script allows Siddhi to process events using other programming languages by including their functions in the Siddhi queries. Eval script functions can be defined like event tables or streams and referred in the queries as Inbuilt Functions of Siddhi.

**Syntax**

The following is the syntax for a Siddhi query with an Eval Script definition.

```sql
define function <function name>[<language name>] return <return type> {
    <operation of the function>
};
```

The following parameters are configured when defining an eval script.

+ `function name`: 	The name of the function from another programming language that should be included in the Siddhi query.

+ `language name`: The name of the other programming language from which the function included in the Siddhi query is taken. The languages supported are JavaScript, R and Scala.

+ `return type`: The return type of the function defined. The return type can be `int`, `long`, `float`, `double`, `string`, `bool` or `object`. Here the function implementer should be responsible for returning the output on the defined return type for proper functionality. 

+ `operation of the function`: Here, the execution logic of the defined logos should be added. This logic should be written in the language specified in the `language name` parameter, and the return should be of the type specified in the `return type` parameter.

**Examples**

+ Concatenating a JavaScript function

The following query performs the concatenating function of the JavaScript language and returns the output as a string.

```sql
define function concatFn[JavaScript] return string {
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

+ Concatenating an R function

The following query performs the concatenating function of the R language and returns the output as a string.

```sql
define function concatFn[R] return string {
    return(paste(data, collapse=""));
};
  
define stream TempStream(deviceID long, roomNo int, temp double);
  
from TempStream
select concatFn(roomNo,'-',deviceID) as id, temp
insert into DeviceTempStream;
```

+ Concatenating a Scala function

The following query performs the concatenating function of the Scala language and returns the output as a string.

```sql
define function concatFn[Scala] return string {
    var concatenatedString =
     for(i <- 0 until data.length){
         concatenatedString += data(i).toString
     }
     concatenatedString
};
  
define stream TempStream(deviceID long, roomNo int, temp double);
  
from TempStream
select concatFn(roomNo,'-',deviceID) as id, temp
insert into DeviceTempStream;
```
##Siddhi extensions

Siddhi supports an extension architecture to support custom code and functions to be incorporated with Siddhi in a seamless manner. Extension will follow the following syntax;

```sql
<namespace>:<function name>(<parameter1>, <parameter2>, ... )
```

Here the namespace will allow Siddhi to identify the function as an extension and its extension group, the function name will denote the extension function within the given group, and the parameters will be the inputs that can be passed to the extension for evaluation and/or configuration.  

E.g. A window extension created with namespace foo and function name unique can be referred as follows:

```sql
from StockExchangeStream[price >= 20]#window.foo:unique(symbol)
select symbol, price
insert into StockQuote
```
**Extension types**

Siddhi supports following five type of extensions:

**1.Function Extension**

For each event it consumes zero or more parameters and output a single attribute as an output. This could be used to manipulate event attributes to generate new attribute like Function operator. Implemented by extending "org.wso2.siddhi.core.executor.function.FunctionExecutor".

E.g. "math:sin(x)" here the sin function of math extension will return the sin value its parameter x.
  
**2.Aggregate Function Extension**

For each event it consumes zero or more parameters and output a single attribute having an aggregated results based in the input parameters as an output. This could be used with conjunction with a window in order to find the aggregated results based on the given window like Aggregate Function operator. Implemented by extending "org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator".

E.g. "custom:std(x)" here the std aggregate function of custom extension will return the standard deviation of value x based on the assigned window to its query. 

**3.Window Extension**

Allows events to be collected and expired without altering the event format based on the given input parameters like the Window operator. Implemented by extending "org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor".

E.g. "custom:unique(key)" here the unique window of custom extension will return all events as current events upon arrival as current events and when events arrive with the same value based on the "key" parameter the corresponding to a previous event arrived the previously arrived event will be emitted as expired event.

**4.Stream Function Extension**

Allows events to be altered by adding one or more attributes to it. Here events could be outputted upon each event arrival. Implemented by extending "org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor".
    
E.g. "custom:pol2cart(theta,rho)" here the pol2cart function of custom extension will return all events by calculating the cartesian coordinates x & y and adding them as new attributes to the existing events.

**5.Stream Processor Extension**
    
Allows events to be collected and expired with altering the event format based on the given input parameters. Implemented by extending "oorg.wso2.siddhi.core.query.processor.stream.StreamProcessor".
    
E.g. "custom:perMinResults(arg1, arg2, ...)" here the perMinResults function of custom extension will return all events by adding one or more attributes the events based on the conversion logic and emitted as current events upon arrival as current events and when at expiration expired events could be emitted appropriate expiring events attribute values for matching the current events attributes counts and types.

**Available Extentions**

Siddhi currently have several prewritten extensions as follows; 

Extensions released under Apache License v2 : 

+ **math**:   Supporting mathematical operations 
+  **str**:Supporting String operations 
+ **geo**: Supporting geocode operations
+ **regex**: Supporting regular expression operations
+ **time**: Supporting time expression operations
+ **ml**: Supporting Machine Learning expression operations
+ **timeseries**: Supporting Time Series operations
+  **kf** (Kalman Filter): Supporting filtering capabilities by detecting outliers of the data.
+ **map**: Supporting to send a map object inside Siddhi stream definitions and use it inside queries.
+ **reorder**: Supporting for reordering events from an unordered event stream using Kslack algorithm. 

Extensions released under GNU/GPL License v3 : 

  + **geo**: Supporting geographical processing operations   
  + **r**: Supporting R executions
  + **nlp**: Supporting Natural Language Processing expression operations
  + **pmml**: Supporting Predictive Model Markup Language expression operations
  
  
**Writing Custom Extensions**

Custom extensions can be written in order to cater usecase specific logics that are not out of the box available in Siddhi or as an extension. 

To create custom extensions two things need to be done.

1.Implementing the extension logic by extending well defined Siddhi interfaces. E.g implementing a UniqueWindowProcessor by extending org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor.

```sql
package org.wso2.test;
  
public class UniqueWindowProcessor extends WindowProcessor {
   ...
}
```

2.Add an extension mapping file to map the written extension class with the extension function name and namespace. Here extension mapping file should be named as "<namespace>.siddhiext". E.g Mapping the written UniqueWindowProcessor extension with function name "unique" and namespace "foo", to do so the mapping file should be named as foo.siddhiext and the context of the file should as below; 

```sql
# function name to class mapping of 'foo' extension
unique=org.wso2.test.UniqueWindowProcessor
```

## Siddhi extensions

Siddhi supports an extension architecture to support custom code and functions to be incorporated with Siddhi in a seamless manner. Extension will follow the following syntax;
```sql
<namespace>:<function name>(<parameter1>, <parameter2>, ... )
```
Here the namespace will allow Siddhi to identify the function as an extension and its extension group, the function name will denote the extension function within the given group, and the parameters will be the inputs that can be passed to the extension for evaluation and/or configuration. <br>
E.g. A window extension created with namespace foo and function name unique can be referred as follows:


```sql
from StockExchangeStream[price >= 20]#window.foo:unique(symbol)
select symbol, price
insert into StockQuote
```

**Extension types** 

Siddhi supports following five type of extensions: 

**Function Extension**

For each event it consumes zero or more parameters and output a single attribute as an output. This could be used to manipulate event attributes to generate new attribute like Function operator. Implemented by extending "org.wso2.siddhi.core.executor.function.FunctionExecutor".<br>
E.g. "math:sin(x)" here the sin function of math extension will return the sin value its parameter x.

**Aggregate Function Extension**

For each event it consumes zero or more parameters and output a single attribute having an aggregated results based in the input parameters as an output. This could be used with conjunction with a window in order to find the aggregated results based on the given window like Aggregate Function operator. Implemented by extending "org.wso2.siddhi.core.query.selector.attribute.aggregator.AttributeAggregator".<br>
E.g. "custom:std(x)" here the std aggregate function of custom extension will return the standard deviation of value x based on the assigned window to its query. 

**Window Extension**

Allows events to be collected and expired without altering the event format based on the given input parameters like the Window operator. Implemented by extending "org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor".<br>
E.g. "custom:unique(key)" here the unique window of custom extension will return all events as current events upon arrival as current events and when events arrive with the same value based on the "key" parameter the corresponding to a previous event arrived the previously arrived event will be emitted as expired event.

**Stream Function Extension**

Allows events to be altered by adding one or more attributes to it. Here events could be outputted upon each event arrival. Implemented by extending "org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor".<br>
E.g. "custom:pol2cart(theta,rho)" here the pol2cart function of custom extension will return all events by calculating the cartesian coordinates x & y and adding them as new attributes to the existing events.

**Stream Processor Extension**
Allows events to be collected and expired with altering the event format based on the given input parameters. Implemented by extending "oorg.wso2.siddhi.core.query.processor.stream.StreamProcessor".<br>
E.g. "custom:perMinResults(arg1, arg2, ...)" here the perMinResults function of custom extension will return all events by adding one or more attributes the events based on the conversion logic and emitted as current events upon arrival as current events and when at expiration expired events could be emitted appropriate expiring events attribute values for matching the current events attributes counts and types.

**Available Extensions**

Siddhi currently have several prewritten extensions as follows; <br>
Extensions released under Apache License v2 : 
* `math`: Supporting mathematical operations 
* `str`: Supporting String operations 
* `geo`: Supporting geocode operations
* `regex`: Supporting regular expression operations
* `time`: Supporting time expression operations
* `ml`: Supporting Machine Learning expression operations
* `timeseries`: Supporting Time Series operations
* `kf` (Kalman Filter): Supporting filtering capabilities by detecting outliers of the data.
* `map`: Supporting to send a map object inside Siddhi stream definitions and use it inside queries.
* `reorder`: Supporting for reordering events from an unordered event stream using Kslack algorithm.
* `Extensions` released under GNU/GPL License v3 : 
* `geo`: Supporting geographical processing operations   
* `r`:  Supporting R executions
* `nlp`: Supporting Natural Language Processing expression operations
* `pmml`: Supporting Predictive Model Markup Language expression operations

You can get them from <a href="https://github.com/wso2-gpl/siddhi">https://github.com/wso2-gpl/siddhi</a>

**Writing Custom Extensions**
Custom extensions can be written in order to cater usecase specific logics that are not out of the box available in Siddhi or as an extension. 
To create custom extensions two things need to be done. 
1. Implementing the extension logic by extending well defined Siddhi interfaces. E.g implementing a UniqueWindowProcessor by extending org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor.

```sql
package org.wso2.test;
  
public class UniqueWindowProcessor extends WindowProcessor {
   ...
}
```

2. Add an extension mapping file to map the written extension class with the extension function name and namespace. Here extension mapping file should be named as "<namespace>.siddhiext". E.g Mapping the written UniqueWindowProcessor extension with function name "unique" and namespace "foo", to do so the mapping file should be named as foo.siddhiext and the context of the file should as below; 

```sql
# function name to class mapping of 'foo' extension
unique=org.wso2.test.UniqueWindowProcessor
```
