# Siddhi Query Language Guide 4.0


## Event streams
The event stream definition defines the event stream schema. An event stream definition contains a unique name and a 
set of attributes assigned specific types, with uniquely identifiable names within the stream.

**Purpose**
To define the schema based on which events are selected to be processed in the execution plan to which the event stream
 is attached.

**Syntax**

The following is the syntax of a query that defines a new event stream.
```sql
`define stream <stream name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );`
```
The following parameters are configured in an event stream definition.

| Parameter     | Description |
| ------------- |-------------|
| `stream name`      | The name of the event stream to be created. |
| ```<attribute name> <attribute type>```     | The schema of an event stream is defined by adding the name and the type of each attribute as a value pair. <br> *`attribute name`: A unique name for the attribute. </br> <br> *`attribute type`: The data type of the attribute value. This can be `STRING`, `INT`, `LONG`, `DOUBLE`, `FLOAT` or `BOOLEAN`.</br>     |

**Example**
```sql
`define stream TempStream (deviceID long, roomNo int, temp double);`
The above creates an event stream named TempStream with the following attributes.
```
+ `deviceID` of the `long` attribute type
+ `roomNo` of the `int` attribute type
+ `temp` of the `double` attribute type

## Event sources

## Event sinks

## Filters
Filters are included in queries to filter information from input streams based on a specified condition.

**Purpose**
A filter allows you to separate events that match a specific condition as the output, or for further processing.

**Syntax**

Filter conditions should be defined in square brackets next to the input stream name as shown below.

```sql
from <input stream name>[<filter condition>]select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

The following parameters are configured in a filter definition.

| Parameter                   | Description                                                               |
|-----------------------------|---------------------------------------------------------------------------|
|`input stream name`|The event stream from which events should be selected.|
|`filter condition`| The condition based on which the events should be filtered to be inserted into the output stream.|
|`attribute name`| The specific attributes of which the values should be inserted into the output stream. Multiple
||attributes can be added as a list of comma-separated values.|
|`output stream name`|The event stream to which the filtered events should be inserted.|

**Example**

The following query filters all server rooms of which the temperature is greater than 40 degrees. The filtered events
are taken from the `TempStream` input stream and inserted into the `HighTempStream` output stream.

```sql
from TempStream [(roomNo >= 100 and roomNo < 110) and temp > 40 ]select roomNo, temp
insert into HighTempStream;
```

## Windows
Windows allow you to capture a subset of events based on a specific criterion from an input event stream for calculation. Each input stream can only have maximum of one window.

**Purpose**

To create subsets of events within an event stream based on time or length. Event windows always operate as sliding windows.

e.g., If you define a filter condition to find the maximum value out of every 10 events, you need to define a length window of 10 events. This window operates as a sliding window where the following 3 subsets can be identified in a list of 12 events received in a sequential order.

|Subset|Event Range|
|------|-----------|
| 1 | 1-10 |
| 2 | 2-11 |
|3| 3-12 |

**Syntax**

The `#window` prefix should be inserted next to the relevant event stream in order to add a window.

```sql
from <input stream name>[<filter condition>]#window.<window name>(<parameter>, <parameter>, ... )select <attribute name>, <attribute name>, ...
insert into <output stream name>
```
The following parameters are configured in a window definition.

| Parameter | Description |
|-----------|-------------|
| `input stream name` | The stream to which the window is applied. |
| `filter condition` | The filter condition based on which events are filtered from the defined window. |
| `window name` | <br>The name of the window. The name should include the following information:  <br>The type of the window. The supported inbuilt windows are listed in Inbuilt Windows.</br> <br>The range of the window in terms of the time duration or the number of events.</br> <br>e.g, time(10 min) defines a time window of 10 minutes.</br>|
|`parameter`    | |
| `attribute name` | The attributes of the events selected from the window that should be inserted into the output stream. Multiple attributes can be specified as a comma-separated list.|
| `output stream name` | |


   `For details on inbuilt windows supported for Siddhi, see Inbuilt Windows.
   For details on how to manipulate window output based on event categories, see Output Event Categories.
   For information on how to perform aggregate calculations within windows, see Aggregate Functions.`


### Aggregate functions
Aggregate functions perform aggregate calculations within defined windows.For the complete list of inbuilt aggregate
functions supported see Inbuilt Aggregate Functions.

**Syntax**
```sql
from <input_stream>#window.<window_name>select <aggregate_function>(<attribute_name>) as <attribute_name>, <attribute1_name>, <attribute2_name>, ...
insert <output_event_category> into <output_stream>;
```

The parameters configured are as follows:

**Example**
The following query calculates the average for the temp attribute of the `TempStream` input stream within each time
window of 10 minutes. Then it emits the events to the `AvgTempStream` output stream. Each event emitted to the output
stream has the following.

* `avgTemp` attribute with the average temperature calculated as the value.
* `roomNo` and deviceID attributes with their original values from the input stream.

```sql
from TempStream#window.time(10 min)select avg(temp) as avgTemp, roomNo, deviceID
insert all events into AvgTempStream;
```

#### Aggregate function types
The following aggregate function types are supported

##### Group By
**Group By** allows you to group the aggregate based on specified attributes.

**Syntax**

```sql
from <input_stream>#window.<window_name>select <aggregate_function>(<attribute_name>) as <attribute_name>, <attribute1_name>, <attribute2_name>, ...
group by <attribute_name>, <attribute1_name>, <attribute2_name>, ...
insert into <output_stream>;
```

The parameters configured are as follows.

**Example**
The following query calculates the average temperature (`avgTemp`) per room number (`roomNo`) within each sliding time
window of 10 minutes (`#window.time(10min)`) in the `TempStream` input stream, and inserts the output into the
`AvgTempStream` output stream.

```sql
`<br>from TempStream#window.time(10 min)select avg(temp) as avgTemp, roomNo, deviceID</br>
<br>group by roomNo, deviceID</br>
insert into AvgTempStream;`
```

##### Having
Having allows you to filter events after aggregation and after processing at the selector.

**Syntax**
```sql
`<br>from <input_stream>#window.<window_name>select <aggregate_function>(<attribute_name>) as <attribute_name>, 
<attribute1_name>, <attribute2_name>, ...</br>
<br>group by <attribute_name>, <attribute1_name>, <attribute2_name>, ...</br>
<br>having <condition></br>
 insert into <output_stream>;
```

The query parameters configured are as follows.

**Example**

The following query calculates the average temperature for the last 10 minutes, and alerts if it exceeds 30 degrees.
```sql
`<br>from TempStream#window.time(10 min)select avg(temp) as avgTemp, roomNo</br>
<br>group by roomNo</br>
<br>having avgTemp > 30</br>
 insert into AlertStream;
```
##### Inbuilt aggregate functions
Aggregate functions are used to perform operations such as sum on aggregated set of events through a window. Usage of 
an aggregate function is as follows.

```sql
@info(name = 'query1')
from cseEventStream#window.timeBatch(1 sec)
select symbol, sum(price) as price
insert into resultStream;
```

###### Sum
Detail|Value
----------|----------
**Syntax**|`<long|double> sum(<int|long|double|float> arg)`
**Extension Type**|Aggregate Function
**Description**|Calculates the sum for all the events.
**Parameter**|The value that needs to be summed.|
**Return Type** |Returns long if the input parameter type is `int` or `long`, and returns double if the input parameter type is `float` or `double`.
**Examples**|<br>`sum(20)` returns the sum of 20s as a long value for each event arrival and expiry.</br> `sum(temp)` returns the sum of all temp attributes based on each event arrival and expiry.

###### Average
Detail|Value
---------|---------
**Syntax**|`<double> avg(<int|long|double|float> arg)`
**Extension Type**| Aggregate Function
**Description**| Calculates the average for all the events.
**Parameter**|`arg` : The value that need to be averaged.
**Return Type**| Returns the calculated average value as a double.
**Example**| `avg(temp)` returns the average temp value for all the events based on their arrival and expiry.

###### Maximum
Detail|Value
---------|---------
**Syntax**|`<int|long|double|float> min(<int|long|double|float> arg)`
**Extension Type**|Aggregate Function
**Description**|Returns the minimum value for all the events.
**Parameter**}|**`arg`**: The value that needs to be compared to find the minimum value.
**Return Type**|Returns the minimum value in the same type as the input.
**Example**|`min(temp)` returns the minimum temp value recorded for all the events based on their arrival and expiry.

###### Minimum
Detail|Value
---------|---------
**Syntax**|`<int|long|double|float> min(<int|long|double|float> arg)`
**Extension Type**|Aggregate Function
**Description**|Returns the minimum value for all the events.
**Parameter**|`arg`: The value that needs to be compared to find the minimum value.
**Return Type**|Returns the minimum value in the same type as the input.
**Example**|`min(temp)` returns the minimum temp value recorded for all the events based on their arrival and expiry.

###### Count
Detail|Value
---------|---------
***Syntax***|`<long> count()`
***Extension Type***|Aggregate Function
***Description***|Returns the count of all the events.
***Return Type***|Returns the event count as a long.
***Example***|`count()` returns the count of all the events.

###### Standard Deviation
Detail|Value
---------|---------
***Syntax***|`<double> stddev(<int|long|double|float> arg)`
***Extension***|Aggregate Function
***Description***|Returns the calculated standard deviation for all the events.
***Parameter***|`arg`:The value that should be used to calculate the standard deviation.

###### Distinct Count
Detail|Value
---------|---------
***Syntax***|`<long> distinctcount(<int|long|double|float|string> arg)`
***Extension***|Aggregate Function
***Description***|Returns the count of distinct occurrences for a given arg.
***Parameter***|`arg`: The value that should be counted.
***Return Type***|Returns the count of distinct occurances for a given arg.
***Example***|<br>`distinctcount(pageID)` for the following output returns `3`.</br> <br>"WEB_PAGE_1"</br><br>"WEB_PAGE_1"</br><br>"WEB_PAGE_2"</br><br>"WEB_PAGE_3"</br><br>"WEB_PAGE_1"</br><br>"WEB_PAGE_2"</br>

###### Forever Maximum
Detail|Value
---------|---------
***Syntax***|`<int|long|double|float> maxForever (<int|long|double|float> arg)`
***Extension***|Aggregate Function
***Description***|This is the attribute aggregator to store the maximum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.
***Parameter***|`arg`: The value that needs to be compared to find the maximum value.
***Return Type***|Returns the maximum value in the same data type as the input.
***Example***|`maxForever(temp)` returns the maximum temp value recorded for all the events throughout the lifetime of the query.

###### Forever Minimum
Detail|Value
---------|---------
***Syntax***|`<int|long|double|float> minForever (<int|long|double|float> arg)`
***Extension***| Aggregate Function
***Description***| This is the attribute aggregator to store the minimum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.
***Parameter***|`arg`: The value that needs to be compared to find the minimum value.
***Return Type***|Returns the minimum value in the same data type as the input.
***Example***|`minForever(temp)` returns the minimum temp value recorded for all the events throughout the lifetime of the query.

#### Inbuilt windows
WSO2 Siddhi supports the following inbuilt windows.

##### time
Detail|Value
---------|---------
***Syntax***|`<event> time(<int|long|time> windowTime)`
***Extension Type***|Window
***Description***|A sliding time window that holds events that arrived during the last windowTime period at a given time, and gets updated for each event arrival and expiry.
***Parameter***|`windowTime`: The sliding time period for which the window should hold events.
***Return Type***|Returns current and expired events.
***Examples***|<br>`time(20)` for processing events that arrived within the last 20 milliseconds.</br><br>`time(2 min)` for processing events that arrived within the last 2 minutes.</br>

##### timeBatch
Detail|Value
---------|---------
***Syntax***|`<event> timeBatch(<int|long|time> windowTime, <int> startTime )`
***Extension***|Window
***Description***|A batch (tumbling) `time window` that holds events that arrive during `windowTime` periods, and gets updated for each `windowTime`.
***Parameters***|<br>`windowTime`: The batch time period for which the window should hold events.</br> <br>`startTime (Optional)`: This specifies an offset in milliseconds in order to start the window at a time different to the standard time.</br>
***Return Type***|Returns current and expired events.
***Examples***|<br>`timeBatch(20)` processes events that arrive every 20 milliseconds.<br>`timeBatch(2 min)` processes events that arrive every 2 minutes.</br><br>`timeBatch(10 min, 0)` processes events that arrive every 10 minutes starting from the 0th minute. e.g., If you deploy your window at 08:22 and the first event arrives at 08:26, this event occurs within the time window 08.20 - 08.30. Therefore, this event is emitted at 08.30.</br> <br>`timeBatch(10 min, 1000*60*5)`processes events that arrive every 10 minutes starting from 5th minute. e.g., If you deploy your window at 08:22 and the first event arrives at 08:26, this event occurs within the time window 08.25 - 08.35. Therefore, this event is emitted at 08.35.</br>

###### length
Detail|Value
---------|---------
***Syntax***|`<event> length(<int> windowLength)`
***Extension Type***|Window
***Description***|A sliding length window that holds the last `windowLength` events at a given time, and gets updated for each arrival and expiry.
***Parameter***|`windowLength`: The number of events that should be included in a sliding length window.
***Return Type***"|Returns current and expired events.
***Examples***|<br>`length(10)` for processing the last 10 events.</br><br>`length(200)` for processing the last 200 events.</br>

##### lengthBatch
Detail|Value
---------|---------
***Syntax***|`<event> lengthBatch(<int> windowLength)`
***Extension Type***|Window
***Description***|A batch (tumbling) length window that holds a number of events specified as the `windowLength`. The window is updated each time a batch of events that equals the number specified as the `windowLength`arrives.
***Parameter***|`windowLength`:The number of events the window should tumble.
***Return Type***|Returns current and expired events.
***Examples***|<br>`lengthBatch(10)` for processing 10 events as a batch.</br>`lengthBatch(200)` for processing 200 events as a batch.

##### externalTime
Detail|Value
---------|---------
***Syntax***|`<event> externalTime(<long> timestamp, <int|long|time> windowTime)`
***Extension Type***| Window
***Description***|A sliding time window based on external time. It holds events that arrived during the last `windowTime` period from the external timestamp, and gets updated on every monotonically increasing timestamp.
***Parameter***| `windowTime`:The sliding time period for which the window should hold events.
***Return Type***| Returns current and expired events.
***Examples***|<br>`externalTime(eventTime,20)` for processing events arrived within the last 20 milliseconds from the `eventTime`.<br>`externalTime(eventTimestamp, 2 min)` for processing events arrived within the last 2 minutes from the `eventTimestamp`.</br>

##### cron
Detail|Value
---------|---------
***Syntax***|`<event> cron(<string> cronExpression)`
***Extension Type***|Window
***Description***|This window returns events processed periodically as the output in time-repeating patterns, triggered based on time passing.
***Parameter***|`cronExpression`: cron expression that represents a time schedule.
***Return Type***|Returns current and expired events.
***Examples***|`cron('*/5 * * * * ?')` returns processed events as the output every 5 seconds.

##### firstUnique
Detail|Value
---------|---------
***Syntax***|`<event> firstUnique(<string> attribute)`
***Extension***|Window
***Description***|First unique window processor keeps only the first events that are unique according to the given unique attribute.
***Parameter***:`attribute`: The attribute that should be checked for uniqueness.
***Return Type***|Returns current and expired events.
***Examples***|`firstUnique(ip)` returns the first event arriving for each unique ip.

##### unique
Detail|Value
---------|---------
***Syntax***|`<event>  unique (<string>  attribute )`
***Extension***|Window
***Description***|This window keeps only the latest events that are unique according to the given unique attribute.
***Parameter***|`attribute`: The attribute that should be checked for uniqueness.
***Return Type***|Returns current and expired events.
***Example***|`unique(ip)` returns the latest event that arrives for each unique `ip`.

##### sort
Detail|Value
---------|---------
***Syntax***|<br>`<event> sort(<int> windowLength)`</br><br>`<event> sort(<int> windowLength, <string> attribute, <string> order)`</br><br>`<event> sort(<int> windowLength, <string> attribute, <string> order, .. , <string> attributeN, <string> orderN)`</br>
***Extension***|Window
***Description***|This window holds a batch of events that equal the number specified as the windowLength and sorts them in the given order.
***Parameter***|`attribute`: The attribute that should be checked for the order.
***Return Type***|Returns current and expired events.
***Example***|`sort(5, price, 'asc')` keeps the events sorted by `price` in the ascending order. Therefore, at any given time, the window contains the 5 lowest prices.

##### frequent
Detail|Value
---------|---------
***Syntax***|<br>`<event> frequent(<int> eventCount)`</br><br>`<event> frequent(<int> eventCount, <string> attribute, .. , <string> attributeN)`</br>
***Extension Type***|Window
***Description***|This window returns the latest events with the most frequently occurred value for a given attribute(s). Frequency calculation for this window processor is based on Misra-Gries counting algorithm.
***Parameter***|`eventCount`: The number of most frequent events to be emitted to the stream.
***Return Type***|The number of most frequent events to be emitted to the stream.
***Examples***|<br>`frequent(2)` returns the 2 most frequent events.</br><br>`frequent(2, cardNo)` returns the 2 latest events with the most frequently appeared card numbers.</br>

##### lossyFrequent
Detail|Value
---------|---------
***Syntax***|<br>`<event> lossyFrequent(<double> supportThreshold, <double> errorBound)`</br><br>`<event> lossyFrequent(<double> supportThreshold, <double> errorBound, <string> attribute, .. , <string> attributeN)`</br>
***Extension Type***|Window
***Description***|This window identifies and returns all the events of which the current frequency exceeds the value specified for the `supportThreshold` parameter.
***Parameters***|<br>`errorBound`: The error bound value.</br><br>`attribute`:The attributes to group the events. If no attributes are given, the concatenation of all the attributes of the event is considered.</br>
***Return Type***|Returns current and expired events.
***Examples***|<br>`lossyFrequent(0.1, 0.01)` returns all the events of which the current frequency exceeds `0.1`, with an error bound of `0.01`.</br><br>'lossyFrequent(0.3, 0.05, cardNo)` returns all the events of which the `cardNo` attributes frequency exceeds `0.3`, with an error bound of `0.05`.

##### externalTimeBatch
Detail|Value
---------|---------
***Syntax***|`<event> externalTimeBatch(<long> timestamp, <int|long|time> windowTime, <int|long|time> startTime, <int|long|time> timeout)`
***Extension Type***|Window
***Description***|A batch (tumbling) time window based on external time, that holds events arrived during `windowTime` periods, and gets updated for every `windowTime`.
***Parameters***|<br>`timestamp`: The time that the window determines as current time and will act upon. The value of this parameter should be monotonically increasing.</br><br>`windowTime`: The batch time period for which the window should hold events.</br><br>`startTime` (Optional): User defined start time. This could either be a constant (of type int, long or time) or an attribute of the corresponding stream (of type long). If an attribute is provided, initial value of attribute would be considered as `startTime`. When `startTime` is not given, initial value of `timestamp` is used as the default.<br>`timeout`(Optional): Time to wait for arrival of new event, before flushing and giving output for events belonging to a specific batch. If timeout is not provided, system waits till an event from next batch arrives to flush current batch.</br>
***Return Type***|Returns current and expired events.
***Examples***| <br>`externalTimeBatch(eventTime,20)` for processing events that arrive every 20 milliseconds from the `eventTime`.</br><br>`externalTimeBatch(eventTimestamp, 2 min)` for processing events that arrive every 2 minutes from the `eventTimestamp`.</br><br>`externalTimeBatch(eventTimestamp, 2 sec, 0)` for processing events that arrive every 2 seconds from the `eventTimestamp`. This starts on 0th millisecond of an hour.</br><br>`externalTimeBatch(eventTimestamp, 2 sec, eventTimestamp, 100)` for processing events that arrive every 2 seconds from the `eventTimestamp`. This considers the `eventTimeStamp` of the first event as the start time, and waits 100 milliseconds for the arrival of a new event before flushing the current batch.</br>

##### timeLength
Detail|Value
---------|---------
***Syntax***|`<event> timeLength ( < int|long|time >   windowTime,  < int >  windowLength ) `
***Extension Type***|Window
***Description***|A sliding time window that, at a given time holds the last windowLength events that arrived during last windowTime period, and gets updated for every event arrival and expiry.
***Parameters***|<br>`windowTime`: The sliding time period for which the window should hold events.</br><br>`windowLength`: The number of events that should be be included in a sliding length window.</br>
***Return Type***|Returns current and expired events.
***Examples***|<br>`timeLength(20 sec, 10)` for processing the last 10 events that arrived within the last 20 seconds.</br><br>`timeLength(2 min, 5)` for processing the last 5 events that arrived within the last 2 minutes.</br>
 
##### uniqueExternalTimeBatch
Detail|Value
 ---------|---------
***Syntax***|`<event> uniqueExternalTimeBatch(<string> attribute, <long> timestamp, <int|long|time> windowTime, <int|long|time> startTime, <int|long|time> timeout, <bool> replaceTimestampWithBatchEndTime )`
***Extension Type***|Window
***Description***|A batch (tumbling) time window based on external time that holds latest unique events that arrive during the `windowTime` periods, and gets updated for each `windowTime`.
***Parameters***|<br>`attribute`:The attribute that should be checked for uniqueness.</br><br>`timestamp`:The time considered as the current time for the window to act upon. The value of this parameter should be monotonically increasing.</br><br>`windowTime`:The batch time period for which the window should hold events.</br>`startTime` (Optional): The user-defined start time. This can either be a constant (of the `int`, `long` or `time` type) or an attribute of the corresponding stream (of the long type). If an attribute is provided, the initial value of this attribute is considered as the `startTime`. When the `startTime` is not given, the initial value of the timestamp is used by default.</br><br>`timeout` (Optional): The time duration to wait for the arrival of new event before flushing and giving output for events belonging to a specific batch. If the timeout is not provided, the system waits until an event from next batch arrives to flush the current batch.</br><br>`replaceTimestampWithBatchEndTime` (Optional): Replaces the value for the `timestamp` parameter with the corresponding batch end time stamp.</br>
***Return Type***|Returns current and expired events.
***Examples***|<br>`uniqueExternalTimeBatch(ip, eventTime, 20)` processes unique events based on the `ip` attribute that arrives every 20 milliseconds from the  `eventTime`.</br><br>`uniqueExternalTimeBatch(ip, eventTimestamp, 2 min)` processes unique events based on the `ip` attribute that arrives every 2 minutes from the `eventTimestamp`.</br><br>`uniqueExternalTimeBatch(ip, eventTimestamp, 2 sec, 0)` processes unique events based on the `ip` attribute that arrives every 2 seconds from the `eventTimestamp`. It starts on 0th millisecond of an hour.</br><br>`uniqueExternalTimeBatch(ip, eventTimestamp, 2 sec, eventTimestamp, 100)` processes unique events based on the `ip` attribute that arrives every 2 seconds from the `eventTimestamp`. It considers the `eventTimestamp` of the first event as the `startTime`, and waits 100 milliseconds for the arrival of a new event before flushing current batch.</br><br>`uniqueExternalTimeBatch(ip, eventTimestamp, 2 sec, eventTimestamp, 100, true)` processes unique events based on the ip attribute that arrives every 2 seconds from the `eventTimestamp`. It considers the `eventTimestamp` of the first event as `startTime`, and waits 100 milliseconds for the arrival of a new event before flushing current batch. Here, the value for `eventTimestamp` is replaced with the timestamp of the batch end.
 
#### Output event categories
The output of a window can be current events, expired events or both as specified. Following are the key words to be used to specify the event category that is emitted from a window.
 
Key Word|Outcome
---------|---------
`current events`|This emits all the events that arrive to the window. This is the default event category emitted when no particular category is specified.
`expired events`|This emits all the events that expire from the window.
`all events`|This emits att the events that arrive to the window as well as all the events that expire from the window.
 
**Syntax**
```sql
from <input_stream>#window.<window_name>select *
insert <output_event_category> into <output_stream>
```
**Example**
The following selects all the expired events of the `TempStream` input stream within a time window of 1 minute, and inserts them into the `DelayedTempStream` output stream.
```sql
from TempStream#window.time(1 min)select *
insert expired events into DelayedTempStream
```
 
## Output rate limiting
Output rate limiting allows queries to emit events periodically based on the condition specified.

**Purpose**

This allows you to limit the output to what is required in order to avoid viewing unnecessary information.

**Syntax**

The following is the syntax of an output rate limiting configuration.

```sql
from <input stream name>...select <attribute name>, <attribute name>, ...
output ({<output-type>} every (<time interval>|<event interval> events) | snapshot every <time interval>)
insert into <output stream name>
```
The parameters configured for the output rate limiting are described in the following table.

Parameter|Description
---------|---------
`output-time`|<br>This specifies which event(s) should be emitted as the output of the query. The possible values are as follows:</br><br>*`first`*: Only the first event processed by the query in the specified time interval/sliding window is emitted.</br><br>*`last`*: Only the last event processed by the query in the specified time interval/sliding window is emitted.</br><br>*`all`*: All the events processed by the query in the specified time interval/sliding window are emitted.</br>
`time interval`|The time interval for the periodic event emission.
`event interval`|The number of events that defines the interval at which the emission of events take place. e.g., if the event interval is 10, the query emits a result for every 10 events.

**Examples**

+ Emitting events based on number of events
<br>Here the events are emitted every time the specified number of events arrive You can also specify whether to to emit only the first event, last event, or all events out of the events that arrived.</br>
In this example, the last temperature per sensor is emitted for every 10 events.
```sql
from TempStreamselect temp
group by deviceID
output last every 10 events
insert into LowRateTempStream;
```
+ Emitting events based on time
Here, events are emitted for every predefined time interval. You can also specify whether to to emit only the first event, last event, or all events out of the events that arrived during the specified time interval.

```sql
from TempStreamoutput every 10 sec
insert into LowRateTempStream;
```
+ Emitting a periodic snapshot of events
<br>This method works best with windows. When an input stream is connected to a window, snapshot rate limiting emits all the current events that have arrived and do not have corresponding expired events for every predefined time interval. If the input stream is not connected to a window, only the last current event for each predefined time interval is emitted.</br>
The following emits a snapshot of the events in a time window of 5 seconds every 1 second. 

```sql
from TempStream#window.time(5 sec)output snapshot every 1 sec
insert into SnapshotTempStream;
```

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

**Example**
The following generates output events for all the events in the `twitterStream` stream whether there is a match for the symbol in the `stockStream` stream or not.
```sql
from stockStream#window.length(2) 
right outer join twitterStream#window.length(1)
on stockStream.symbol== twitterStream.symbol
select stockStream.symbol as symbol, twitterStream.tweet, stockStream.price
insert all events into outputStream ;
```

### Full Outer Join

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

**Example**
The following query sends an alert when the room temperature reaches the temperature set on the regulator. The pattern matching is reset every time the temperature set on the regulator changes.
```sql
define stream TempStream(deviceID long, roomNo int, temp double);
define stream RegulatorStream(deviceID long, roomNo int, tempSet double);
  
from every( e1=RegulatorStream ) -> e2=TempStream[e1.roomNo==roomNo and e1.tempSet <= temp ] or e3=RegulatorStream[e1.roomNo==roomNo]
select e1.roomNo, e2.temp as roomTemp
having e3 is null
insert into AlertStream;
```

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

**Example**

The following query creates a notification when a regulator event is followed by both temperature and humidity events.

```sql
define stream TempStream(deviceID long, temp double);define stream HumidStream(deviceID long, humid double);
define stream RegulatorStream(deviceID long, isOn bool);
  
from every e1=RegulatorStream, e2=TempStream and e3=HumidStream
select e2.temp, e3.humid
insert into StateNotificationStream;
```
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

## Event Table

An event table is a stored version of an event stream or a table of events. It allows Siddhi to work with stored events. Events are stored in-memory by default, and Siddhi also provides an extension to work with data/events stored in RDBMS data stores.

### Defining An Event Table

An event table definition defines the table schema. The syntax for an event table definition is as follows.
```sql
define table <table name> (<attribute name> <attribute type>, <attribute name> <attribute type>, ... );
```

**Example**

The following creates a table named `RoomTypeTable` with the attributes `roomNo` (an `INT` attribute) and `type` (a `STRING` attribute).

```sql
define table RoomTypeTable (roomNo int, type string);
```

### Event Table Types

Siddhi supports the following types of event tables.

#### In-memory Event Table

In memory event tables are created to store events for later access. Event tables can be configured with primary keys to avoid the duplication of data, and indexes for fast event access.
Primary keys are configured by including the `@PrimaryKey` annotation within the event table configuration. Each event table can have only one attribute defined as the primary key, and the value for this attribute should be unique for each entry saved in the table. This ensures that entries in the table are not duplicated
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

#### Hazlecast Event Table

Event tables allow event data to be persisted in a distributed manner using Hazelcast in-memory data grids. This functionality is enabled using the `From` annotation. Also, the connection instructions for the Hazelcast cluster can be provided with the `From` annotation.
The following is a list of connection instructions that can be provided with the `From` annotation.

+ `cluster.name : Hazelcast cluster/group name [Optional]  (i.e cluster.name='cluster_a')`
+ `cluster.password : Hazelcast cluster/group password [Optional]  (i.e cluster.password='pass@cluster_a')`
+ `cluster.addresses : Hazelcast cluster addresses (ip:port) as a comma separated string [Optional, client mode only] (i.e cluster.addresses='192.168.1.1:5700,192.168.1.2:5700')`
+ `well.known.addresses : Hazelcast WKAs (ip) as a comma separated string [Optional, server mode only] (i.e well.known.addresses='192.168.1.1,192.168.1.2')`
+ `collection.name : Hazelcast collection object name [Optional, can be used to share single table between multiple EPs] (i.e collection.name='stockTable')`

**Examples**

+ Creating a table backed by a new Hazelcast instance

The following query creates an event table named `RoomTypeTable` with the attributes `roomNo` (an `INT` attribute) and `type` (a `STRING` attribute), backed by a *new* Hazelcast Instance.
 
```sql
@from(eventtable = 'hazelcast')
define table RoomTypeTable(roomNo int, type string);
```

+ Creating a table backed by a new Hazelcast instance in a new Hazelcast cluster
The following query creates an event table named `RoomTypeTable` with the attributes `roomNo` (an `INT` attribute) and `type` (a `STRING` attribute), backed by a *new* Hazelcast Instance in a new Hazelcast cluster.

```sql
@from(eventtable = 'hazelcast', cluster.name = 'cluster_a', cluster.password = 'pass@cluster_a')
define table RoomTypeTable(roomNo int, type string);
```

+ Creating a table backed by an existing Hazelcast instance in an existing Hazelcast cluster
The following query creates an event table named `RoomTypeTable` with the attributes `roomNo` (an `INT` attribute) and `type` (a `STRING` attribute), backed by an existing Hazelcast Instance in an existing Hazelcast Cluster.

```sql
@from(eventtable = 'hazelcast', cluster.name = 'cluster_a', cluster.password = 'pass@cluster_a', cluster.addresses='192.168.1.1:5700,192.168.1.2.5700')
define table RoomTypeTable(roomNo int, type string);
```

#### RDBMS Event Table

An event table can be backed with an RDBMS event store using the `From` annotation. You can also provide the connection instructions to the event table with this annotation. The RDBMS table name can be different from the event table name defined in Siddhi, and Siddhi always refers to the defined event table name. However the defined event table name cannot be same as an already existing stream name because syntactically, both are considered the same in the Siddhi Query Language.

*The RDBMS event table has been tested with the following databases:

+ MySQL
+ H2
+ Oracle*

**Example**

+ Creating an event table backed by an RDBMS table

The following query creates an event table named `RoomTypeTable` with the attributes `roomNo` (an `INT` attribute) and `type` (a `STRING` attribute), backed by an RDBMS table named `RoomTable` from the data source named `AnalyticsDataSource`.
```sql
@From(eventtable='rdbms', datasource.name='AnalyticsDataSource', table.name='RoomTable')
define table RoomTypeTable (roomNo int, type string);
```

The datasource.name given here is injected to the Siddhi engine by the CEP/DAS server. To configure data sources in the CEP/DAS, see WSO2 Administration Guide - Configuring an RDBMS Datasource.
+ Creating an event table backed by a MySQL table
The following query creates an event table named RoomTypeTable with the attributes roomNo (an INT attribute) and type (a STRING attribute), backed by a MySQL table named `RoomTable` from the `cepdb` database located at `localhost:3306` with `root` as both the username and the password.

##### Caching Events
<br>Several caches can be used with RDBMS backed event tables in order to reduce I/O operations and improve their performance. Currently all cache implementations provide size-based algorithms.</br>

The following elements are added with the Fromannotation to add caches.

+ `cache`: This specifies the cache implementation to be added. The supported cache implementations are as follows.

    * `Basic`: Events are cached in a First-In-First-Out manner where the oldest event is dropped when the cache is full.
    
    * `LRU` (Least Recently Used): The least recently used event is dropped when the cache is full.
    
    * `LFU` (Least Frequently Used): The least frequently used event is dropped when the cache is full.
    
    If the `cache` element is not specified, the basic cache is added by default.

+ `cache.size`: This defines the size of the cache. If this element is not added, the default cache size of 4096 is added by default.


**Example**

The following query creates an event table named `RoomTypeTable` with the attributes `roomNo` (an `INT` attribute) and `type` (a `STRING` attribute), backed by an RDBMS table using the LRU algorithm for caching 3000 events.

```sql
@From(eventtable='rdbms', datasource.name='AnalyticsDataSource', table.name='RoomTable', cache='LRU', cache.size='3000')define table RoomTypeTable (roomNo int, type string);
```

##### Using Bloom filters

A Bloom Filter is an algorithm or an approach that can be used to perform quick searches. If you apply a Bloom Filter to a data set and carry out an `isAvailablecheck` on that specific Bloom Filter instance, an accurate answer is returned if the search item is not available. This allows the quick improvement of updates, joins and `isAvailable` checks.

**Example**

The following example shows how to include Bloom filters in an event table update query.

```sql
define stream StockStream (symbol string, price float, volume long);define stream CheckStockStream (symbol string, volume long);
@from(eventtable = 'rdbms' ,datasource.name = 'cepDB' , table.name = 'stockInfo' , bloom.filters = 'enable')
define table StockTable (symbol string, price float, volume long);
   
@info(name = 'query1')
from StockStream
insert into StockTable ;
   
@info(name = 'query2')
from CheckStockStream[(StockTable.symbol==symbol) in StockTable]
insert into OutStream;
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

*Example*
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
## Event Window
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
## Event Trigger
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

## Eval Script

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

