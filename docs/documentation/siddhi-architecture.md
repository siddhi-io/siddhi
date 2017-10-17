# Siddhi Architecture 

WSO2 Siddhi is a software library that can be utilized in any of the following ways:

- Run as a server on its own
- Run within WSO2 SP as a service
- Embedded into any Java or Python based application
- Run on an Android application
 
It provides analytical operators, orchestrates data flows, calculates analytics, and detects
patterns on event data from multiple disparate live data sources. This allows developers to build applications that sense, 
think, and act in real time.

This section illustrates the architecture of Siddhi and guides you through its key functionality. We hope this article 
helps developers to understand Siddhi and its the codebase better, and also contribute to improve Siddhi by reporting and fixing bugs.

## Main Design Decisions

- Event by event processing of real-time streaming data to achieve low latency. 
- Ease of use with Streaming SQL providing an intuitive way to express stream processing logic and complex 
event processing constructs such as Patterns.  
- Achieve high performance by processing all events in-memory and by storing their states in-memory. 
- Optimizing performance by enforcing a strict schema for event streams and by pre-compiling the queries.
- Optimizing memory consumption by having only the absolutly necessary information in-memory and 
dropping the rest as soon as possible. 
- Supporting multiple extension points to accommodate a diverse set of functionality such as supporting multiple sources, sinks, functions, 
aggregation operations, windows, etc.

## High Level Architecture

![Simple Siddhi Overview](../images/architecture/siddhi-overview-simple.png?raw=true "Simple Siddhi Overview")
 
At a high level, Siddhi consumes events from various events sources, processes them according to the defined Siddhi application, 
and produces results to the subscribed event sinks. 
Siddhi can store and consume events from in-memory tables or from external data stores such as `RDBMS`, `MongoDB`, 
`Hazelcast` in-memory grid, etc. (i.e., when configured to do so). Siddhi also allows applications and users to query Siddhi via its Store Query API to interactively 
retrieve data from its in-memory and other stores.
 
### Main Modules in Siddhi

Siddhi comprises four main modules, they are: 

- **[Siddhi Query API](https://github.com/wso2/siddhi/tree/master/modules/siddhi-query-api)** : This allows you to define the execution logic of the Siddhi application as queries and definitions using POJO classes. 
Internally, Siddhi uses these objects to identify the tasks that it is expected to perform. 

- **[Siddhi Query Compiler](https://github.com/wso2/siddhi/tree/master/modules/siddhi-query-compiler)** : This allows you to define the Siddhi application using the Siddhi Streaming SQL, 
 and then it converts the Streaming SQL script to Siddhi Query POJO Objects so that Siddhi can execute them. 
 
- **[Siddhi Core](https://github.com/wso2/siddhi/tree/master/modules/siddhi-core)** : This builds the execution runtime based on the defined Siddhi Application and processes the events as and when they arrive. 
 
- **[Siddhi Annotation](https://github.com/wso2/siddhi/tree/master/modules/siddhi-annotations)** : This is a helper module that allows all extensions to be annotated, so that they can be 
picked by Siddhi Core for processing. This also helps Siddhi to generate the extension documentation. 

## Siddhi Component Architecture 

The following diagram illustrates the mail components of Siddhi and how they work together. 

![Siddhi Component Architecture](../images/architecture/siddhi-architecture-highlevel.png "Siddhi Component Architecture")
 
Here [Siddhi Core](https://github.com/wso2/siddhi/tree/master/modules/siddhi-core) module maintains
the execution logic. It also interacts with the external environment and systems 
for consuming and publishing events. To achieve these tasks, it uses the following components:  
 
- [SiddhiManager](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/SiddhiManager.java) : 
  This is a key component of Siddhi Core that manages Siddhi Application Runtimes 
  and facilitates their functionality via Siddhi Context with periodic state persistence, statistics reporting and extension loading. 
   
- [SiddhiAppRuntime](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/SiddhiAppRuntime.java) : 
 One Siddhi Application Runtime is generated for each Siddhi Application deployed. Siddhi Application Runtimes
 provide an isolated execution environment for each Siddhi Application defined.
 These Siddhi Application Runtimes are based on the logic of their Siddhi Application, and they consume and publish events from various external systems and Java or Python programmes. 
  
- [SiddhiContext](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/config/SiddhiContext.java) : 
This is a shared object for all the Siddhi Application Runtimes within the a Siddhi manager, and it contains references 
to the persistence store for periodic persistence, statistics manager to report performance statistics of Siddhi Application Runtimes, 
and extension holders for loading Siddhi extensions. 

## Siddhi Application Creation
 
Execution logic in Siddhi is composed as a Siddhi Application, and this is passed as a string to 
**SiddhiManager** to create the **SiddhiAppRuntime** for execution. 

When a Siddhi Application is passed to the `SiddhiManager.createSiddhiAppRuntime()`, it is processed internally with the 
[SiddhiCompiler](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-compiler/src/main/java/org/wso2/siddhi/query/compiler/SiddhiCompiler.java). 
Here, the **SiddhiApp** String is converted to [SiddhiApp](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/SiddhiApp.java)
 object model by the [SiddhiQLBaseVisitorImpl](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-compiler/src/main/java/org/wso2/siddhi/query/compiler/internal/SiddhiQLBaseVisitorImpl.java) class. 
The model is then passed to the [SiddhiAppParser](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/util/parser/SiddhiAppParser.java) 
for the **SiddhiAppRuntime** creation.


## Siddhi App Execution Flow

Following diagram depicts the execution flow within a Siddhi App Runtime. 

![Execution Flow in Siddhi App](../images/architecture/siddhi-event-flow.png "Execution Flow in Siddhi App")
 
The path taken by events within Siddhi is indicated in blue. 

The components that are involved in handling the events are as follows: 

- [StreamJunction](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/StreamJunction.java)
    
    This routes events to various components within the Siddhi App Runtime. A stream junction is
    generated for each stream defined or inferred in the Siddhi Application. A stream junction by default uses the incoming event's thread
    for processing subscribed components, but it can also be configured via the `@Async` annotation to buffer the events and 
    use a different thread for subsequent execution.
    
- [InputHandler](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/input/InputHandler.java) 
    
    An instance of input handler is created for each stream junction, and this is used for pushing `Event` and
     `Event[]` objects into stream junctions from sources, and Java/Python programmes. 
    
- [StreamCallback](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/output/StreamCallback.java) 

    This receives `Event[]`s from stream junction and passes them to sinks to publish to external endpoints, 
    or passes them to subscribed Java/Python programmes for further processing. 
    
- [Queries](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/QueryRuntime.java) & [Partitions](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/PartitionRuntime.java) 

    These components process the events by filtering, transforming, joining, patten matching, 
    etc. They consume events from one or more stream junctions, process them and publish the processed events 
    into stream junctions based on the defined query or partition. 
     
- [Source](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/input/source/Source.java) 
    
    Sources consume events from external sources in various data formats, convert them into Siddhi events
    and pass them to corresponding Stream Junctions via their Input Handlers. A source is generated 
    for each `@Source` annotation defined above a stream definition. Each source consumes events from an external source in the data format configured for it. 
    
- [SourceMapper](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/input/source/SourceMapper.java) 

    A source mapper needs to be configured for each source in order to convert the format of each incoming event to that of a Siddhi event. 
    The source mapper type can be configured using the `@Map` annotation within the `@Source` annotation. When the `@Map` annotation is
    not defined, Siddhi uses the [PassThroughSourceMapper](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/input/source/PassThroughSourceMapper.java),
    where it assumes that the incoming message is already in the Siddhi Event format, and therefore makes no changes to the event format.
    
- [Sink](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/output/sink/Sink.java) 

    Sinks convert the Siddhi Events to various data formats and publish them to external endpoints. 
    A Sink generated for each `@Sink` annotation defined above a stream definition to publish the events arriving 
    to that stream. 
    
- [SinkMapper](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/output/sink/SinkMapper.java)

    A sink mapper is configured for each Sink in order to map the Siddhi events to the specified data format so that they 
    can be published via the sink. The sink mapper type can be configured using the `@Map` annotation within the `@Sink`
    annotation. When the `@Map` annotation is not defined, Siddhi uses [PassThroughSinkMapper](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/stream/output/sink/PassThroughSinkMapper.java), 
    where it  passes the Siddhi events in the existing format (i.e., the Siddhi event format) to the Sink.
    
- [Table](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/table/Table.java)
    
    Tables store events. By default, Siddhi uses the [InMemoryTable](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/table/InMemoryTable.java) 
    implementation to store events in-memory. When `@Store` annotation
    is used, it loads the associated Table implantation based on the defined `store` type. Most table implementations are 
    extended from the [AbstractRecordTable](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/table/record/AbstractRecordTable.java) 
    abstract class for the ease of development.
    
- [Window](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/window/Window.java)
    
    Windows store events and determine when events can be considered expired based on the given window constrain. Multiple types of windows are
    can be implemented by extending the [WindowProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/stream/window/WindowProcessor.java) 
    abstract class. 
    
- [IncrementalAggregation](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/attribute/aggregator/incremental/IncrementalAggregationProcessor.java) 

    This allows you to obtain aggregates in an incremental manner for a specified set of time periods.
    Incremental aggregation functions can be implemented by extending [IncrementalAttributeAggregator](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/attribute/aggregator/incremental/IncrementalAttributeAggregator.java). 
     
- [Trigger](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/trigger/Trigger.java)
 
    A triggers triggers events at a given interval to the stream junction that has the same name as the trigger.
    
- [QueryCallback](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/output/callback/QueryCallback.java)

    A query callback receives notifications when events are emitted from queries. Then it notifies the event occurrence `timestamp`, and classifies 
    the events into `currentEvents`, and `expiredEvents`. 
    

## Siddhi Query Execution 

Siddhi [QueryRuntime](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/QueryRuntime.java)s
 can be categorized in to three main types such as [SingleInputStream](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/execution/query/input/stream/SingleInputStream.java) 
 queries that comprise of query types such as filters and windows,
 [JoinInputStream](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/execution/query/input/stream/JoinInputStream.java)
  queries comprising joins, and 
   [StateInputStream](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/execution/query/input/stream/StateInputStream.java) 
   queries comprising patterns and sequences. 

Following section explains the internal of each query type. 

### [SingleInputStream](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/execution/query/input/stream/SingleInputStream.java) Query Runtime (Filter & Windows)

![Single Input Stream Query](../images/architecture/siddhi-query-single.png "Single Input Stream Query (Filter & Window)")
 
Single input stream query runtime is generated for filter and window queries, they consumes events from a Stream Junction or Window
and convert the incoming events according to the expected output stream format at the [ProcessStreamReceiver](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/input/ProcessStreamReceiver.java)
 and by dropping 
all the unrelated incoming stream attributes.

Then the converted events are passed through few Processors such as [FilterProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/filter/FilterProcessor.java),
[StreamProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/stream/StreamProcessor.java), 
[StreamFunctionProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/stream/function/StreamFunctionProcessor.java), 
[WindowProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/stream/window/WindowProcessor.java)
 and [QuerySelector](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/QuerySelector.java). 
 Here StreamProcessor, StreamFunctionProcessor, and WindowProcessor can be extended with 
various stream processing capabilities. The chain of processors are always ended with a QuerySelector and the chain can 
only have at most one WindowProcessor. When query runtime consumes events from a Window its chain of processors cannot 
contain a WindowProcessor.

When it comes to the FilterProcessor, its implemented with an [ExpressionExecutor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/executor/ExpressionExecutor.java) 
that returns boolean value. Expressions have a tree structure, and 
they are processed based on the Depth First Search Algorithm. To achieve high performance, currently, Siddhi depends on the user
to formulate the lease success case in the leftmost side of the condition, thereby increasing the chances of early false detection.

The condition expression `price >= 100 and ( Symbol == 'IBM' or Symbol == 'MSFT' )` will be represented as below.

![Siddhi Expression](../images/architecture/siddhi-expressions.png "Single Input Query (Filter & Window)")

These Expressions also supports executing user defined functions (UDFs), and they can be implemented by extending the [FunctionExecutor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/executor/function/FunctionExecutor.java) class. 

Events after getting processed by each processor they will reach the QuerySelector for transformation. At the QuerySelector
Events are transformed based on the `select` clause of the query. If there is a `Group By` defined then the [GroupByKeyGenerator](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/GroupByKeyGenerator.java)
 identifies the group by key and then each [AttributeProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/attribute/processor/AttributeProcessor.java)
 is executed based on that group by key. AttributeProcessors can contain any Expression including 
 constant values, variables and user defined functions, at the same time they can also contain [AttributeAggregator](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/attribute/aggregator/AttributeAggregator.java)s
 for processing aggregation operations such as `sum`, `count`, etc. Here for each group by key a AttributeAggregator will be generated to 
keep track of the aggregation state and when it becomes obsolete it will be destroyed. Through this operation the events 
will be transformed to the expected output format. 
 
After the event got transformed to the output format it will evaluated against the HavingConditionExecutor if a `having` clause is 
provided, and only the succeeding events will be pushed to OutputRateLimiter.   

At OutputRateLimiter the events output is controlled before sending the events to the stream junction or to the query callback. 
When `output` clause is not defined PassThroughOutputRateLimiter is used by passing all the events without any rate limiting. 

#### Temporal Processing with Windows

The temporal event processing aspect is achieved through [Window](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/window/Window.java)
 and [AttributeAggregator](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/selector/attribute/aggregator/AttributeAggregator.java)s
Siddhi uses four type of events to achieve temporal processing : 

Current Events: Events that are newly arriving to a query from streams

Expired Events: Events that have expired from a window. 

Timer Events: Events informing query about an update of execution time. Usually these events are generated by Schedulers. 
 
Reset Events: Events that resets Siddhi query states.
 
In Siddhi, when an event comes into a [WindowProcessor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/stream/window/WindowProcessor.java), 
it will create an appropriate expired event corresponding to the incoming current event with the expiring timestamp and store that 
event in the window. The WindowProcessor will also forward the current event to the next Processor for further processing. 
WindowProcessor using a scheduler or otherwise decides when to emit the events it has in memory and during that time it emits 
those events as expired events, at times if its emitting all the events in the memory at ones it emits a single reset event interred of 
sending one expired event for each event it has stored to reset the states in one go. From a window for each current event it has emitted it is essential 
to emmit the same event as an expired event or a common reset event should be emitted.  
This is vital in Siddhi because Siddhi relies on these events to calculate the Aggregations at the QuerySelector. 
In QuerySelector, the arrived current events increase the aggregation values, expired events decrease the values, and 
reset events reset the aggregation calculation.

For example if we take the sliding [TimeWindow](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/query/processor/stream/window/TimeWindowProcessor.java),
 when a current event arrives it creates the corresponding expired event, adds that to 
the window, adds an entry to the Scheduler to notify when that event need to be expired by sending it a timer event, and 
finally sends the current event to the next processor for subsequent processing.  
When the window get an indication that the expected expiry time has come for the oldest event in the window via a 
a timer event or other means it removes the expired event from the window and pass that to the next processor. 

![Siddhi Time Window](../images/architecture/siddhi-time-window.png "Siddhi Time Window")

### [JoinInputStream](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/execution/query/input/stream/JoinInputStream.java) Query Runtime (Join)
 
![Join Input Stream Query](../images/architecture/siddhi-query-join.png "Join Input Stream Query")
 
Join input stream query runtime is generated for join queries. This can consume events from two Stream Junction as perform join operation as depicted above. 
It can also perform Join by consuming events from one Stream Junction and join against itself or it can join against a 
table, window or an incremental aggregation. When join is performed with table, window or incremental aggregation
the WindowProcessor in the image will be replaced with the table, window or incremental aggregation and on their side no  
basic processors will be used. 

The joining operation will be triggered by the events arriving from the Stream Junction.
Here when an event from one stream reaches the pre JoinProcessor, it matches against all the available events of the other stream's WindowProcessor. 
When a match is found, those matched events are then sent to the QuerySelector as current events; at the same time, 
the original event will be added to the WindowProcessor and it will remain there until it expires. Similarly, when an 
event expires from WindowProcessor, it matches against all the available events of the other stream's WindowProcessor; 
when a match is found, those matched events are sent to the QuerySelector as expired events.

Note: Despite of the optimizations, a join query is quite expensive when it comes to performance, and this is because
 the WindowProcessor will be locked during the matching process to avoid race conditions and to achieve accuracy while 
 joining; therefore, users should avoid matching huge windows in high volume streams. Based on the scenario, 
 using appropriate window sizes (by time or length) will help to achieve maximum performance.
 
### [StateInputStream](https://github.com/wso2/siddhi/blob/master/modules/siddhi-query-api/src/main/java/org/wso2/siddhi/query/api/execution/query/input/stream/StateInputStream.java) Query Runtime (Pattern & Sequence)

![State Input Stream Query (Pattern & Sequence)](../images/architecture/siddhi-query-state.png "State Input Stream Query (Pattern & Sequence)")


State input stream query runtime is generated for pattern and sequence queries. This consume events from one or more Stream Junctions 
via ProcessStreamReceivers. There will be a ProcessStreamReceiver and a set of basic processors for each condition defined in the query. 
When a ProcessStreamReceiver consume events they update the states with the incoming events that are generated by previous conditions, or 
if its for the first condition in the query then it creates a new state and update that with the incoming event. This is then passed 
to basic processors to perform filter and transformation operations. The states 
that passes the basic processors are consumed by the PostStateProcessor and stored at the PreStateProcessor of the following pattern or 
sequence condition. When the state reaches the final condition's PostStateProcessor 
the output event is generated and emitted by QuerySelector. 

## Siddhi Partition Execution 

![Siddhi Partition](../images/architecture/siddhi-partition.png "Siddhi Partition")

Partition is a wrapper around one or more siddhi queries and inner streams that connect them. 
Partition is implemented in Siddhi as [PartitionRuntime](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/PartitionRuntime.java) 
and each unique partition instances are implemented as [PartitionInstanceRuntime](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/PartitionInstanceRuntime.java)
Each partitioned stream entering the partition
goes through the appropriate [PartitionStreamReceiver](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/PartitionStreamReceiver.java)
and the [PartitionExecutor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/executor/PartitionExecutor.java)
 of stream receiver evaluates   
the incoming events to identify the partition key using the [RangePartitionExecutor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/executor/RangePartitionExecutor.java)
 or using the [ValuePartitionExecutor](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/partition/executor/ValuePartitionExecutor.java)
  and passes 
the events to the QueryRuntimes of the appropriate PartitionInstanceRuntime based on the partition key. 
If a PartitionInstanceRuntime is not available for the given partition key its dynamically generated the appropriate QueryRuntimes and InnerStreamJunctions. 
  
Based on the partition definition the QueryRuntimes in the PartitionInstanceRuntime wire themselves via inner StreamJunctions or using 
StreamJunctions outside the partitions. 
When a partition query consumes a non partitioned global stream, all instances of its QueryRuntime that are part of multiple PartitionInstanceRuntime will 
receive the same event as depicted in the above diagram. 

## Siddhi Event Formats

Siddhi has three event formats. 

- [Event](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/event/Event.java) 

    This is the format exposed to end users when they send events via InputHandler, and consume events via StreamCallback or QueryCallback.
    This comprises an `Object[]` containing all the values in accordance to the corresponding stream. 
     
- [StreamEvent](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/event/stream/StreamEvent.java) (Subtype of [ComplexEvent](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/event/ComplexEvent.java))

    This is used within queries. This contains three `Object[]`s named as beforeWindowData, onAfterWindowData and outputData. Here the 
    outputData contains the values in accordance to the output Stream of the query, and beforeWindowData contains values that are only used 
    in Processors that are executed before the WindowProcessor, the onAfterWindowData will contains values that are only used by WindowProcessor
    and other processors that are after it but not sent out as output. Here the content on the beforeWindowData will be cleared before the 
    event enter into the WindowProcessor to optimize the amount to data that will be stored in in-memory at windows. StreamEvents can also be chained 
    by linking each other via the `next` property in them.  
    
- [StateEvent](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/event/state/StateEvent.java) (Subtype of [ComplexEvent](https://github.com/wso2/siddhi/blob/master/modules/siddhi-core/src/main/java/org/wso2/siddhi/core/event/ComplexEvent.java))

    This is used in Joins, Patterns and Sequences when we need to associate events of multiple different type of Streams, Tables, Windows and Aggregations together.
    This has a collection StreamEvents representing different streams, tables, etc, that are used in the query, and outputData for containing the values that need for the query output. 
    StreamEvents can also be chained by linking each other with the `next` property in them. 
    
**Event Chunks**

Event Chunks provide an easier way of manipulating the chain of StreamEvents and StateEvents, such that they are be easily iterated, inserted and removed. 

## Summery 

This article focuses on describing the architecture of Siddhi, rationalize the architectural decisions made, and
also explains the key features of Siddhi.  
This is possibly a great starting point for new developers to understand Siddhi and to start contributing to it.
