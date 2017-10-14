# Siddhi Architecture 

WSO2 Siddhi is a software library that can run as server by its own, run within WSO2 Stream Processor as a service, and embeddable into any Java based, 
Python based or on an Android application. It provides analytical operators, orchestrate data flows, calculate analytics, and detect
patterns on event data from multiple disparate live data sources. This allows developers to build applications that sense, 
think, and act in real time.

This section illustrate the architecture of Siddhi and take you through its key functionality; We hope that the article 
will help developers to better understand the code, and will help them to better used siddhi, fix bugs and also improve Siddhi. 

## Main Design Decisions

- Siddhi is implemented specifically for event by event processing of real-time streaming data. 
- It provides an intuitive way to expressing stream processing logic and Complex 
Event Processing constructs like Patterns and Sequences with its Siddhi Streaming SQL.  
- It processes all events in-memory and when necessary it also keeps its processing state in-memory. 
- It enforces strict schema for event streams such that it can precompile the queries to support efficient event data retrieval, 
manipulation, optimise performance and to optimise memory consumption.  
- All processing is optimise for memory consumption by only having the absolute necessary information in memory and 
dropping the rest as soon as possible. 
- Have multiple extension points to support diverse set of functionality such as supporting multiple sources, sinks, functions, 
aggregation operations, windows and many more.

## High Level Architecture

![Simple Siddhi Overview](../images/architecture/siddhi-overview-simple.png?raw=true "Simple Siddhi Overview")
 
In very high level Siddhi consumes events from various events source processed them according to the defined Siddhi application 
 and produce results to subscribed event sinks. 
 Siddhi stores its processing events in in-memory table or when configured on external data stores such as `RDBMS`, `MangoDB`, 
 `Hazelcast` in-memory grid, etc. Siddhi also allows applications and users to query Siddhi via its Store Query API to interactively 
 retrieve data from its in-memory and other stores.
 
### Main Modules of Siddhi

Siddhi comprises four main components, they are: 

- **Siddhi Query API** : This let you define execution logic via the Siddhi application and its queries and definitions using POJO classes. 
Internally Siddhi uses these object to understand what user expects Siddhi to do. 

- **Siddhi Query Compiler** : This let you use the Siddhi Streaming SQL to define the Siddhi application and its queries and definitions, 
 and it converts the Streaming SQL to Siddhi Query Objects such that Siddhi to execute them. 
 
- **Siddhi Core** : This is responsible for building the execution runtime and processing events as and when they arrive. 
 
- **Siddhi Annotation** : This is a helping component that lets all extensions to be annotated such that they can be 
pricked by Siddhi Core for processing and help you generate appropriate documentation. 

## Siddhi Component Architecture 

The following diagram gives more detail information on major components of Siddhi and how they are related together. 

![Siddhi Component Architecture](../images/architecture/siddhi-architecture-highlevel.png "Siddhi Component Architecture")
 
Here Siddhi Core module is responsible for maintaining the execution logic, and interacting with the external environment and systems 
 to consuming and publishing events. It uses multiple components to achieve its tasks such as:  
 
- Siddhi Manager : This is one of the critical component inside Siddhi Core that is responsible for managing Siddhi App Runtimes 
  and facilitate their functionality via Siddhi Context with periodic state persistence, statistics reporting and extension loading. 
   
- Siddhi App Runtime : There will be one Siddhi App runtime generated for each Siddhi App deployed by the user. Siddhi Apps runtime
provide an isolated execution environment for all of the queries and execution logic defined in the corresponding Siddhi App.
 These Siddhi App Runtimes based on the logic defied consumes and publishes events from various external system, Java and/or Python programmes. 
  
- Siddhi Context : This is a shared object for all the Siddhi App runtimes within the a Siddhi manager, and it contains references 
to the persistence store for periodic persistence, statistics manager to report performance statistics of Siddhi App Runtimes 
and for loading Siddhi extensions. 

## Siddhi App Creation
 
Execution logic in Siddhi is composed as Siddhi Application and this is passes as a string to 
SiddhiManager to create the SiddhiAppRuntime for execution. 

When Siddhi App is passed to `SiddhiManager.createSiddhiAppRuntime()` internally its processed with the 
SiddhiCompiler. Here SiddhiApp String is converted to SiddhiApp Object model by the SiddhiQLBaseVisitorImpl class. 
The model is then passed to the SiddhiAppParser for the SiddhiAppRuntime creation.


## Siddhi App Execution Flow

Following diagram depicts the execution flow within a Siddhi App Runtime. 

![Execution Flow in Siddhi App](../images/architecture/siddhi-event-flow.png "Execution Flow in Siddhi App")
 
The path events take within the system is coloured in blue. 

Flowing are the components get involve in handling the events. 

- Stream Junction
    
    Responsible for routing events to various components within the Siddhi App Runtime, there will a Stream Junction 
    generated for for each stream defined or inferred in the Siddhi App. This by default uses the incoming event's thread
    for processing subscribed components, but it can also be configured via `@Async` annotation to buffer the events and 
    use a different thread for subsequent execution.
    
- Input Handler 
    
    There will an instance of Input Handler for each Stream Junction, and this is used for pushing `Event` and
     `Event[]`s objects into Stream Junctions from Sources and from Java and Python programmes 
    
- Stream Callback 

    This is responsible for receiving `Event[]`s from Stream Junction and pass them to Sinks to publish to external endpoints 
    or pass them subscribed Java and Python programmes for further processing. 
    
- Queries & Partitions 

    They are the components responsible for processing the events by filtering, transforming, joining, patten matching, 
    etc. They consume events from one or more Stream Junctions, process them and publishes the newly produced events 
     into Stream Junctions based on the defined query or partition. 
     
- Source 
    
    This is responsible for consuming events from external sources on various data formats convert the events into Siddhi event
    and then pass them to corresponding Stream Junction via it's Input Handler. There will be one Source generated 
    for each `@Source` annotation defined on top of the Streams. 
    
- Source Mapper 

    There will be a Source Mapper for each source, responsible for converting the incoming event format into Siddhi Events. 
    Source Mapper type can be configured using the `@Map` annotation used within the `@Source` annotation. When `@Map` annotation 
    is not defined Siddhi uses PassThroughMapper, assuming that the incoming message is already in Siddhi Event format and 
    it does not need any changes.
    
- Sink 

    This is responsible for publish the events to external endpoints by converting the Siddhi Events to various data formats. 
    There will be one Sink generated for each `@Sink` annotation defined on top of the Streams to consume and publish the events arriving 
    on that stream. 
    
- Sink Mapper 

    There will be one Sink Mapper for each Sink inorder to map the Siddhi events to various data formats such that they 
    can be published via Sink. Sink Mapper type can be configured using the `@Map` annotation used within the `@Sink`
    annotation. When `@Map` annotation is not defined Siddhi uses PassThroughMapper, by passing the events as it is 
    any to the Sink.
    
- Table
    
    Table is responsible for storing events, by default Siddhi uses In-Memory Table implementation, and when `@Store` annotation
    is used it load the associated Table implantation based on the defined `store` type. Most table implementations are 
    extended from the AbstractRecordTable abstract class for the easy of development.
    
- Window
    
    Window is responsible for storing and expiring the events based on the given window constrain. Multiple version of windows 
    can be implemented by extending the WindowProcessor abstract class. 
    
- Incremental Aggregation 

    Manges incremental aggregation by letting you obtain aggregates in an incremental manner for a specified set of time periods.
    When defining aggregates incremental aggregation functions can be implemented by extending IncrementalAttributeAggregator. 
     
- Trigger
 
    Trigger triggers events on a given interval to the stream junction with has the same name as of the Trigger.
    
- Query Callback 

    This can be used to get events generated from Queries, this notifies the event occurrence `timestamp`, and classifies 
    the events into `currentEvents`, and `expiredEvents`. 
    

## Siddhi Query Execution 

Siddhi queries can be categorised in to three main types such as single input stream queries which comprises query types filter and windows,
 join input stream queries comprising joins, and state input stream queries comprising pattern and sequences. 

Following section explains the internal of each query type. 

### Single Input Stream Query Runtime (Filter & Windows)

![Single Input Stream Query](../images/architecture/siddhi-query-single.png "Single Input Stream Query (Filter & Window)")
 
Single input stream query runtime is generated for filter and window queries, they consumes events from a Stream Junction 
and convert the events according to the expected output stream format at the ProcessStreamReceiver and by dropping 
all the unrelated incoming stream attributes. Query runtime can also consume events from a Window.

Then the converted events are passed through few Processors such as Filter, StreamProcessor, StreamFunctionProcessor, 
WindowProcessor and QuerySelector. Here StreamProcessor, StreamFunctionProcessor, and WindowProcessor can be extended with 
various stream processing capabilities. The chain of processors are always ended with a QuerySelector and the chain can 
only have at most one WindowProcessor. When query runtime consumes events from a Window its chain of processors cannot 
contain a WindowProcessor.

When it comes to the Filter processor, its implemented with an Expression that returns boolean value. Expressions have a tree structure, and 
they are processed with the Depth First Search Algorithm. To achieve high performance, currently, Siddhi depends on the user
to formulate the lease success case in the leftmost side of the condition, thereby increasing the chances of early false detection.

The condition expression `price >= 100 and ( Symbol == 'IBM' or Symbol == 'MSFT' )` will be represented as below.

![Siddhi Expression](../images/architecture/siddhi-expressions.png "Single Input Query (Filter & Window)")

These Expressions also supports executing user defined functions (UDFs), and they can be implemented by extending the FunctionExecutor class. 

Events after getting processed by each processor they will reach to the QuerySelector for transformation. At the QuerySelector
Events are transformed based on the `select` clause of the query. If there is a `Group By` defined then the GroupByKeyGenerator identifies the 
group by key and then each AttributeProcessor is executed based on the group by key. AttributeProcessors can contain any Expression including 
constant values, variables and user defined functions, at the same time they can also contain AttributeAggregators for 
processing aggregation operations such as `sum`, `count`, etc. Here for each group by key a AttributeAggregator will be generated to 
keep track of the aggregation state and when it becomes obsolete it will be destroyed. Through this operation the event 
will be transformed to the expected output format. 
 
After the event got transformed to the output format it will evaluated against the HavingConditionExecutor if a `having` clause is 
provided, and only the succeeding events will be pushed to OutputRateLimiter.   

At OutputRateLimiter the events output is controlled before sending the events to the stream junction or to the query callback. 
When `output` clause is not defined PassThroughOutputRateLimiter is used by passing all the events without any rate limiting. 

#### Temporal Processing with Windows

The temporal event processing aspect is achieved through Windows and Attribute Aggregations
Siddhi uses four type of events to achieve temporal processing : 

Current Events: This are events newly arriving to a query from streams

Expired Events: These are events that have expired from a window. 

Timer Events: Are events informing query about an update of the execution time. Usually the are generated by Schedulers. 
 
Reset Events: Events that resets states in side Siddhi queries.
 
In Siddhi, when an event comes into a WindowProcessor, 
it will create an appropriate expired event corresponding to the incoming current event with the expiring timestamp and store that 
event in the window. The WindowProcessor will also forward the current event to the next Processor for further processing. 
WindowProcessor using a scheduler or otherwise decides when to emit the events it has in memory and during that time it emits 
the events as expired events, at times if its emitting all the events at ones it emits a single reset event interred of 
sending one expired event for each event it has stored. From a window for each current event it has emitted it is essential 
to emmit the same events as expired events or emit a reset event.  

This is vital in Siddhi because Siddhi relies on these events to calculate the Aggregations at the QuerySelector. 
In QuerySelector, the arrived current events increase the aggregation values, expired events decrease the values, and 
reset events reset the aggregation calculation.

For example if we take the sliding time window, when a current event arrives it creates the corresponding expired event, adds that to 
the window, add an entry to the Scheduler to notify when the event need to be expired by sending a timer event at that time, and 
finally send the current event to the next processor for processing.  
When the window get an indication the the expected expiry time has come for the oldest event in the window via a 
a timer event from the scheduler or other means it removes the expired event from the window and pass that to the next processor. 

![Siddhi Time Window](../images/architecture/siddhi-time-window.png "Siddhi Time Window")

### Join Input Stream Query Runtime (Join)
 
![Join Input Stream Query](../images/architecture/siddhi-query-join.png "Join Input Stream Query")
 
Join input stream query runtime is generated for join queries. This can consume events from two Stream Junction as perform join depicted above, 
or consume from the one Stream Junction and do join against the same stream or it can also consume from one Stream Junction can join against with 
a table, window or incremental aggregation. When join is performed with table, window or incremental aggregation
the WindowProcessor will be replaced with the table, window or incremental aggregation and on that side no  
basic processors will be used. 

The joining operation will be triggered by the events arriving from the Stream Junction.
Here when an event from one stream reaches the pre JoinProcessor, it is matched against all the available events of the other stream's WindowProcessor. 
When a match is found, those matched events are then sent to the QuerySelector as the current events; at the same time, 
the original event will be added to the WindowProcessor and it will remain there until it expires. Similarly, when an 
event expires from its WindowProcessor, it is matched against all the available events of the other stream's Window Processor; 
when a match is found, those matched events are sent to the QuerySelector as expired events.

Note: Despite of the optimizations, a join query is quite expensive when it comes to performance, and this is because
 the WindowProcessor will be locked during the matching process to avoid race conditions and to achieve accuracy in 
 joining process; therefore, users should avoid matching huge windows in high volume streams. Based on the scenario, 
 using appropriate window sizes (by time or length) will help to achieve maximum performance.
 
### State Input Stream Query Runtime (Pattern & Sequences)

![State Input Stream Query (Pattern & Sequence)](../images/architecture/siddhi-query-state.png "State Input Stream Query (Pattern & Sequence)")


State input stream query runtime is generated for pattern and sequence queries. This consume events from one or more Stream Junctions 
via ProcessStreamReceivers. There will be a ProcessStreamReceiver and a set of basic processors for each condition defined in the query. 
When ProcessStreamReceiver consume events they update the existing states generated by previous conditions with the incoming events or 
if its for the first condition in the query then it creates a new state and update that with the incoming event. This is then passed 
to basic processors for to perform filter and transformation operations, the states 
that passes the basic processors are consumed by the PostStateProcessor and stored at the PreStateProcessor of the following pattern or 
sequence condition evaluations. When the state reaches the final condition's PostStateProcessor 
the output event is generated by QuerySelector and emitted as output. 

## Siddhi Partition Execution 

![Siddhi Partition](../images/architecture/siddhi-partition.png "Siddhi Partition")

Partition is a wrapper around one or more siddhi queries and inner streams that connect them. 
Partition is implemented in Siddhi as PartitionRuntime, each partitioned stream entering the partition
goes through the appropriate PartitionStreamReceiver the PartitionExecutor of receiver evaluates   
the incoming events to identify the partition key using RangePartitionExecutor or by ValuePartitionExecutor and pass 
the events to the QueryRuntimes of the appropriate PartitionInstanceRuntime based on the partition key. 
If a PartitionInstanceRuntime is not available for the partition key its dynamically generated with QueryRuntimes and InnerStreamJunctions. 
  
Based on the partition definition the QueryRuntimes in the PartitionInstanceRuntime wire themselves via InnerStreamJunctions or using 
StreamJunctions outside the partitions. 
When a partition query consumes a non partitioned global stream, all instances of its QueryRuntime that are part of multiple PartitionInstanceRuntime will 
receive the same event as depicted in the above diagram. 

## Siddhi Event Formats

Siddhi has three event formats. 

- Event 

    This is the format exposed to end users when they send events via InputHandler and Consume events via StreamCallback or QueryCallback.
    This comprises an `Object[]` containing all the values in accordance to the corresponding Streams. 
     
- StreamEvent (Subtype of ComplexEvent)

    This is used within queries. This contains three `Object[]`s named as beforeWindowData, onAfterWindowData and outputData. Here the 
    outputData contains the values in accordance to the output Stream of the query, and beforeWindowData contains values that are only used 
    in Processors that are executed before the WindowProcessor, the onAfterWindowData will be contains values that are only used by WindowProcessor
    and other processors after that and that are not sent out as output. Here the content on the beforeWindowData will be cleared before the 
    event enter into the WindowProcessor to optimize the amount to data that will be stored in-memory at windows. StreamEvents can also be chained 
    by linking each other via the `next` property in them.  
    
- StateEvent (Subtype of ComplexEvent)

    This is used in Joins, Patterns and Sequences to when we need to associate with multiple different type of Streams, Tables, Windows and Aggregations.
    This has a collection StreamEvents representing different streams used in the query and outputData for containing the values that need to be outputted 
    according to the output Stream of the query. StreamEvents can also be chained by linking each other with the `next` property in them. 
    
**Event Chunks**

Event Chunks provide an easier way of manipulating the chain of StreamEvents and StateEvents, such that they are be easily iterated, removed 
and letting new events to be inserted. 

## Summery 

This article focuses on describing the architecture of Siddhi and rationale for the architectural decisions made, this
also explains the key features of Siddhi.  
This is possibly a great starting point for new developers to understand Siddhi and to start contributing towards it.
