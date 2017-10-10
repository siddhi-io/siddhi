# Siddhi Architecture 

WSO2 Siddhi is a software that can run as server by it own or within WSO2 Stream Processor, embalmed in to Java based 
systems and Android applications to provide analytical operators, orchestrate data flow, calculate analytics, and detect
patterns on event data from multiple, disparate live data sources to allow developers to build applications that sense, 
think, and act in real time.

This section illustrate the architecture of Siddhi and take you through its key functionality; We hope that the article 
will help developers to better understand the code, and will help them to better used siddhi, fix bugs and also improve Siddhi. 

## Main design decisions behind Siddhi

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

## High level architecture

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

###Siddhi Component Architecture 

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

###Siddhi App Execution Flow

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
   Source type can be configured using the `@Map` annotation used within the `@Source` annotation. When `@Map` annotation 
   is not defined Siddhi uses PassThroughMapper, assuming that the incoming message is already in Siddhi Event format and 
    it does not need any changes.
    
- Sink 

    This is responsible for publish the events to external endpoints by converting the Siddhi Events to various data formats. 
    There will be one Sink generated for each `@Sink` annotation defined on top of the Streams to consume and publish the events arriving 
    on that stream. 
    
    
- Table
- Window
- Trigger 
- Aggregation 

- Query
    - filter
    - window
    - join 
    - pattern/sequence 
    
- Partition
    - inner stream
