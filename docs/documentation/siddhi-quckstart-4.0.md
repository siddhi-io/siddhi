Siddhi Application can be written in a Streaming SQL language to process event streams and identify complex event occurrences.

Siddhi Application can run 

* By embedding Siddhi as a Java library in your project
* Or with WSO2 Stream Processor

## A Beginner's Guide to Siddhi - Complex Event Processor
[Siddhi](https://github.com/wso2/siddhi) is a 100% open source Complex Event Processor(CEP) offered by WSO2, Inc. 
It was initially started as a research project at University of Moratuwa, Sri Lanka. The main application of Siddhi 
is processing data streams in real time. It is written in Java and thoroughly optimized for high performance.

Siddhi is used by many companies including Uber, and Cleveland Clinic. In fact **Uber processes more than 20 billion 
events per day using Siddhi**. Before diving into how to use Siddhi we will first discuss about CEP.

#### 1. Complex Event Processing (CEP)
First we will look at what an event is through an example. **If we take the transactions through an ATM as a data 
stream, one withdrawal from it would be an event**. This event will contain data about amount, time, account number etc. 
Many such transactions will make up a stream.

[Gartner’s IT Glossary](https://www.gartner.com/it-glossary/complex-event-processing) defines CEP in the following way,

_"CEP is a kind of computing in which **incoming data about events is distilled into more useful, higher level “complex” 
event data** that provides insight into what is happening._

_**CEP is event-driven** because the computation is triggered by the receipt of event data. CEP is used for highly 
demanding, continuous-intelligence applications that enhance situation awareness and support real-time decisions."_

Basically a CEP receives data event-by-event and processes them in real time to give meaningful information.

#### Overview of Siddhi
![](../images/siddhi-overview.png?raw=true "Overview")

As you can see above Siddhi can,
* accept event inputs from many different types of sources
* process it to generate insights
* publish them to many types of sinks.

To use Siddhi a user will write **a Siddhi query** which will be discussed in the 4th section. After writing a Siddhi 
query and starting **a Siddhi application**, it will

1. take data event-by-event
2. process the input data in each event
3. add the high level data generated to each event
4. send them to the output stream.

#### Using Siddhi for the First Time
We will use WSO2 Stream Processor(will be addressed as _‘SP_’ hereafter) — a server version of Siddhi that has 
sophisticated editor with GUI (called _“Stream Processor Studio”_)where you can write your query and simulate events in 
a data stream.

**Step 1** — Install [Oracle Java SE Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8\
**Step 2** — Set the JAVA_HOME environment variable\
**Step 3** — Download the [WSO2 Stream Processor](https://github.com/wso2/product-sp/releases)\
**Step 4** — Extract the downloaded zip and navigate to <SP_HOME>/bin (SP_HOME refers to the extracted folder)\
**Step 5** — Issue the following command in command prompt(Windows)/terminal(Linux)\
```
For Windows: editor.bat
For Linux: sh editor.sh
```
\
After successfully starting the SP the terminal should look like this in Linux,\
\
![](../images/after-starting-sp.png?raw=true "Terminal after starting WSO2 Stream Processor Text Editor")

Refer to the SP [Quick Start Guide](https://docs.wso2.com/display/SP400/Quick+Start+Guide) for more information. 
After starting the WSO2 Stream Processor access the Text 
Editor by visiting the following link in your browser

```
http://localhost:9090/editor
```
\
After successfully starting the SP and accessing the editor it should display the following,\
\
![](../images/sp-studio.png?raw=true "Stream Processor Studio")

#### Siddhi ‘Hello World!’ — Your First Siddhi Query
Siddhi QL is a rich, compact, easy-to-learn SQL-like language. **We will first learn how to add data from events 
one-by-one** and output total value with each event. Siddhi has lot of in-built functions and extensions available for 
complex analysis but we will start with a simple one. You can find more information about the grammar and the functions 
in [Siddhi Query Guide](https://wso2.github.io/siddhi/documentation/siddhi-4.0/).\
\
We will **consider a scenario where we are loading cargo boxes into a ship**. We need to keep track of the total weight of 
the cargo added. **Measuring the weight of a cargo box when loading is treated as an event**.
\
\
![](../images/loading-ship.jpg?raw=true "Loading Cargo on Ship")
\
\
We can write a Siddhi program for the above scenario which will have 3 parts.\
\
**Part 1 — Giving our Siddhi application a suitable name.** This is a Siddhi routine. Here we will name our app as 
_“HelloWorlApp”_

```
@App:name("HelloWorldApp")
```
\
**Part 2 — Defining the input stream.** This will have a name for the input stream, name and type of the data coming 
with each event. Here,\
* Name of the input stream — _“CargoStream”_
* name of the data in each event — _“weight”_
* Type of the data _“weight”_ — int

```
define stream CargoStream (weight int);
```
\
**Part 3 — The actual Siddhi query.** Here we will specify 4 things.\
1. A name for the query — _“HelloWorldQuery”_
2. which stream should be taken into processing — _“CargoStream”_
3. what data we require in the output stream — _“weight”_, _“totalWeight”_
4. which stream should be populated with the output — _“OutputStream”_
```
@info(name='HelloWorldQuery')
from CargoStream
select weight, sum(weight) as totalWeight
insert into OutputStream;
```


## Using Siddhi as an Java Library

To use Siddhi as a library by embedding it in a Java project, follow the steps below:

#### Step 1: Creating a Java Project

* Create a Java project using Maven and include the following dependencies in its `pom.xml` file.

```xml
   <dependency>
     <groupId>org.wso2.siddhi</groupId>
     <artifactId>siddhi-core</artifactId>
     <version>4.x.x</version>
   </dependency>
   <dependency>
     <groupId>org.wso2.siddhi</groupId>
     <artifactId>siddhi-query-api</artifactId>
     <version>4.x.x</version>
   </dependency>
   <dependency>
     <groupId>org.wso2.siddhi</groupId>
     <artifactId>siddhi-query-compiler</artifactId>
     <version>4.x.x</version>
   </dependency>
   <dependency>
     <groupId>org.wso2.siddhi</groupId>
     <artifactId>siddhi-annotations</artifactId>
     <version>4.x.x</version>
   </dependency>   
```
  
Add the following repository configuration to the same file.
  
```xml
   <repositories>
     <repository>
         <id>wso2.releases</id>
         <name>WSO2 Repository</name>
         <url>http://maven.wso2.org/nexus/content/repositories/releases/</url>
         <releases>
             <enabled>true</enabled>
             <updatePolicy>daily</updatePolicy>
             <checksumPolicy>ignore</checksumPolicy>
         </releases>
     </repository>
   </repositories>
```
  
  **Note**: You can create the Java project using any method you prefer. The required dependencies can be downloaded from [here](http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/).
* Create a new Java class in the Maven project.

#### Step 2: Creating Siddhi Application
A Siddhi application is a self contained execution entity that defines how data is captured, processed and sent out.  

* Create a Siddhi Application by defining a stream definition E.g.`StockEventStream` defining the format of the incoming
 events, and by defining a Siddhi query as follows.
```java
  String siddhiApp = "define stream StockEventStream (symbol string, price float, volume long); " + 
                     " " +
                     "@info(name = 'query1') " +
                     "from StockEventStream#window.time(5 sec)  " +
                     "select symbol, sum(price) as price, sum(volume) as volume " +
                     "group by symbol " +
                     "insert into AggregateStockStream ;";
```
  This Siddhi query groups the events by symbol and calculates aggregates such as the sum for price and sum of volume 
  for the last 5 seconds time window. Then it inserts the results into a stream named `AggregateStockStream`. 
  
#### Step 3: Creating Siddhi Application Runtime
This step involves creating a runtime representation of a Siddhi application.
```java
SiddhiManager siddhiManager = new SiddhiManager();
SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
```
The Siddhi Manager parses the Siddhi application and provides you with an Siddhi application runtime. 
This Siddhi application runtime can be used to add callbacks and input handlers such that you can 
programmatically invoke the Siddhi application.

#### Step 4: Registering a Callback
You can register a callback to the Siddhi application runtime in order to receive the results once the events are processed. There are two types of callbacks:

+ **Query callback**: This subscribes to a query.
+ **Stream callback**: This subscribes to an event stream.
In this example, a Stream callback is added to the `AggregateStockStream` to capture the processed events.

```java
siddhiAppRuntime.addCallback("AggregateStockStream", new StreamCallback() {
           @Override
           public void receive(Event[] events) {
               EventPrinter.print(events);
           }
       });
```
Here, once the results are generated they are sent to the receive method of this callback. An event printer is added 
inside this callback to print the incoming events for demonstration purposes.

#### Step 5: Sending Events
In order to programmatically send events from the stream you need to obtain it's an input handler as follows:
```java
InputHandler inputHandler = siddhiAppRuntime.getInputHandler("StockEventStream");
```
Use the following code to start the Siddhi application runtime, send events and to shutdown Siddhi:
```java
//Start SiddhiApp runtime
siddhiAppRuntime.start();

//Sending events to Siddhi
inputHandler.send(new Object[]{"IBM", 100f, 100L});
Thread.sleep(1000);
inputHandler.send(new Object[]{"IBM", 200f, 300L});
inputHandler.send(new Object[]{"WSO2", 60f, 200L});
Thread.sleep(1000);
inputHandler.send(new Object[]{"WSO2", 70f, 400L});
inputHandler.send(new Object[]{"GOOG", 50f, 30L});
Thread.sleep(1000);
inputHandler.send(new Object[]{"IBM", 200f, 400L});
Thread.sleep(2000);
inputHandler.send(new Object[]{"WSO2", 70f, 50L});
Thread.sleep(2000);
inputHandler.send(new Object[]{"WSO2", 80f, 400L});
inputHandler.send(new Object[]{"GOOG", 60f, 30L});
Thread.sleep(1000);
 
//Shutdown SiddhiApp runtime
siddhiAppRuntime.shutdown();

//Shutdown Siddhi
siddhiManager.shutdown();
```
When the events are sent, you can see the output logged by the event printer.

Find the executable Java code of this example [here](https://github.com/wso2/siddhi/tree/master/modules/siddhi-samples/quick-start-samples/src/main/java/org/wso2/siddhi/sample/TimeWindowSample.java) 

For more code examples, see [quick start samples for Siddhi](https://github.com/wso2/siddhi/tree/master/modules/siddhi-samples/quick-start-samples).