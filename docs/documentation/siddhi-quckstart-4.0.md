Siddhi Application can be written in a Streaming SQL language to process event streams and identify complex event occurrences.

Siddhi Application can run 

* By embedding Siddhi as a Java library in your project
* Or with WSO2 Stream Processor

## Creating Siddhi Applications via WSO2 Stream Processor

For instructions to write and deploy a Siddhi application in WSO2 Stream Processor, see the [WSO2 SP Quick Start Guide](https://docs.wso2.com/display/SP400/Quick+Start+Guide).

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