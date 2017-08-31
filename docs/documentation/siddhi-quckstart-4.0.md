## Siddhi Quick Start Guide

Siddhi Query Language (SiddhiQL) is designed to process event streams to identify complex event occurrences.

The Siddhi Query Language can be used as a library by embedding it in a Java project, or you can use it with WSO2 Stream Processor which allows you to write a Siddhi Application using the WSO2 SP Editor and then deploy that application to the WSO2 SP server.

For instructions to write and deploy a Siddhi application in WSO2 SP, see the [WSO2 SP Quick Start Guide](https://docs.wso2.com/display/SP400/Quick+Start+Guide)

To use Siddhi as a library by embedding it in a Java project, follow the steps below:

## Step 1: Creating a Java Project

* Create a Java project using Maven and include the following dependencies in its pom.xml file.
  ```xml
  <dependency>
     <groupId>org.wso2.siddhi</groupId>
     <artifactId>siddhi-core</artifactId>
     <version>3.0.2</version>
  </dependency>
  <dependency>
     <groupId>org.wso2.siddhi</groupId>
     <artifactId>siddhi-query-api</artifactId>
     <version>3.0.2</version>
  </dependency>
  <dependency>
              <groupId>org.wso2.siddhi</groupId>
              <artifactId>siddhi-query-compiler</artifactId>
              <version>3.0.2</version>
  </dependency>
  ```
  Add the following repository configuration to the same file.
  ```xml
  <repositories>
          <repository>
              <id>wso2.releases</id>
              <name>WSO2 internal Repository</name>
              <url>http://maven.wso2.org/nexus/content/repositories/releases/</url>
              <releases>
                  <enabled>true</enabled>
                  <updatePolicy>daily</updatePolicy>
                  <checksumPolicy>ignore</checksumPolicy>
              </releases>
          </repository>
  </repositories>
  ```
  **Note**: You can create the Java project using any method you prefer. The required dependencies can be downloaded from[here](http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/).
* Create a new Java class in the Maven project.
* Define a stream definition as follows. The stream definition defines the format of the incoming events.
  ```java
  String definition = "@config(async = 'true') define stream cseEventStream (symbol string, price float, volume long);";
  ```
* Define a Siddhi query as follows.
  ```java
  String query = "@info(name = 'query1') from cseEventStream#window.timeBatch(500)  select symbol, sum(price) as price, sum(volume) as volume group by symbol insert into outputStream ;";
  ```
  This Siddhi query stores incoming events for 500 milliseconds, groups them by symbol and calculates the sum for price and volume. Then it inserts the results into a stream named `outputStream`.
  
## Step 2: Creating an Execution Plan Runtime
An execution plan is a self contained, valid set of stream definitions and queries. This step involves creating a runtime representation of an execution plan by combining the stream definition and the Siddhi query you created in Step 1.
```java

SiddhiManager siddhiManager = new SiddhiManager();
 
ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(definition + query);
```
In the above example, `definition + query` forms the execution plan.  The Siddhi Manager parses the execution plan and provides you with an execution plan runtime. This execution plan runtime is used to add callbacks and input handlers to the execution plan.

## Step 3: Registering a Callback
You can register a callback to the execution plan runtime in order to receive the results once the events are processed. There are two types of callbacks:

+ **Query callback**: This subscribes to a query.
+ **Stream callback**: This subscribes to an event stream.
In this example, a query callback is added because the Maven project has only one query.

```java
executionPlanRuntime.addCallback("query1", new QueryCallback() {
	  @Override
 	  public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
       EventPrinter.print(timeStamp, inEvents, removeEvents);
	  }
});
```
Here, a new query callback is added to a query named query1. Once the results are generated, they are sent to the receive method of this callback. An event printer is added inside this callback to print the incoming events for demonstration purposes.

## Step 4: Sending Events
In order to send events from the event stream to the query, you need to obtain an input handler as follows:
```java
InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
```
Use the following code to start the execution plan runtime and send events:
```java
executionPlanRuntime.start();
 
inputHandler.send(new Object[]{"ABC", 700f, 100l});
inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
inputHandler.send(new Object[]{"DEF", 700f, 100l});
inputHandler.send(new Object[]{"ABC", 700f, 100l});
inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
inputHandler.send(new Object[]{"DEF", 700f, 100l});
inputHandler.send(new Object[]{"ABC", 700f, 100l});
inputHandler.send(new Object[]{"WSO2", 60.5f, 200l});
inputHandler.send(new Object[]{"DEF", 700f, 100l});
 
executionPlanRuntime.shutdown();
```
When the events are sent, they are printed by the event printer.

For code examples, see [quick start samples for Siddhi] (https://github.com/wso2/siddhi/tree/master/modules/siddhi-samples/quick-start-samples).