# Quick Start Guide to Siddhi 

[Siddhi](https://github.com/wso2/siddhi) is a 100% open source Java library that is thoroughly optimized for high performance. 
It performs **_Stream Processing_** and **_Complex Event Processing_** on real time data streams. 

Siddhi is used by many companies including Uber and eBay (via Apache Eagle). **Uber processes more than 20 billion 
events per day using Siddhi** for fraud analytics, and Siddhi is used in Apache Eagle as a policy enforcement engine.

This quick start guide contains the following six sections:

1. Stream Processing and Complex Event Processing Overview - about the **domain** of Siddhi
2. An Overview of Siddhi - Explaining the basic **architecture**
3. Using Siddhi for the First Time - how to **set up** the software
4. Siddhi ‘Hello World!’ - Your **First Siddhi Application**
5. Simulating Events - **Testing** your query with simulated events
6. A Bit of Stream Processing - **temporal event processing**

## 1. Stream Processing and Complex Event Processing (CEP) Overview

Before diving into using Siddhi, let's first discuss Stream Processing 
and Complex Event Processing in brief so that we can identify the use-cases where Siddhi can be used.

First let's understand what an event is through an example. **If we consider the transactions carried out via an ATM as a data 
stream, one withdrawal from it can be considered an event**. This event contains data about the amount, time, account number etc. 
Many such transactions form a stream.

![](../images/quickstart/event-stream.png?raw=true "Event Stream")

[Forrester](https://reprints.forrester.com/#/assets/2/202/'RES136545'/reports) defines Streaming Analytics as:

> Software that provides analytical operators to **orchestrate data flow**, **calculate analytics**, and **detect patterns** on 
event data **from multiple, disparate live data sources** to allow developers to build applications that **sense, think, 
and act in real time**.

[Gartner’s IT Glossary](https://www.gartner.com/it-glossary/complex-event-processing) defines CEP as follows:

>"CEP is a kind of computing in which **incoming data about events is distilled into more useful, higher level “complex” 
event data** that provides insight into what is happening."
>
>"**CEP is event-driven** because the computation is triggered by the receipt of event data. CEP is used for highly 
demanding, continuous-intelligence applications that enhance situation awareness and support real-time decisions."

![](../images/quickstart/siddhi-basic.png?raw=true "Siddhi Basic Representation")

Basically, Siddhi receives data event-by-event and processes them in real time to produce meaningful information.

Siddhi Can be used in the following use-cases:

* Fraud Analytics 
* Monitoring 
* Anomaly Detection
* Sentiment Analysis
* Processing Customer Behaviour
* .. etc

## 2. Overview of Siddhi

![](../images/siddhi-overview.png?raw=true "Overview")

As indicated above, Siddhi can:

+ accept event inputs from many different types of sources
+ process them to generate insights
+ publish them to many types of sinks.

To use Siddhi, you need to write the processing logic as a **Siddhi Application** in the **Siddhi Streaming SQL** 
language which is discussed in the 4th section. After writing and starting **a Siddhi application**, it:

1. Takes data one-by-one as events
2. Processes the data in each event
3. Generates new high level events based on the processing done so far
4. Sends newly generated events as the output to streams.

## 3. Using Siddhi for the First Time

In this section, we will be using the WSO2 Stream Processor(referred to as SP in the rest of this guide) — a server version of Siddhi that has a
sophisticated editor with a GUI (referred to as _**“Stream Processor Studio”**_) where you can write your query and simulate events
as a data stream.

**Step 1** — Install 
[Oracle Java SE Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8. <br>
**Step 2** — [Set the JAVA_HOME](https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/) environment 
variable. <br>
**Step 3** — Download the latest [WSO2 Stream Processor](http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17). <br>
**Step 4** — Extract the downloaded zip and navigate to `<SP_HOME>/bin`. <br> (`SP_HOME` refers to the extracted folder) <br>
**Step 5** — Issue the following command in the command prompt (Windows) / terminal (Linux) 

```
For Windows: editor.bat
For Linux: ./editor.sh
```
For more details about WSO2 Stream Processor, see its [Quick Start Guide](https://docs.wso2.com/display/SP400/Quick+Start+Guide).

After successfully starting the Stream Processor Studio, the terminal in Linux should look like as shown below:

![](../images/quickstart/after-starting-sp.png?raw=true "Terminal after starting WSO2 Stream Processor Text Editor")

After starting the WSO2 Stream Processor, access the Stream Processor Studio by visiting the following link in your browser.
```
http://localhost:9390/editor
```
This takes you to the Stream Processor Studio landing page.

![](../images/quickstart/sp-studio.png?raw=true "Stream Processor Studio")

## 4. Siddhi ‘Hello World!’ — Your First Siddhi Application

Siddhi Streaming SQL is a rich, compact, easy-to-learn SQL-like language. **Let's first learn how to find the total** of values 
coming into a data stream and output the current running total value with each event. Siddhi has lot of in-built functions and extensions 
available for complex analysis, but to get started, let's use a simple one. You can find more information about the Siddhi grammar 
and its functions in the [Siddhi Query Guide](https://wso2.github.io/siddhi/documentation/siddhi-4.0/).

Let's **consider a scenario where we are loading cargo boxes into a ship**. We need to keep track of the total 
weight of the cargo added. **Measuring the weight of a cargo box when loading is considered an event**.

![](../images/quickstart/loading-ship.jpeg?raw=true "Loading Cargo on Ship")

We can write a Siddhi program for the above scenario which has **4 parts**.

**Part 1 — Giving our Siddhi application a suitable name.** This is a Siddhi routine. In this example, let's name our application as 
_“HelloWorldApp”_

```
@App:name("HelloWorldApp")
```
**Part 2 — Defining the input stream.** The stream needs to have a name and a schema defining the data that each incoming event should contain.
The event data attributes are expressed as name and type pairs. In this example:

* The name of the input stream — _“CargoStream”_ <br>
This contains only one data attribute:
* The name of the data in each event — _“weight”_
* Type of the data _“weight”_ — int

```
define stream CargoStream (weight int);
```
**Part 3 - Defining the output stream.** This has the same info as the previous definition with an additional 
_totalWeight_ attribute that contains the total weight calculated so far. Here, we need to add a 
_"sink"_  to log the `OutputStream` so that we can observe the output values. (**Sink is the Siddhi way to publish 
streams to external systems.** This particular `log` type sink just logs the stream events. To learn more about sinks, see 
[sink](https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink))
```
@sink(type='log', prefix='LOGGER')
define stream OutputStream(weight int, totalWeight long);
```
**Part 4 — The actual Siddhi query.** Here we need to specify the following:

1. A name for the query — _“HelloWorldQuery”_
2. Which stream should be taken into processing — _“CargoStream”_
3. What data we require in the output stream — _“weight”_, _“totalWeight”_
4. How the output should be calculated - by calculating the *sum* of the the *weight*s  
5. Which stream should be populated with the output — _“OutputStream”_

```
@info(name='HelloWorldQuery')
from CargoStream
select weight, sum(weight) as totalWeight
insert into OutputStream;
```

![](../images/quickstart/hello-query.png?raw=true "Hello World in Stream Processor Studio")

## 5. Simulating Events

The Stream Processor Studio has in-built support to simulate events. You can do it via the _“Event Simulator”_ 
panel at the left of the Stream Processor Studio. You should save your _HelloWorldApp_ by browsing to **File** -> 
**Save** before you run event simulation. Then click  **Event Simulator** and configure it as shown below.

![](../images/quickstart/event-simulation.png?raw=true "Simulating Events in Stream Processor Studio")

**Step 1 — Configurations:**

* Siddhi App Name — _“HelloWorldApp”_
* Stream Name — _“CargoStream”_
* Timestamp — (Leave it blank)
* weight — 2 (or some integer)

**Step 2 — Click “Run” mode and then click “Start”**. This starts the Siddhi Application. 
If the Siddhi application is successfully started, the following message is printed in the Stream Processor Studio console:</br>
_“HelloWorldApp.siddhi Started Successfully!”_ 

**Step 3 — Click “Send” and observe the terminal** where you started WSO2 Stream Processor Studio. 
You can see a log that contains _“outputData=[2, 2]”_. Click **Send** again and observe a log with 
_“outputData=[2, 4]”_. You can change the value of the weight and send it to see how the sum of the weight is updated.

![](../images/quickstart/log.png?raw=true "Terminal after sending 2 twice")

Bravo! You have successfully completed creating Siddhi Hello World! 

## 6. A Bit of Stream Processing - **temporal event processing**

This section demonstrates how to carry out **temporal window processing** with Siddhi.

Up to this point, we have been carrying out the processing by having only the running sum value in-memory. 
No events were stored during this process. 

[Window processing](https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window) 
is a method that allows us to store some events in-memory for a given period so that we can perform operations 
such as calculating the average, maximum, etc values within them.

Let's imagine that when we are loading cargo boxes into the ship **we need to keep track of the average weight of 
the recently loaded boxes** so that we can balance the weight across the ship. 
For this purpose, let's try to find the **average weight of last three boxes** of each event.

![](../images/quickstart/siddhi-windows.png?raw=true "Terminal after sending 2 twice")

For window processing, we need to modify our query as follows:
```
@info(name='HelloWorldQuery') 
from CargoStream#window.length(3)
select weight, sum(weight) as totalWeight, avg(weight) as averageWeight
insert into OutputStream;
```

* `from CargoStream#window.length(3)` - Here, we are specifying that the last 3 events should be kept in memory for processing.
* `avg(weight) as averageWeight` - Here, we are calculating the average of events stored in the window and producing the 
average value as _"averageWeight"_ (Note: Now the `sum` also calculates the `totalWeight` based on the last three events).

We also need to modify the _"OutputStream"_ definition to accommodate the new _"averageWeight"_.

```
define stream OutputStream(weight int, totalWeight long, averageWeight double);
```

The updated Siddhi Application should look as shown below:

![](../images/quickstart/window-processing-app.png?raw=true "Window Processing with Siddhi")

Now you can send events using the Event Simulator and observe the log to see the sum and average of the weights of the last three 
cargo events.

It is also notable that the defined `length window` only keeps 3 events in-memory. When the 4th event arrives, the 
first event in the window is removed from memory. This ensures that the memory usage does not grow beyond a specific limit. There are also other 
implementations done in Siddhi  to reduce the memory consumption. For more information, see [Siddhi Architecture](https://wso2.github.io/siddhi/documentation/siddhi-architecture/).  

To learn more about the Siddhi functionality, see [Siddhi Query Guide](https://wso2.github.io/siddhi/documentation/siddhi-4.0/).

Feel free to try out Siddhi and event simulation to understand Siddhi better.

If you have questions please post them to the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a> with <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag.
