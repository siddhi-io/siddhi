# Siddhi 5.x Quick Start Guide

Siddhi is a 100% open source Streaming engine that listens to events from various data streams, detects complex conditions described via a Streaming SQL language, and triggers actions. It's highly optimized for performance and performs stream processing, complex event processing, streaming data integration, and adaptive intelligence for mission-critical systems.

Siddhi is used by many companies including Uber, eBay, PayPal (via Apache Eagle), here **Uber processed more than 20 billion events per day using Siddhi** for their fraud analytics use cases. Siddhi is also used in various analytics and integration platforms such as Apache Eagle as a policy enforcement engine, WSO2 API Manager as analytics and throttling engine, WSO2 Identity Server as an adaptive authorization engine.

This quick start guide contains the following six sections:

1. **Domain** of Siddhi
2. Overview of Siddhi **architecture**
3. Using Siddhi for the first time
4. Siddhi ‘Hello World!’
5. Testing Siddhi Application
6. A bit of Stream Processing

## 1. Domain of Siddhi

Siddhi is an event driven system where all the data it consumes, processes and sends are modeled as events. Therefore Siddhi can play a vital part in any system built using event-driven architecture.   

As Siddhi works with events first let's understand what an event is through an example. **If we consider the transactions carried out via an ATM as a data stream, one withdrawal from it can be considered an event**. This event contains data about the amount, time, account number etc. Many such transactions form a stream.
![](../../images/quickstart/event-stream.png?raw=true "Event Stream")
Siddhi provides following functionalities,

* Streaming data analytics<br/>
  [Forrester](https://reprints.forrester.com/#/assets/2/202/'RES136545'/reports) defines Streaming Analytics as:<br/>
  > Software that provides analytical operators to **orchestrate data flow**, **calculate analytics**, and **detect patterns** on event data **from multiple, disparate live data sources** to allow developers to build applications that **sense, think, and act in real time**.

* Complex Event Processing (CEP)<br/>
  [Gartner’s IT Glossary](https://www.gartner.com/it-glossary/complex-event-processing) defines CEP as follows:<br/>
  >"CEP is a kind of computing in which **incoming data about events is distilled into more useful, higher level “complex”event data** that provides insight into what is happening."
  >
  >"**CEP is event-driven** because the computation is triggered by the receipt of event data. CEP is used for highly demanding, continuous-intelligence applications that enhance situation awareness and support real-time decisions."

* Streaming data integration<br/>
  > Streaming data integration is way of integrating several systems by continuously moving data in realtime from one system to another, while processing, correlating, and analyzing the data in-memory.

* Adaptive Intelligence
* Decision Engine


Basically, Siddhi receives data event-by-event and processes them in real time to produce meaningful information.

![](../../images/quickstart/siddhi-basic.png?raw=true "Siddhi Basic Representation")

Using the above Siddhi can be used to solve may use-cases as follows:

* Fraud Analytics
* Monitoring
* Anomaly Detection
* Sentiment Analysis
* Processing Customer Behavior
* .. etc

## 2. Overview of Siddhi architecture

![](../../images/siddhi-overview.png?raw=true "Overview")

As indicated above, Siddhi can:

+ Accept event inputs from many different types of sources
+ Process them to generate insights
+ Publish them to many types of sinks.

To use Siddhi, you need to write the processing logic as a **Siddhi Application** in the **Siddhi Streaming SQL**
language which is discussed in the [section 4](#4-siddhi-hello-world-your-first-siddhi-application). When the **Siddhi application** is started, it:

1. Consumes data one-by-one as events
2. Processes the data in each event
3. Generates new high level events based on the processing done so far
4. Sends newly generated events as the output to streams.

## 3. Using Siddhi for the first time

In this section, we will be using the Tooling distribution — a server version of Siddhi that has a sophisticated editor with a GUI(referred to as _**“Siddhi Editor”**_) where you can write your query and simulate events as a data stream.

**Step 1** — Install
[Oracle Java SE Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8. <br>
**Step 2** — [Set the JAVA_HOME](https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/) environment
variable. <br>
**Step 3** — Download the latest tooling distribution from [here](https://github.com/siddhi-io/distribution/releases/download/v0.1.0/siddhi-tooling-0.1.0.zip).<br>
**Step 4** — Extract the downloaded zip and navigate to `<TOOLING_HOME>/bin`. <br> (`TOOLING_HOME` refers to the 
extracted folder) <br>
**Step 5** — Issue the following command in the command prompt (Windows) / terminal (Linux)

```
For Windows: tooling.bat
For Linux: ./tooling.sh
```

After successfully starting the Siddhi Editor, the terminal should look like as shown below:

![](../../images/quickstart/after-starting-editor.png?raw=true "Terminal after starting Siddhi Editor")

After starting the Siddhi Editor, access the Editor GUI by visiting the following link in your browser.
```
http://localhost:9390/editor
```
This takes you to the Siddhi Editor landing page.

![](../../images/quickstart/siddhi-editor.png?raw=true "Siddhi Editor")

## 4. Siddhi ‘Hello World!’

Siddhi Streaming SQL is a rich, compact, easy-to-learn SQL-like language. **Let's first learn how to find the total** of values
coming into a data stream and output the current running total value with each event. Siddhi has lot of in-built functions and extensions
available for complex analysis, but to get started, let's use a simple one. You can find more information about the Siddhi grammar
and its functions in the [Siddhi Query Guide](http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/).

Let's **consider a scenario where we are loading cargo boxes into a ship**. We need to keep track of the total
weight of the cargo added. **Measuring the weight of a cargo box when loading is considered an event**.

![](../../images/quickstart/loading-ship.jpeg?raw=true "Loading Cargo on Ship")

We can write a Siddhi program for the above scenario which has **4 parts**.

**Part 1 — Giving our Siddhi application a suitable name.** This is a Siddhi routine. In this example, let's name our application as
_“HelloWorldApp”_

```
@App:name("HelloWorldApp")
```
**Part 2 — Defining the input stream.** The stream needs to have a name and a schema defining the data that each incoming event should contain.
The event data attributes are expressed as name and type pairs. Also we need to attach a_"source"_  to the 
created stream so that we can send events to that stream. (**Source is the Siddhi way to consume streams from 
external systems.** This particular `http` type source will spin up a HTTP endpoint and keep on listening to events 
though that endpoint. To learn more about sources, see [source](http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source))


In this example:

* The name of the input stream — _“CargoStream”_ <br>
This contains only one data attribute:
* The name of the data in each event — _“weight”_
* Type of the data _“weight”_ — int
* Type of source - HTTP
* HTTP endpoint address - http://0.0.0.0:8006/cargo
* Accepted input data type - JSON

```
@source(type = 'http', receiver.url = "http://0.0.0.0:8006/cargo",@map(type = 'json'))
define stream CargoStream (weight int);
```
**Part 3 - Defining the output stream.** This has the same info as the previous definition with an additional
_totalWeight_ attribute that contains the total weight calculated so far. Here, we need to add a
_"sink"_  to log the `OutputStream` so that we can observe the output values. (**Sink is the Siddhi way to publish
streams to external systems.** This particular `log` type sink just logs the stream events. To learn more about sinks, see
[sink](http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#sink))
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
Final Siddhi application in the editor will look like below.
![](../../images/quickstart/hello-query.png?raw=true "Hello World in Stream Processor Studio")

You can copy the final Siddhi app from below.
<script src="https://gist.github.com/tishan89/dafb25a494add587b7259c077ac49914.js"></script>

## 5. Testing Siddhi Application
In this section we will be testing the logical accuracy of Siddhi query using in-built functions of Siddhi Editor. In
 a later section we will invoke the HTTP endpoint and perform an end to end integration test.

The Siddhi Editor has in-built support to simulate events. You can do it via the _“Event Simulator”_
panel at the left of the Siddhi Editor. You should save your _HelloWorldApp_ by browsing to **File** ->
**Save** before you run event simulation. Then click  **Event Simulator** and configure it as shown below.

![](../../images/quickstart/event-simulation.png?raw=true "Simulating Events in Stream Processor Studio")

**Step 1 — Configurations:**

* Siddhi App Name — _“HelloWorldApp”_
* Stream Name — _“CargoStream”_
* Timestamp — (Leave it blank)
* weight — 2 (or some integer)

**Step 2 — Click “Run” mode and then click “Start”**. This starts the Siddhi Application.
If the Siddhi application is successfully started, the following message is printed in the Stream Processor Studio console:</br>
_“HelloWorldApp.siddhi Started Successfully!”_

**Step 3 — Click “Send” and observe the terminal**
You can see a log that contains _“outputData=[2, 2]”_. Click **Send** again and observe a log with
_“outputData=[2, 4]”_. You can change the value of the weight and send it to see how the sum of the weight is updated.

![](../../images/quickstart/log.png?raw=true "Terminal after sending 2 twice")

Bravo! You have successfully completed creating Siddhi Hello World!

## 6. A bit of Stream Processing

This section will improve our Siddhi app to demonstrates how to carry out **temporal window processing** with Siddhi.

Up to this point, we have been carrying out the processing by having only the running sum value in-memory.
No events were stored during this process.

[Window processing](http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#window)
is a method that allows us to store some events in-memory for a given period so that we can perform operations
such as calculating the average, maximum, etc values within them.

Let's imagine that when we are loading cargo boxes into the ship **we need to keep track of the average weight of
the recently loaded boxes** so that we can balance the weight across the ship.
For this purpose, let's try to find the **average weight of last three boxes** of each event.

![](../../images/quickstart/siddhi-windows.png?raw=true "Terminal after sending 2 twice")

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

The updated Siddhi Application is given below:
<script src="https://gist.github.com/tishan89/6b57a2c226486e745451ef2f691f9750.js"></script>

Now you can send events using the Event Simulator and observe the log to see the sum and average of the weights of the last three
cargo events.

It is also notable that the defined `length window` only keeps 3 events in-memory. When the 4th event arrives, the
first event in the window is removed from memory. This ensures that the memory usage does not grow beyond a specific limit. There are also other
implementations done in Siddhi  to reduce the memory consumption. For more information, see [Siddhi Architecture](http://siddhi.io/documentation/siddhi-5.x/architecture-5.x/).

## 7. Running streaming application as a micro service

This step we will run above developed Siddhi application as a micro service utilizing docker. For othere available options please refer [here](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-in-various-environments). Here we will use siddhi-runner docker distribution. Follow below steps.

* Install docker in your machine and start the daemon.([https://docs.docker.com/install/](https://docs.docker
.com/install/))
* Pull the latest siddhi-runner image by executing below command
```
docker pull siddhiio/siddhi-runner-alpine:latest
```
* Navigate to Siddhi Editor and choose **File -> Export File** for download above Siddhi application as a file.
* Move downloaded Siddhi file(_HelloWorldModifiedApp.siddhi_) to a desired location(/home/me/siddhi-apps)
* Execute below command to start a streaming micro service which runs above developed siddhi application. **(Please 
pay attention to the siddhi app name as in the Quick Start Guide we have changed the modified Siddhi app name from 
that of the original app.)**
```
docker run -it -p 8006:8006 -v /home/me/siddhi-apps:/apps siddhiio/siddhi-runner-alpine 
-Dapps=/apps/HelloWorldModifiedApp.siddhi
```
* Once container is started use below curl command to send events into _"CargoStream"_ 
```
curl -X POST http://localhost:8006/cargo \
  --header "Content-Type:application/json" \
  -d '{"event":{"weight":2}}'
```
* You will observe below messages in container logs.
```
[2019-04-24 08:54:51,755]  INFO {io.siddhi.core.stream.output.sink.LogSink} - LOGGER : Event{timestamp=1556096091751, data=[2, 2, 2.0], isExpired=false}
[2019-04-24 08:56:25,307]  INFO {io.siddhi.core.stream.output.sink.LogSink} - LOGGER : Event{timestamp=1556096185307, data=[2, 4, 2.0], isExpired=false}
```

To learn more about the Siddhi functionality, see [Siddhi Query Guide](http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/).

Feel free to try out Siddhi and event simulation to understand Siddhi better.

If you have questions please post them to the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a> with <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag.
