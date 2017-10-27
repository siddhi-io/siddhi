# Quick Start Guide to Siddhi 

[Siddhi](https://github.com/wso2/siddhi) is a 100% open source Java library thoroughly optimized for high performance. 
It performs **_Stream Processing_** and **_Complex Event Processing_** on real time data streams. 

Siddhi is used by many companies including Uber, and eBay (via Apache Eagle). In fact **Uber processes more than 20 billion 
events per day using Siddhi** for fraud analytics and Siddhi is used in Apache Eagle as a policy enforcement engine.

The quick start guide has **6 sections** as following,

1. Overview on Stream Processing and Complex Event Processing - about the **domain** of Siddhi
2. Overview of Siddhi - basic **architecture** explained
3. Using Siddhi for the First Time - how to **set up** the software
4. Siddhi ‘Hello World!’ — Your **First Siddhi Application**
5. Simulating Events - **testing** your query with simulated events
6. Bit of Stream Processing - **temporal event processing**

## 1. Overview on Stream Processing and Complex Event Processing (CEP)

Before diving into how to use Siddhi we will first discuss in brief what Stream Processing 
and Complex Event Processing is such that we can get more idea on what use-cases can be covered by Siddhi.

First we will look at **what an event is** through an example. **If we take the transactions through an ATM as a data 
stream, one withdrawal from it would be an event**. This event will contain data about amount, time, account number etc. 
Many such transactions will make up a stream.

![](../images/quickstart/event-stream.png?raw=true "Event Stream")

[Forster](https://reprints.forrester.com/#/assets/2/202/'RES136545'/reports) defines Streaming Analytics as, 

> Software that provides analytical operators to **orchestrate data flow**, **calculate analytics**, and **detect patterns** on 
event data **from multiple, disparate live data sources** to allow developers to build applications that **sense, think, 
and act in real time**.

[Gartner’s IT Glossary](https://www.gartner.com/it-glossary/complex-event-processing) defines CEP as following,

>"CEP is a kind of computing in which **incoming data about events is distilled into more useful, higher level “complex” 
event data** that provides insight into what is happening."
>
>"**CEP is event-driven** because the computation is triggered by the receipt of event data. CEP is used for highly 
demanding, continuous-intelligence applications that enhance situation awareness and support real-time decisions."

![](../images/quickstart/siddhi-basic.png?raw=true "Siddhi Basic Representation")

Basically Siddhi receives data event-by-event and processes them in real time to give meaningful information.

Siddhi Can be used for use-cases like;

* Fraud Analytics 
* Monitoring 
* Abnormal Detection
* Sentiment Analysis
* Processing Customer Behaviour
* .. etc

## 2. Overview of Siddhi

![](../images/siddhi-overview.png?raw=true "Overview")

As you can see above Siddhi can,
* accept event inputs from many different types of sources
* process them to generate insights
* publish them to many types of sinks.

To use Siddhi a user have to write the processing logic as a **Siddhi Application** using **Siddhi Streaming SQL** 
language which will be discussed in the 4th section. After writing and starting **a Siddhi application**, it will

1. take data one-by-one as events
2. process the data in each event
3. generate new high level events based on the processing done so far
4. send newly generated events to the output as streams.

## 3. Using Siddhi for the First Time

We will use WSO2 Stream Processor(will be addressed as _‘SP_’ hereafter) — a server version of Siddhi that has 
sophisticated editor with GUI (called _**“Stream Processor Studio”**_)where you can write your query and simulate events
as a data stream.

**Step 1** — Install 
[Oracle Java SE Development Kit (JDK)](http://www.oracle.com/technetwork/java/javase/downloads/index.html) version 1.8 <br>
**Step 2** — [Set the JAVA_HOME](https://docs.oracle.com/cd/E19182-01/820-7851/inst_cli_jdk_javahome_t/) environment 
variable <br>
**Step 3** — Download the latest [WSO2 Stream Processor](https://github.com/wso2/product-sp/releases) <br>
**Step 4** — Extract the downloaded zip and navigate to `<SP_HOME>/bin` <br> (`SP_HOME` refers to the extracted folder) <br>
**Step 5** — Issue the following command in command prompt (Windows) / terminal (Linux) 

```
For Windows: editor.bat
For Linux: ./editor.sh
```
For more details on WSO2 Stream Processor refer it's [Quick Start Guide](https://docs.wso2.com/display/SP400/Quick+Start+Guide).

After successfully starting the Stream Processor Studio the terminal should look like this in Linux,

![](../images/quickstart/after-starting-sp.png?raw=true "Terminal after starting WSO2 Stream Processor Text Editor")

After starting the WSO2 Stream Processor access the Stream Processor Studio by visiting the following link in your browser
```
http://localhost:9090/editor
```
This should lead you the Stream Processor Studio landing page,

![](../images/quickstart/sp-studio.png?raw=true "Stream Processor Studio")

## 4. Siddhi ‘Hello World!’ — Your First Siddhi Application

Siddhi Streaming SQL is a rich, compact, easy-to-learn SQL-like language. **We will first learn how to find the total** of values 
coming in a data stream and output current running total value with each event. Siddhi has lot of in-built functions and extensions 
available for complex analysis but we will start with a simple one. You can find more information about the grammar 
and its functions in [Siddhi Query Guide](https://wso2.github.io/siddhi/documentation/siddhi-4.0/).

We will **consider a scenario where we are loading cargo boxes into a ship**. We need to keep track of the total 
weight of the cargo added. **Weight measurement of a cargo box when loading is treated as an event**.

![](../images/quickstart/loading-ship.jpeg?raw=true "Loading Cargo on Ship")

We can write a Siddhi program for the above scenario which will have **4 parts**.

**Part 1 — Giving our Siddhi application a suitable name.** This is a Siddhi routine. Here we will name our app as 
_“HelloWorldApp”_

```
@App:name("HelloWorldApp")
```
**Part 2 — Defining the input stream.** Stream will have a name and a schema defining the data that each of it's incoming event will contain.
The event data attributes are expressed as name and type pairs. Here,

* Name of the input stream — _“CargoStream”_ <br>
This contains only one data attribute:
* name of the data in each event — _“weight”_
* Type of the data _“weight”_ — int

```
define stream CargoStream (weight int);
```
**Part 3 - Defining the output stream.** This will have the same info as the previous definition with an additional 
_totalWeight_ attribute containing the total weight calculated so far. Here we will add a 
_"sink"_  to log the `OutputStream` so that we can observe the output values. (**Sink is the Siddhi way to publish 
streams to external systems.** This particular `log` type sink will just log the stream events. Learn more about sinks 
[here](https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink))
```
@sink(type='log', prefix='LOGGER')
define stream OutputStream(weight int, totalWeight long);
```
**Part 4 — The actual Siddhi query.** Here we will specify 5 things.

1. A name for the query — _“HelloWorldQuery”_
2. Which stream should be taken into processing — _“CargoStream”_
3. What data we require in the output stream — _“weight”_, _“totalWeight”_
4. How the output should be calculated - by *sum*ing the *weight*s  
5. which stream should be populated with the output — _“OutputStream”_

```
@info(name='HelloWorldQuery')
from CargoStream
select weight, sum(weight) as totalWeight
insert into OutputStream;
```

![](../images/quickstart/hello-query.png?raw=true "Hello World in Stream Processor Studio")

## 5. Simulating Events

The Stream Processor Studio has in-built support to simulate events. You can do it using the _“Event Simulator”_ 
panel in the left side of the Stream Processor Studio. **You should save your HelloWorldApp by going to `file` -> 
`save` before you run event simulation.** Then click on the _“Event Simulator”_ and configure it as shown below.

![](../images/quickstart/event-simulation.png?raw=true "Simulating Events in Stream Processor Studio")

**Step 1 — Configurations,**

* Siddhi App Name — _“HelloWorldApp”_
* Stream Name — _“CargoStream”_
* Timestamp — (Leave it blank)
* weight — 2 (or some integer)

**Step 2 — Click on “Run” mode and hit the “Start” button**. This will start the Siddhi App. 
You can verify Siddhi app getting started by the message 
_“HelloWorldApp.siddhi Started Successfully!”_ printed in the Stream Processor Studio console.

**Step 3 — Click on the “Send” button and observe the terminal** where you started WSO2 Stream Processor Studio. 
You could see a log that contains _“outputData=[2, 2]”_. Click on send again and you should see a log with 
_“outputData=[2, 4]”_. You can change the value of the weight and send it to see how its getting summed up.

![](../images/quickstart/log.png?raw=true "Terminal after sending 2 twice")

Bravo! You have successfully finished you Siddhi Hello World! 

## 6. Bit of Stream Processing - **temporal event processing**

As mentioned previously, Siddhi has a lot of functionality. **In this section we will see how to do temporal
 window processing**
with Siddhi.

**Upto now** we have been processing **by only having the running sum value in-memory**. 
We are not storing any events during this processing. 

[Window processing](https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window) 
is a method which will help us to stored some events in-memory for a given period such that it let us perform operations 
such as finding average, maximum, etc values within them.

Let's imagine that when we are loading cargo boxes into the ship **we need to keep track of the average weight of 
recently loaded boxes** so that we can balance the weight across the ship. 
For that matter lets try to find the **average weight of last three boxes** on each event.

![](../images/quickstart/siddhi-windows.png?raw=true "Terminal after sending 2 twice")

We need to modify our query as following for window processing,
```
@info(name='HelloWorldQuery') 
from CargoStream#window.length(3)
select weight, sum(weight) as totalWeight, avg(weight) as averageWeight
insert into OutputStream;
```

* `from CargoStream#window.length(3)` - Here we are asking Siddhi to keep last 3 events in memory for processing
* `avg(weight) as averageWeight` - Here we are finding the average of events stored in the window and outputting the 
average value as _"averageWeight"_ (Note: Now the `sum` also calculates the `totalWeight` based on last three events).

We also need to modify the _"OutputStream"_ definition to accommodate the new _"averageWeight"_

```
define stream OutputStream(weight int, totalWeight long, averageWeight double);
```

Now the updated Siddhi App should look like the following,

![](../images/quickstart/window-processing-app.png?raw=true "Window Processing with Siddhi")

Now you can send events using Event Simulator and observe the log to see the average and sum of weights of last three 
cargo events.

It is also notable that the defined `length window` only keeps 3 events in-memory. Where when the 4th event comes the 
first event in the window get removed from memory. This will not make the memory usage grow beyond a limit, Siddhi has also done other 
optimizations to reduce memory consumptions refer [Siddhi Architecture](https://wso2.github.io/siddhi/documentation/siddhi-architecture/) 
for more details.  

You can learn about more Siddhi functionalities from [Siddhi Query Guide](https://wso2.github.io/siddhi/documentation/siddhi-4.0/).

Feel free to play around with the Siddhi and event simulation to get better understanding.

If you have questions please post them at <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a> with <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag.