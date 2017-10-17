WSO2 Siddhi 
===========

Siddhi is a java library that listens to events from data streams, detect complex conditions described via a **Streaming
 SQL language**, and trigger actions. It can be used to do both **_Stream Processing_** and 
 **_Complex Event Processing_**.  
 
## Overview 

![](images/siddhi-overview.png?raw=true "Overview")
![](docs/images/siddhi-overview.png?raw=true "Overview")

It can be used for;
 
* Data preprocessing
* Generate alerts based on thresholds
* Calculate aggregations over a short window or a long time period
* Joining multiple data streams
* Data correlation while finding missing and erroneous events
* Interact streaming data with databases
* Detecting temporal event patterns
* Tracking (something over space or time)
* Trend analysis (rise, fall, turn, tipple bottom)
* Real-time predictions with existing and online machine learning models
* And many more ... <a target="_blank" href="http://www.kdnuggets.com/2015/08/patterns-streaming-realtime-analytics.html">Patterns of Streaming Realtime Analytics</a>

Siddhi is free and open source, under **Apache Software License v2.0**.

## Get Started!

Get started with Siddhi in few minutes by following <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-quckstart-4.0/">Siddhi Quick Start Guide</a>

## Why use Siddhi ? 

* Fast, that <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a> 
use it to process 20 Billion events per day (300,000 events per second). 
* Lightweight (<2MB), embeddable in Android and RaspberryPi
* Has 40+ <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">Siddhi Extensions</a>
* 60+ companies including many Fortune 500 companies use Siddhi in production, following are some; 
    * WSO2 use Siddhi in their products such as <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Data Analytics Server</a> 
   and <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> to provide stream processing capabilities. 
   Uses it as the **edge analytics** library of [WSO2 IoT Server](http://wso2.com/iot?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17), 
   core of <a target="_blank" href="http://wso2.com/api-management?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 API Manager</a>'s throttling, and core of 
   <a target="_blank" href="http://wso2.com/platform?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 products'</a> analytics.
    * <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a> for fraud analytics
    * <a target="_blank" href="http://eagle.apache.org/docs/index.html">Apache Eagle</a> use Siddhi as a policy engine
* Solutions based on Siddhi have been finalists at <a target="_blank" href="http://dl.acm.org/results.cfm?query=(%252Bgrand%20%252Bchallenge%20%252Bwso2)&within=owners.owner=HOSTED&filtered=&dte=&bfr=">ACM DEBS Grand Challenge Stream Processing competitions in 2014, 2015, 2016, 2017</a>.
* Been basis of many academic research projects and have <a target="_blank" href="https://scholar.google.com/scholar?cites=5113376427716987836&as_sdt=2005&sciodt=0,5&hl=en">60+ citations</a>. 

If you also use Siddhi, we would love to hear more. 

## Try Siddhi with <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>

<a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> is a server version of Siddhi that's also released under 
 **Apache Software License v2.0**. It was a Strong Performer in <a target="_blank" href="https://go.forrester.com/blogs/16-04-16-15_true_streaming_analytics_platforms_for_real_time_everything/">The Forrester Wave: Big Data Streaming Analytics, Q1 2016</a> 
 (<a target="_blank" href="https://www.forrester.com/report/The+Forrester+Wave+Big+Data+Streaming+Analytics+Q1+2016/-/E-RES129023">Report</a>) 
and a <a target="_blank" href="https://www.gartner.com/doc/3314217/cool-vendors-internet-things-analytics">Cool Vendors in Internet of Things Analytics, 2016</a>. 

To get following capabilities by using <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> :  

* **Siddhi Query Editor** 
* **Siddhi Debugger**
* **Event Simulator** 
* Run Siddhi as a Server with High Availability and Scalability
* Monitoring support for Siddhi
* Realtime dashboard 
* Business user friendly query generation and deployment

There are domain specific solutions built using Siddhi, including <a target="_blank" href="https://wso2.com/analytics/solutions?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">Fraud Detection, Stock Market Surveillance, Location analytics, Proximity Marketing, Contextual Recommendation, Ad Optimization, Operational Analytics, and Detecting Chart Patterns</a>. 

If you want more information please contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.

## Features 

* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#cron-window">cron</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>This window returns events processed periodically as the output in time-repeating patterns, triggered based on time passing.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#externaltimebatch-window">externalTimeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) time window based on external time, that holds events arrived during windowTime periods, and gets updated for every windowTime.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#externaltime-window">externalTime</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A sliding time window based on external time. It holds events that arrived during the last windowTime period from the external timestamp, and gets updated on every monotonically increasing timestamp.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#frequent-window">frequent</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>This window returns the latest events with the most frequently occurred value for a given attribute(s). Frequency calculation for this window processor is based on Misra-Gries counting algorithm.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#lengthbatch-window">lengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) length window that holds a number of events specified as the windowLength. The window is updated each time a batch of events that equals the number specified as the windowLength arrives.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#length-window">length</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A sliding length window that holds the last windowLength events at a given time, and gets updated for each arrival and expiry.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#lossyfrequent-window">lossyFrequent</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>This window identifies and returns all the events of which the current frequency exceeds the value specified for the supportThreshold parameter.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#sort-window">sort</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>This window holds a batch of events that equal the number specified as the windowLength and sorts them in the given order.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#timebatch-window">timeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) time window that holds events that arrive during window.time periods, and gets updated for each window.time.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#timelength-window">timeLength</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A sliding time window that, at a given time holds the last window.length events that arrived during last window.time period, and gets updated for every event arrival and expiry.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#time-window">time</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#window">(Window)</a>)*<br><div style="padding-left: 1em;"><p>A sliding time window that holds events that arrived during the last windowTime period at a given time, and gets updated for each event arrival and expiry.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#cast-function">cast</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Converts the first parameter according to the cast.to parameter. Incompatible arguments cause Class Cast exceptions if further processed. This function is used with map extension that returns attributes of the object type. You can use this function to cast the object to an accurate and concrete type.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#coalesce-function">coalesce</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the value of the first input parameter that is not null, and all input parameters have to be on the same type.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#convert-function">convert</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Converts the first input parameter according to the convertedTo parameter.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#currenttimemillis-function">currentTimeMillis</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the current timestamp of siddhi application in milliseconds.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#default-function">default</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks if the 'attribute' parameter is null and if so returns the value of the 'default' parameter</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#eventtimestamp-function">eventTimestamp</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the timestamp of the processed event.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#ifthenelse-function">ifThenElse</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Evaluates the 'condition' parameter and returns value of the 'if.expression' parameter if the condition is true, or returns value of the 'else.expression' parameter if the condition is false. Here both 'if.expression' and 'else.expression' should be of the same type.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#instanceofboolean-function">instanceOfBoolean</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Boolean or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#instanceofdouble-function">instanceOfDouble</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Double or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#instanceoffloat-function">instanceOfFloat</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Float or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#instanceofinteger-function">instanceOfInteger</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Integer or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#instanceoflong-function">instanceOfLong</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Long or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#instanceofstring-function">instanceOfString</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of String or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#maximum-function">maximum</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the maximum value of the input parameters.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#minimum-function">minimum</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the minimum value of the input parameters.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#uuid-function">UUID</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#function">(Function)</a>)*<br><div style="padding-left: 1em;"><p>Generates a UUID (Universally Unique Identifier).</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#log-stream-processor">log</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processor">(Stream Processor)</a>)*<br><div style="padding-left: 1em;"><p>The logger stream processor logs the message with or without event for the given log priority.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#avg-aggregate-function">avg</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Calculates the average for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#count-aggregate-function">count</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the count of all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#distinctcount-aggregate-function">distinctCount</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the count of distinct occurrences for a given arg.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#max-aggregate-function">max</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the maximum value for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#maxforever-aggregate-function">maxForever</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>This is the attribute aggregator to store the maximum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#min-aggregate-function">min</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the minimum value for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#minforever-aggregate-function">minForever</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>This is the attribute aggregator to store the minimum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#stddev-aggregate-function">stdDev</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the calculated standard deviation for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#sum-aggregate-function">sum</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-function">(Aggregate Function)</a>)*<br><div style="padding-left: 1em;"><p>Returns the sum for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#pol2cart-stream-function">pol2Cart</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-function">(Stream Function)</a>)*<br><div style="padding-left: 1em;"><p>The pol2Cart function calculating the cartesian coordinates x & y for the given theta, rho coordinates and adding them as new attributes to the existing events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#inmemory-source">inMemory</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>)*<br><div style="padding-left: 1em;"><p>In-memory source that can communicate with other in-memory sinks within the same JVM, it is assumed that the publisher and subscriber of a topic uses same event schema (stream definition).</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#passthrough-source-mapper">passThrough</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mapper">(Source Mapper)</a>)*<br><div style="padding-left: 1em;"><p>Pass-through mapper passed events (Event[]) through without any mapping or modifications.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#inmemory-sink">inMemory</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>)*<br><div style="padding-left: 1em;"><p>In-memory transport that can communicate with other in-memory transports within the same JVM, itis assumed that the publisher and subscriber of a topic uses same event schema (stream definition).</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT/#passthrough-sink-mapper">passThrough</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mapper">(Sink Mapper)</a>)*<br><div style="padding-left: 1em;"><p>Pass-through mapper passed events (Event[]) through without any mapping or modifications.</p></div>

## Siddhi Versions

Find the released Siddhi libraries from <a target="_blank" href="http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/">here</a>.

 <a target="_blank" href=""></a> 
 
* **Active development version of Siddhi** : **v4.0.0**  _built on Java 8._ 
     
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/">Siddhi Query Guide</a> for Siddhi v4.x.x
    
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-architecture/">Architecture</a> of Siddhi v4.x.x


* **Latest Stable Release of Siddhi** : **v3.0.5** _built on Java 7._

    <a target="_blank" href="https://docs.wso2.com/display/DAS310/Siddhi+Query+Language">Siddhi Query Guide</a> for Siddhi v3.x.x

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M107-SNAPSHOT">4.0.0-M107-SNAPSHOT</a>.

## Jenkins Build Status

|  Siddhi Branch | Jenkins Build Status |
| :---------------------------------------- |:---------------------------------------
| master         | [![Build Status](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi/badge/icon)](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi )|

## How to Contribute
* Please report issues at <a target="_blank" href="https://github.com/wso2/siddhi/issues">GitHub Issue Tracker</a>.
* Feel fee to play with the <a target="_blank" href="https://github.com/wso2/siddhi">Siddhi source code</a> and send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2/siddhi/tree/master">master branch</a>. 
 
## Contact us 
 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 * For more details and support contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>
 
## Support 
* We are committed to ensuring support for Siddhi (with it's <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>) and <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> from development to production. 
* Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 
* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 

Siddhi was joint research project initiated by WSO2 and University of Moratuwa, Sri Lanka.
