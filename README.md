Siddhi Complex Event Processing Engine 
======================================

Siddhi CEP is a lightweight, easy-to-use Open Source Streaming Complex Event Processing Engine (CEP) released as a Java 
Library under Apache Software License v2.0. Siddhi can receive events from external sources, analyze them according to 
user specified **SQL-Like queries** and notifies appropriate complex events via multiple transports and data formats.

This project was initiated as a research project at University of Moratuwa, Sri Lanka, and now being improved by [WSO2 Inc](http://wso2.com/). 

* **Active development version of Siddhi** : **v4.0.0**  _built on Java 8._ 
 
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/">Siddhi Query Guide</a> for Siddhi v4.x.x

* **Latest Stable Release of Siddhi** : **v3.0.5** _built on Java 7._

    <a target="_blank" href="https://docs.wso2.com/display/DAS310/Siddhi+Query+Language">Siddhi Query Guide</a> for Siddhi v3.x.x

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2/siddhi">Source code</a>
* <a target="_blank" href="https://github.com/wso2/siddhi/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2/siddhi/issues">Issue tracker</a>
* <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">Siddhi Extensions</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52">4.0.0-M52</a>.

## Jenkins Build Status

|  Siddhi Branch | Jenkins Build Status |
| :---------------------------------------- |:---------------------------------------
| master         | [![Build Status](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi/badge/icon)](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi )|


## How to use 

**Using the Siddhi in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use Siddhi in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* All <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">Siddhi extensions</a> are shipped by default with WSO2 Stream Processor.

**Using Siddhi as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* To embed Siddhi as a library in your project add the following maven dependencies to your project.

``` 
    <dependency>
        <groupId>org.wso2.siddhi</groupId>
        <artifactId>siddhi-annotations</artifactId>
        <version>x.x.x</version>
    </dependency>  
    <dependency>
        <groupId>org.wso2.siddhi</groupId>
        <artifactId>siddhi-query-api</artifactId>
        <version>x.x.x</version>
    </dependency>
    <dependency>
        <groupId>org.wso2.siddhi</groupId>
        <artifactId>siddhi-query-compiler</artifactId>
        <version>x.x.x</version>
    </dependency>
    <dependency>
        <groupId>org.wso2.siddhi</groupId>
        <artifactId>siddhi-core</artifactId>
        <version>x.x.x</version>
    </dependency>    

```

* Siddhi is also used with [WSO2 IoT Server](http://wso2.com/iot?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17) for IoT analytics and as an edge analytics library.


## Features Supported

* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#sort-window">sort</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This window holds a batch of events that equal the number specified as the windowLength and sorts them in the given order.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#externaltimebatch-window">externalTimeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) time window based on external time, that holds events arrived during windowTime periods, and gets updated for every windowTime.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#lossyfrequent-window">lossyFrequent</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This window identifies and returns all the events of which the current frequency exceeds the value specified for the supportThreshold parameter.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#time-window">time</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A sliding time window that holds events that arrived during the last windowTime period at a given time, and gets updated for each event arrival and expiry.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#externaltime-window">externalTime</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A sliding time window based on external time. It holds events that arrived during the last windowTime period from the external timestamp, and gets updated on every monotonically increasing timestamp.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#timelength-window">timeLength</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A sliding time window that, at a given time holds the last window.length events that arrived during last window.time period, and gets updated for every event arrival and expiry.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#frequent-window">frequent</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This window returns the latest events with the most frequently occurred value for a given attribute(s). Frequency calculation for this window processor is based on Misra-Gries counting algorithm.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#cron-window">cron</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>This window returns events processed periodically as the output in time-repeating patterns, triggered based on time passing.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#lengthbatch-window">lengthBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) length window that holds a number of events specified as the windowLength. The window is updated each time a batch of events that equals the number specified as the windowLength arrives.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#length-window">length</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A sliding length window that holds the last windowLength events at a given time, and gets updated for each arrival and expiry.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#timebatch-window">timeBatch</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#windows">Window</a>)*<br><div style="padding-left: 1em;"><p>A batch (tumbling) time window that holds events that arrive during window.time periods, and gets updated for each window.time.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#maximum-function">maximum</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the maximum value of the input parameters.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#uuid-function">UUID</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Generates a UUID (Universally Unique Identifier).</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#cast-function">cast</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Converts the first parameter according to the cast.to parameter. Incompatible arguments cause Class Cast exceptions if further processed. This function is used with map extension that returns attributes of the object type. You can use this function to cast the object to an accurate and concrete type.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#ifthenelse-function">ifThenElse</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Evaluates the 'condition' parameter and returns value of the 'if.expression' parameter if the condition is true, or returns value of the 'else.expression' parameter if the condition is false. Here both 'if.expression' and 'else.expression' should be of the same type.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#instanceofboolean-function">instanceOfBoolean</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Boolean or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#convert-function">convert</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Converts the first input parameter according to the convertedTo parameter.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#instanceoflong-function">instanceOfLong</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Long or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#default-function">default</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks if the 'attribute' parameter is null and if so returns the value of the 'default' parameter</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#coalesce-function">coalesce</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the value of the first input parameter that is not null, and all input parameters have to be on the same type.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#instanceofdouble-function">instanceOfDouble</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Double or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#eventtimestamp-function">eventTimestamp</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the timestamp of the processed event.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#instanceoffloat-function">instanceOfFloat</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Float or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#instanceofstring-function">instanceOfString</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of String or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#instanceofinteger-function">instanceOfInteger</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Checks whether the parameter is an instance of Integer or not.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#minimum-function">minimum</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the minimum value of the input parameters.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#currenttimemillis-function">currentTimeMillis</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#functions">Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the current timestamp of siddhi application in milliseconds.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#log-stream-processor">log</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-processors">Stream Processor</a>)*<br><div style="padding-left: 1em;"><p>The logger stream processor logs the message with or without event for the given log priority.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#stddev-aggregate-function">stdDev</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the calculated standard deviation for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#max-aggregate-function">max</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the maximum value for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#minforever-aggregate-function">minForever</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>This is the attribute aggregator to store the minimum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#min-aggregate-function">min</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the minimum value for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#maxforever-aggregate-function">maxForever</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>This is the attribute aggregator to store the maximum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#distinctcount-aggregate-function">distinctCount</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the count of distinct occurrences for a given arg.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#count-aggregate-function">count</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the count of all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#avg-aggregate-function">avg</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Calculates the average for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#sum-aggregate-function">sum</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#aggregate-functions">Aggregate Function</a>)*<br><div style="padding-left: 1em;"><p>Returns the sum for all the events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#pol2cart-stream-function">pol2Cart</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#stream-functions">Stream Function</a>)*<br><div style="padding-left: 1em;"><p>The pol2Cart function calculating the cartesian coordinates x & y for the given theta, rho coordinates and adding them as new attributes to the existing events.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#passthrough-source-mapper">passThrough</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source-mappers">Source Mapper</a>)*<br><div style="padding-left: 1em;"><p>Pass-through mapper passed events (Event[]) through without any mapping or modifications.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#inmemory-source">inMemory</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sources">Source</a>)*<br><div style="padding-left: 1em;"><p>In-memory source that can communicate with other in-memory sinks within the same JVM, it is assumed that the publisher and subscriber of a topic uses same event schema (stream definition).</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#passthrough-sink-mapper">passThrough</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink-mappers">Sink Mapper</a>)*<br><div style="padding-left: 1em;"><p>Pass-through mapper passed events (Event[]) through without any mapping or modifications.</p></div>
* <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M52/#inmemory-sink">inMemory</a> *(<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sinks">Sink</a>)*<br><div style="padding-left: 1em;"><p>In-memory transport that can communicate with other in-memory transports within the same JVM, itis assumed that the publisher and subscriber of a topic uses same event schema (stream definition).</p></div>

## System Requirements
1. Minimum memory - 500 MB (based on in-memory data stored for processing)
2. Processor      - Pentium 800MHz or equivalent at minimum
3. Java SE Development Kit 1.8 (1.7 for 3.x version)
4. To build Siddhi CEP from the Source distribution, it is necessary that you have
   JDK 1.8 version (1.7 for 3.x version) or later and Maven 3.0.4 or later

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2/siddhi/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2/siddhi/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
