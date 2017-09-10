WSO2 Siddhi 
===========

Siddhi is a java library that listens to events from data streams, detect complex conditions described via a **Streaming SQL language**, and trigger actions.
It can be used to do both Stream Processing and Complex Event Processing. 
For example of conditions it can detect, see <a target="_blank" href="http://www.kdnuggets.com/2015/08/patterns-streaming-realtime-analytics.html">“Patterns of Streaming Realtime Analytics”</a>. 
To know about Streaming SQL, refer to its detail feature list and <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/">Siddhi Query Guide</a>.  

It is lightweight (<2MB) and fast, that <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a> 
use it to process 20 Billion events per day (300,000 events per second). Solutions based on Siddhi has been a Finalist at <a target="_blank" href="http://dl.acm.org/results.cfm?query=(%252Bgrand%20%252Bchallenge%20%252Bwso2)&within=owners.owner=HOSTED&filtered=&dte=&bfr=">ACM DEBS Grand Challenge Stream Processing competitions in 2014, 2015, 2016, 2017</a>.
Siddhi also has been basis of many academic research projects and have <a target="_blank" href="https://scholar.google.com/scholar?cites=5113376427716987836&as_sdt=2005&sciodt=0,5&hl=en">60+ citations</a>. 

Siddhi is actively developed by <a target="_blank" href="http://wso2.com?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Inc</a>. It is free and open source, under **Apache Software License v2.0**. 
It is been used in production by <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a>,
<a target="_blank" href="http://eagle.apache.org/docs/index.html">Apache Eagle</a>, 60+ companies including many Fortune 500 companies.
 Siddhi is also heavily used by WSO2 in their products such as <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Data Analytics Server</a> 
 and <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> to provide stream processing capabilities. 
 Siddhi is also the core of <a target="_blank" href="http://wso2.com/api-management?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 API Manager</a>'s throttling, and the core of 
 <a target="_blank" href="http://wso2.com/platform?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 products'</a> analytics.
If you are using Siddhi, we would love to hear more. 

If you want **Editor, Debugger, Simulator** capabilities for Siddhi and looking to use Siddhi as a Server, with High Availability and Scalability, please checkout <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>, 
which is also open source under **Apache Software License v2.0**. 
<a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> the server version of Siddhi was a 
Strong Performer in <a target="_blank" href="https://go.forrester.com/blogs/16-04-16-15_true_streaming_analytics_platforms_for_real_time_everything/">The Forrester Wave™: Big Data Streaming Analytics, Q1 2016</a> (<a target="_blank" href="https://www.forrester.com/report/The+Forrester+Wave+Big+Data+Streaming+Analytics+Q1+2016/-/E-RES129023">Report</a>) 
and a <a target="_blank" href="https://www.gartner.com/doc/3314217/cool-vendors-internet-things-analytics">Cool Vendors in Internet of Things Analytics, 2016</a>. 
There are domain specific solutions built using Siddhi, including <a target="_blank" href="https://wso2.com/analytics/solutions?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">Fraud Detection, Stock Market Surveillance, Location analytics, Proximity Marketing, Contextual Recommendation, Ad Optimization, Operational Analytics, and Detecting Chart Patterns</a>. 
If you want more information please contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.

Siddhi was initiated as a joint research project between WSO2 and and University of Moratuwa, Sri Lanka.

 <a target="_blank" href=""></a> 
 
* **Active development version of Siddhi** : **v4.0.0**  _built on Java 8._ 
 
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/">Siddhi Query Guide</a> for Siddhi v4.x.x

* **Latest Stable Release of Siddhi** : **v3.0.5** _built on Java 7._

    <a target="_blank" href="https://docs.wso2.com/display/DAS310/Siddhi+Query+Language">Siddhi Query Guide</a> for Siddhi v3.x.x

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2/siddhi">Source code</a>
* <a target="_blank" href="https://github.com/wso2/siddhi/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2/siddhi/issues">Issue tracker</a>
* <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">Siddhi Extensions</a>
* <a target="_blank" href="https://wso2.github.io/siddhi/#support">Support</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M62">4.0.0-M62</a>.

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


##Features

- Retrieving Events 
    - From various data sources supporting various message formats
 - Mapping Events
    - Mapping events with various data formats to Stream for processing
    - Mapping streams to multiple data formats for publishing
 - Processing Streams
    - Filter 
        - Filtering stream based on conditions
    - Window
        - Support for sliding and batch (tumbling) and many other type of windows  
    - Aggregation 
        - For long running aggregations and aggregation over windows 
        - Supporting `Avg`, `Sum`, `Min`, `Max`, etc
        - Ability aggregate processing with `Group by` and filter aggrigated data with `Having` conditions
    - Incremental summarisation
        - Support for processing and retrieving long running summarisations
    - Table 
        - For storing events for future processing and retrieving them on demand
        - Supporting storage in in-memory, RDBMs, Solr, mongoDb, etc 
    - Join
        - Joining two streams based on conditions 
        - Joining a streams with table or incremental summarisation based on conditions  
        - Supports Left, Right & Full Outer Joins and Inner Joins
    - Pattern 
        - Identifies event occurrence patterns among streams
        - Identify non occurrence of events
        - Supports repetitive matches of event pattern occurrences with logical conditions and time bound
    - Sequence processing
        - Identifies continuous sequence of events from streams
        - Supports zero to many, one to many, and zero to one condition
    - Partitions
        - Grouping queries and based on keywords and value ranges for isolated parallel processing
    - Scripting 
        - Support writing scripts like JavaScript, Scala and R within Siddhi Queries
    - Process Based on event time
        - Whole execution driven by the event time  
 - Publishing Events 
    - To various data sources with various message formats
    - Supporting load balancing and failover data publishing 
 - Snapshot and restore
    - Support for periodic state persistence and restore capabilities for long running execution

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

* We are committed to ensuring support for Siddhi (with it's extensions) and <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
