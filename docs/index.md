Siddhi 
======

  [![Jenkins Build Status](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi/badge/icon)](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi)
  [![GitHub (pre-)release](https://img.shields.io/github/release/siddhi-io/siddhi/all.svg)](https://github.com/siddhi-io/siddhi/releases)
  [![GitHub (Pre-)Release Date](https://img.shields.io/github/release-date-pre/siddhi-io/siddhi.svg)](https://github.com/siddhi-io/siddhi/releases)
  [![GitHub last commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi.svg)](https://github.com/siddhi-io/siddhi/commits/master)
  [![codecov](https://codecov.io/gh/siddhi-io/siddhi/branch/master/graph/badge.svg)](https://codecov.io/gh/siddhi-io/siddhi)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Siddhi is a **_Streaming_** and **_Complex Event Processing_** engine that listens to events from data streams, detects complex conditions described via a **Streaming
 SQL language**, and triggers actions.   
 
## Overview 

![](https://raw.githubusercontent.com/wso2/siddhi/master/docs/images/siddhi-overview.png?raw=true "Overview")

Siddhi supports:
 
* Streaming data integration 
    * Retrieving data from various event sources (Kafka, NATS, JMS, HTTP, CDC, etc)
    * Map events to and from multiple event formats (JSON, XML, Text, Avro, etc)
    * Data preprocessing & cleaning
    * Joining multiple data streams 
    * Integrate streaming data with databases (RDBMS, Cassandra, HBase, Redis, etc)
    * Integrate with external services
    * Publish data to multiple event sinks (Email, JMS, HTTP, etc)
    
* Streaming data analytics
    * Generating alerts based on thresholds
    * Calculate aggregations over a short windows (time, length, session, unique, etc) or a long time period
    * Calculate aggregations over long time periods with seconds, minutes, hours, days, months & years granularity  
    * Correlating data while finding missing and erroneous events
    * Detecting temporal event patterns
    * Analyzing trends (rise, fall, turn, tipple bottom)
    * Run pretreated machine learning models (PMML, Tensorflow) 
    * Learn and predict at runtime using online machine learning models

* Adaptive Intelligence
    * Static rule processing 
    * Stateful rule processing 
    * Decision making through synchronous stream processing  
    * Query tables, windows and aggregations  
      
* And many more ...  For more information, see <a target="_blank" href="http://www.kdnuggets.com/2015/08/patterns-streaming-realtime-analytics.html">Patterns of Streaming Realtime Analytics</a>

Siddhi is free and open source, under **Apache Software License v2.0**.

## Get Started!

Get started with Siddhi in a few minutes by following the <a target="_blank" href="https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/quckstart-5.x/">Siddhi Quick Start Guide</a>

## Why use Siddhi ? 

* It is **fast**. <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a> 
uses it to process 20 Billion events per day (300,000 events per second). 
* It is **lightweight** (<2MB),  and embeddable in Android, Python and RaspberryPi.
* It has **over 50 <a target="_blank" href="https://siddhi-io.github.io/siddhi/extensions/">Siddhi Extensions</a>**
* It is **used by over 60 companies including many Fortune 500 companies** in production. Following are some examples:
    * **WSO2** uses Siddhi for the following purposes:
        * To provide stream processing capabilities in their products such as <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Stream Processor</a>.
        * As the **edge analytics** library of [WSO2 IoT Server](http://wso2.com/iot?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17).
        * As the core of <a target="_blank" href="http://wso2.com/api-management?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 API Manager</a>'s throttling. 
        * As the core of <a target="_blank" href="http://wso2.com/platform?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 products'</a> analytics.
    * **<a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a>** uses Siddhi for fraud analytics.
    * **<a target="_blank" href="http://eagle.apache.org/docs/index.html">Apache Eagle</a>** uses Siddhi as a policy engine.
* Solutions based on Siddhi have been **finalists at <a target="_blank" href="http://dl.acm.org/results.cfm?query=(%252Bgrand%20%252Bchallenge%20%252Bwso2)&within=owners.owner=HOSTED&filtered=&dte=&bfr=">ACM DEBS Grand Challenge Stream Processing competitions** in 2014, 2015, 2016, 2017</a>.
* Siddhi has been **the basis of many academic research projects** and has <a target="_blank" href="https://scholar.google.com/scholar?cites=5113376427716987836&as_sdt=2005&sciodt=0,5&hl=en">**over 60 citations**</a>. 

If you are a Siddhi user, we would love to hear more. 

## Develop Siddhi using IntelliJ IDEA 

Install <a target="_blank" href="https://siddhi-io.github.io/siddhi-plugin-idea/">IDEA plugin</a> to get the following features:

* **Siddhi Query Editor** with syntax highlighting and with basic auto completion
* **Siddhi Runner and Debugger** support to test Siddhi Application

## Try Siddhi with <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Stream Processor</a>

<a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Stream Processor</a> is a server version of Siddhi that is also released under 
 **Apache Software License v2.0**. It was a Strong Performer in <a target="_blank" href="https://go.forrester.com/blogs/16-04-16-15_true_streaming_analytics_platforms_for_real_time_everything/">The Forrester Wave: Big Data Streaming Analytics, Q1 2016</a> 
 (<a target="_blank" href="https://www.forrester.com/report/The+Forrester+Wave+Big+Data+Streaming+Analytics+Q1+2016/-/E-RES129023">Report</a>) 
and a <a target="_blank" href="https://www.gartner.com/doc/3314217/cool-vendors-internet-things-analytics">Cool Vendors in Internet of Things Analytics, 2016</a>. 

If you use <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Stream Processor</a>, you can use the Siddhi functionality with the following additional features:  

* The **Siddhi Query Editor** tool with syntax highlighting and advanced auto completion support
* The **Siddhi Runner and Debugger** tool
* The **Event Simulator**  tool
* Run Siddhi as a **server** with **high availability** and **scalability**.
* Monitoring support for Siddhi
* Realtime dashboard 
* Business user-friendly query generation and deployment

There are domain specific solutions built using Siddhi, including <a target="_blank" href="https://wso2.com/analytics/solutions?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">Fraud Detection, Stock Market Surveillance, Location analytics, Proximity Marketing, Contextual Recommendation, Ad Optimization, Operational Analytics, and Detecting Chart Patterns</a>. 

For more information please contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.

## Siddhi Versions

 <a target="_blank" href=""></a> 
 
* **Active development version of Siddhi** : **v5.0.0-SNAPSHOT**  _built on Java 8 & 11._ 
     
    Find the released Siddhi libraries <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi/siddhi-core">here</a>.

    <a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/">Siddhi Query Guide</a> for Siddhi v5.x.x
    
    <a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/architecture-5.x/">Architecture</a> of Siddhi v5.x.x

* **Latest Stable Release of Siddhi v4.x.x** : **v4.4.8** _built on Java 8. (Recommended for production use)_
     
    Find the released Siddhi libraries <a target="_blank" href="http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/">here</a>.

    <a target="_blank" href="http://siddhi.io/documentation/siddhi-4.x/query-guide-4.x/">Siddhi Query Guide</a> for Siddhi v4.x.x
    
    <a target="_blank" href="http://siddhi.io/documentation/siddhi-4.x/architecture-4.x/">Architecture</a> of Siddhi v4.x.x


* **Latest Stable Release of Siddhi v3.x.x** : **v3.2.3** _built on Java 7._

    Find the released Siddhi libraries <a target="_blank" href="http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/">here</a>.

    <a target="_blank" href="https://docs.wso2.com/display/DAS310/Siddhi+Query+Language">Siddhi Query Guide</a> for Siddhi v3.x.x

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/5.0.0-m11">5.0.0-m11</a>.

## How to Contribute
* Report issues at <a target="_blank" href="https://github.com/wso2/siddhi/issues">GitHub Issue Tracker</a>.
* Feel free to try out the <a target="_blank" href="https://github.com/wso2/siddhi">Siddhi source code</a> and send your contributions as pull requests to the <a target="_blank" href="https://github.com/wso2/siddhi/tree/master">master branch</a>.

## Build from the Source

### Prerequisites
* [Oracle JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) or [OpenJDK 8](http://openjdk.java.net/install/) (Java 8 should be used for building in order to support both Java 8 and Java 11 at runtime)
* [Maven 3.5.x version](https://maven.apache.org/install.html)

### Steps to Build
1. Get a clone or download source from [Github](https://github.com/siddhi-io/siddhi.git)

    ```bash
    git clone https://github.com/siddhi-io/siddhi.git
    ```
    
1. Run the Maven command ``mvn clean install`` from the root directory
 
## Contact us 
 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 * For more details and support contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>
 
## Support 
* We are committed to ensuring support for Siddhi (with its <a target="_blank" href="https://siddhi-io.github.io/siddhi/extensions/">extensions</a>) and <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Stream Processor</a> from development to production.
* Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 
* For more details and to take advantage of this unique opportunity, contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 


Siddhi was joint research project initiated by <a target="_blank" href="http://wso2.com/">WSO2</a> and <a target="_blank" href="http://www.mrt.ac.lk/web/">University of Moratuwa</a>, Sri Lanka.
