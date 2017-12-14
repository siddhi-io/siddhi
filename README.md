WSO2 Siddhi 
===========

Siddhi is a java library that listens to events from data streams, detects complex conditions described via a **Streaming
 SQL language**, and triggers actions. It performs both **_Stream Processing_** and **_Complex Event Processing_**.  
 
## Overview 

![](docs/images/siddhi-overview.png?raw=true "Overview")

Siddhi supports the following:
 
* Data preprocessing
* Generating alerts based on thresholds
* Calculating aggregations over a short window or a long time period
* Joining multiple data streams
* Correlating data while finding missing and erroneous events
* Interacting streaming data with databases
* Detecting temporal event patterns
* Tracking (something over space or time)
* Analyzing trends (rise, fall, turn, tipple bottom)
* Making real-time predictions with existing and online machine learning models
* And many more ...  For more information, see <a target="_blank" href="http://www.kdnuggets.com/2015/08/patterns-streaming-realtime-analytics.html">Patterns of Streaming Realtime Analytics</a>

Siddhi is free and open source, under **Apache Software License v2.0**.

## Get Started!

Get started with Siddhi in a few minutes by following the <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-quckstart-4.0/">Siddhi Quick Start Guide</a>

## Why use Siddhi ? 

* It is **fast**. <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a> 
uses it to process 20 Billion events per day (300,000 events per second). 
* It is **lightweight** (<2MB),  and embeddable in Android and RaspberryPi.
* It has **over 40 <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">Siddhi Extensions</a>**
* It is **used by over 60 companies including many Fortune 500 companies** in production. Following are some examples:
    * **WSO2** uses Siddhi for the following purposes:
        * To provide stream processing capabilities in their products such as <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Data Analytics Server</a> 
   and <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>.
        * As the **edge analytics** library of [WSO2 IoT Server](http://wso2.com/iot?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17).
        * As the core of <a target="_blank" href="http://wso2.com/api-management?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 API Manager</a>'s throttling. 
        * As the core of <a target="_blank" href="http://wso2.com/platform?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 products'</a> analytics.
    * **<a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a>** uses Siddhi for fraud analytics.
    * **<a target="_blank" href="http://eagle.apache.org/docs/index.html">Apache Eagle</a>** uses Siddhi as a policy engine.
* Solutions based on Siddhi have been **finalists at <a target="_blank" href="http://dl.acm.org/results.cfm?query=(%252Bgrand%20%252Bchallenge%20%252Bwso2)&within=owners.owner=HOSTED&filtered=&dte=&bfr=">ACM DEBS Grand Challenge Stream Processing competitions** in 2014, 2015, 2016, 2017</a>.
* Siddhi has been **the basis of many academic research projects** and has <a target="_blank" href="https://scholar.google.com/scholar?cites=5113376427716987836&as_sdt=2005&sciodt=0,5&hl=en">**over 60 citations**</a>. 

If you are a Siddhi user, we would love to hear more. 

## Try Siddhi with <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>

<a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> is a server version of Siddhi that is also released under 
 **Apache Software License v2.0**. It was a Strong Performer in <a target="_blank" href="https://go.forrester.com/blogs/16-04-16-15_true_streaming_analytics_platforms_for_real_time_everything/">The Forrester Wave: Big Data Streaming Analytics, Q1 2016</a> 
 (<a target="_blank" href="https://www.forrester.com/report/The+Forrester+Wave+Big+Data+Streaming+Analytics+Q1+2016/-/E-RES129023">Report</a>) 
and a <a target="_blank" href="https://www.gartner.com/doc/3314217/cool-vendors-internet-things-analytics">Cool Vendors in Internet of Things Analytics, 2016</a>. 

If you use <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>, you can use the Siddhi functionality with the following additional features:  

* The **Siddhi Query Editor** tool
* The **Siddhi Debugger** tool
* The **Event Simulator**  tool
* Run Siddhi as a **server** with **high availability** and **scalability**.
* Monitoring support for Siddhi
* Realtime dashboard 
* Business user-friendly query generation and deployment

There are domain specific solutions built using Siddhi, including <a target="_blank" href="https://wso2.com/analytics/solutions?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">Fraud Detection, Stock Market Surveillance, Location analytics, Proximity Marketing, Contextual Recommendation, Ad Optimization, Operational Analytics, and Detecting Chart Patterns</a>. 

For more information please contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.

## Siddhi Versions

Find the released Siddhi libraries <a target="_blank" href="http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/">here</a>.

 <a target="_blank" href=""></a> 
 
* **Active development version of Siddhi** : **v4.0.0**  _built on Java 8._ 
     
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/">Siddhi Query Guide</a> for Siddhi v4.x.x
    
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-architecture/">Architecture</a> of Siddhi v4.x.x


* **Latest Stable Release of Siddhi** : **v3.0.5** _built on Java 7._

    <a target="_blank" href="https://docs.wso2.com/display/DAS310/Siddhi+Query+Language">Siddhi Query Guide</a> for Siddhi v3.x.x

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-beta19">4.0.0-beta19</a>.

## Jenkins Build Status

|  Siddhi Branch | Jenkins Build Status |
| :---------------------------------------- |:---------------------------------------
| master         | [![Build Status](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi/badge/icon)](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi )|

## How to Contribute
* Report issues at <a target="_blank" href="https://github.com/wso2/siddhi/issues">GitHub Issue Tracker</a>.
* Feel free to try out the <a target="_blank" href="https://github.com/wso2/siddhi">Siddhi source code</a> and send your contributions as pull requests to the <a target="_blank" href="https://github.com/wso2/siddhi/tree/master">master branch</a>. 
 
## Contact us 
 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 * For more details and support contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>
 
## Support 
* We are committed to ensuring support for Siddhi (with its <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">extensions</a>) and <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> from development to production. 
* Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 
* For more details and to take advantage of this unique opportunity, contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 


Siddhi was joint research project initiated by <a target="_blank" href="http://wso2.com/"> WSO2</a> and <a target="_blank" href="http://www.mrt.ac.lk/web/">University of Moratuwa</a>, Sri Lanka.
