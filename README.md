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
* And many more ... <a target="_blank" href="http://www.kdnuggets.com/2015/08/patterns-streaming-realtime-analytics.html">“Patterns of Streaming Realtime Analytics”</a>

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
* Solutions based on Siddhi has been a finalist at <a target="_blank" href="http://dl.acm.org/results.cfm?query=(%252Bgrand%20%252Bchallenge%20%252Bwso2)&within=owners.owner=HOSTED&filtered=&dte=&bfr=">ACM DEBS Grand Challenge Stream Processing competitions in 2014, 2015, 2016, 2017</a>.
* Been basis of many academic research projects and have <a target="_blank" href="https://scholar.google.com/scholar?cites=5113376427716987836&as_sdt=2005&sciodt=0,5&hl=en">60+ citations</a>. 

If you also use Siddhi, we would love to hear more. 

## Try Siddhi with <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>

<a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a> is a server version of Siddhi that's also released under 
 **Apache Software License v2.0**. It was a Strong Performer in <a target="_blank" href="https://go.forrester.com/blogs/16-04-16-15_true_streaming_analytics_platforms_for_real_time_everything/">The Forrester Wave™: Big Data Streaming Analytics, Q1 2016</a> 
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

## Siddhi Versions

Find the released Siddhi libraries from <a target="_blank" href="http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/">here</a>.

 <a target="_blank" href=""></a> 
 
* **Active development version of Siddhi** : **v4.0.0**  _built on Java 8._ 
 
    <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/">Siddhi Query Guide</a> for Siddhi v4.x.x

* **Latest Stable Release of Siddhi** : **v3.0.5** _built on Java 7._

    <a target="_blank" href="https://docs.wso2.com/display/DAS310/Siddhi+Query+Language">Siddhi Query Guide</a> for Siddhi v3.x.x

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/4.0.0-M85">4.0.0-M85</a>.

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
