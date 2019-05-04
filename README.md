**Siddhi** - Cloud native stream processing 
===========================================

  [![Jenkins Build Status](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi/badge/icon)](https://wso2.org/jenkins/view/wso2-dependencies/job/siddhi/job/siddhi)
  [![GitHub (pre-)release](https://img.shields.io/github/release/siddhi-io/siddhi/all.svg)](https://github.com/siddhi-io/siddhi/releases)
  [![GitHub (Pre-)Release Date](https://img.shields.io/github/release-date-pre/siddhi-io/siddhi.svg)](https://github.com/siddhi-io/siddhi/releases)
  [![GitHub last commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi.svg)](https://github.com/siddhi-io/siddhi/commits/master)
  [![codecov](https://codecov.io/gh/siddhi-io/siddhi/branch/master/graph/badge.svg)](https://codecov.io/gh/siddhi-io/siddhi)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

[Geting Started](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/quckstart-5.x/) | [Download](https://siddhi-io.github.io/siddhi/download/) | [User Guide](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/) | [Contribute](https://siddhi-io.github.io/siddhi/contribution/)

Siddhi is a cloud native **_Streaming_** and **_Complex Event Processing_** engine that understands **Streaming SQL queries** in order to capture events from diverse data sources, process them, detect complex conditions, and publish output to various endpoints in real time.

Siddhi can run as an embedded [Java library](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-a-java-library), and as a micro service on [bare metal, VM](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-local-micro-service), [Docker](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-docker-micro-service) and natively in [Kubernetes](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-kubernetes-micro-service). It also has a [graphical and text editor](#siddhi-development-environment) for building Streaming Data Integration and Streaming Analytics applications.

## Distributions

<a href="https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-kubernetes-micro-service" rel="nofollow">
 <img src="https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/distributions/kubernetes.png?raw=true" alt="Kubernetes" width="19%">
</a>
<a href="https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-docker-micro-service" rel="nofollow">
 <img src="https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/distributions/docker.png?raw=true" alt="Docker" width="19%">
</a>
<a href="https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-local-micro-service" rel="nofollow">
 <img src="https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/distributions/binary.png?raw=true" alt="Binary" width="19%">
</a>
<a href="https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/user-guide-5.x/#using-siddhi-as-a-java-library" rel="nofollow">
 <img src="https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/distributions/java.png?raw=true" alt="Java" width="19%">
</a>
<a href="https://siddhi-io.github.io/siddhi/contribution/#obtaining-the-source-code-and-building-the-project" rel="nofollow">
 <img src="https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/distributions/source.png?raw=true" alt="Source" width="19%">
</a>

And more [installation options](https://siddhi-io.github.io/siddhi/download/) 

## Overview 

![](https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/siddhi-overview.png?raw=true "Overview")

**Siddhi supports:**
 
* **Streaming Data Integration** 
    * Retrieving data from various event sources (NATS, Kafka, JMS, HTTP, CDC, etc)
    * Map events to and from multiple event formats (JSON, XML, Text, Avro, etc)
    * Data preprocessing & cleaning
    * Joining multiple data streams 
    * Integrate streaming data with databases (RDBMS, Cassandra, HBase, Redis, etc)
    * Integrate with external services
    * Publish data to multiple event sinks (Email, JMS, HTTP, etc)
    
* **Streaming Data Analytics**
    * Generating alerts based on thresholds
    * Calculate aggregations over a short windows (time, length, session, unique, etc) or a long time period
    * Calculate aggregations over long time periods with seconds, minutes, hours, days, months & years granularity  
    * Correlating data while finding missing and erroneous events
    * Detecting temporal event patterns
    * Analyzing trends (rise, fall, turn, tipple bottom)
    * Run pretreated machine learning models (PMML, Tensorflow) 
    * Learn and predict at runtime using online machine learning models

* **Adaptive Intelligence**
    * Static rule processing 
    * Stateful rule processing 
    * Decision making through synchronous stream processing  
    * Query tables, windows and aggregations  
      
* And many more ...  For more information, see <a target="_blank" href="http://www.kdnuggets.com/2015/08/patterns-streaming-realtime-analytics.html">Patterns of Streaming Realtime Analytics</a>

Siddhi is free and open source, released under **Apache Software License v2.0**.

## Why use Siddhi ? 

* **Fast**. <a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a> uses it to process 20 Billion events per day (300,000 events per second). 
* **Lightweight** (core Siddhi libs are <2MB), and embeddable in Android, Python and RaspberryPi.
* Has **over 50 <a target="_blank" href="https://siddhi-io.github.io/siddhi/extensions/">Siddhi Extensions</a>**
* **Used by over 60 companies including many Fortune 500 companies** in production. Following are some examples:
    * **WSO2** uses Siddhi for the following purposes:
        * To provide **distributed and high available** stream processing capabilities via <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Stream Processor</a>. It is named as a strong performer in <a target="_blank" href="https://go.forrester.com/blogs/16-04-16-15_true_streaming_analytics_platforms_for_real_time_everything/">The Forrester Wave: Big Data Streaming Analytics, Q1 2016</a> (<a target="_blank" href="https://www.forrester.com/report/The+Forrester+Wave+Big+Data+Streaming+Analytics+Q1+2016/-/E-RES129023">Report</a>).
        * As the **edge analytics** library of [WSO2 IoT Server](http://wso2.com/iot?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17).
        * As the core of <a target="_blank" href="http://wso2.com/api-management?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 API Manager</a>'s throttling. 
        * As the core of <a target="_blank" href="http://wso2.com/platform?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 products'</a> analytics.
    * **<a target="_blank" href="http://wso2.com/library/conference/2017/2/wso2con-usa-2017-scalable-real-time-complex-event-processing-at-uber?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">UBER</a>** uses Siddhi for fraud analytics.
    * **<a target="_blank" href="http://eagle.apache.org/docs/index.html">Apache Eagle</a>** uses Siddhi as a policy engine.
    * Also used by [Punch Platform](https://doc.punchplatform.com/Reference_Guide/Data_Processing/Punch/Cep_Rules.html#siddhi_and_punch), [Sqooba](https://sqooba.io/), and [SiteWhere](https://sitewhere1.sitewhere.io/userguide/tenant/event-processing.html)
* Solutions based on Siddhi have been **finalists at <a target="_blank" href="http://dl.acm.org/results.cfm?query=(%252Bgrand%20%252Bchallenge%20%252Bwso2)&within=owners.owner=HOSTED&filtered=&dte=&bfr=">ACM DEBS Grand Challenge Stream Processing competitions** in 2014, 2015, 2016, 2017</a>.
* Siddhi has been **the basis of many academic research projects** and has <a target="_blank" href="https://scholar.google.com/scholar?cites=5113376427716987836&as_sdt=2005&sciodt=0,5&hl=en">**over 60 citations**</a>. 

If you are a Siddhi user, we would love to hear more on how you use Siddhi? Please share your experience and feedback via the [Siddhi user Google group](https://groups.google.com/forum/#!forum/siddhi-user).

## Get Started!

Get started with Siddhi in a few minutes by following the <a target="_blank" href="https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/quckstart-5.x/">Siddhi Quick Start Guide</a>

## Siddhi Development Environment 

**Siddhi Tooling**

Siddhi provides siddhi-tooling that supports following features to develop and test stream processing applications: 

* **Text Query Editor** with syntax highlighting and advanced auto completion support.
* **Event Simulator and Debugger** to test Siddhi Applications.
    ![](https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/editor/source-editor.png "Source Editor")

* **Graphical Query Editor** with drag and drop query building support.
    ![](https://raw.githubusercontent.com/siddhi-io/siddhi/master/docs/images/editor/graphical-editor.png "Graphical Query Editor")

**IntelliJ IDEA Plugin** 

Install <a target="_blank" href="https://siddhi-io.github.io/siddhi-plugin-idea/">IDEA plugin</a> to get the following features:

* **Siddhi Query Editor** with syntax highlighting and with basic auto completion
* **Siddhi Runner and Debugger** support to test Siddhi Application

## Siddhi Versions

 <a target="_blank" href=""></a> 
 
* **Active development version of Siddhi** : **v5.0.0**  _built on Java 8 & 11._ 
     
    Find the released Siddhi libraries <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi/">here</a>.

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

Latest API Docs is <a target="_blank" href="https://wso2.github.io/siddhi/api/5.0.0">5.0.0</a>.

## Contact us 
* Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
* For questions and feedback please connect via the [Siddhi user Google group](https://groups.google.com/forum/#!forum/siddhi-user).
* Engage in community development through [Siddhi dev Google group](https://groups.google.com/forum/#!forum/siddhi-dev). 

## How to Contribute
Find the detail information on asking questions, providing feedback, reporting issues, building and contributing code on [How to contribute?](https://siddhi-io.github.io/siddhi/contribution/) section.

## Roadmap 

- [x] Support Kafka
- [x] Support NATS
- [x] Siddhi Runner Distribution 
- [x] Siddhi Tooling (Editor)
- [x] Siddhi Kubernetes CRD
- [x] Periodic incremental state persistence  
- [ ] Support Prometheus for metrics collection
- [ ] Support high available Siddhi deployment with NATS via Kubernetes CRD
- [ ] Support distributed Siddhi deployment with NATS via Kubernetes CRD

## Support 
[WSO2](https://wso2.com/) provides production, and query support for Siddhi and its <a target="_blank" href="https://siddhi-io.github.io/siddhi/extensions/">extensions</a>. For more details contact via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>


Siddhi is joint research project initiated by <a target="_blank" href="http://wso2.com/">WSO2</a> and <a target="_blank" href="http://www.mrt.ac.lk/web/">University of Moratuwa</a>, Sri Lanka.
