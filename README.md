Siddhi Complex Event Processing Engine 
======================================

---

|  Branch | Build Status |
| :------------ |:-------------
| master      | [![Build Status](https://wso2.org/jenkins/job/siddhi/badge/icon)](https://wso2.org/jenkins/job/siddhi) |

---

##### Latest Released Version v3.0.4.

Siddhi CEP is a lightweight, easy-to-use Open Source Complex Event Processing
Engine (CEP) under  Apache Software License v2.0. Siddhi CEP processes
events which are triggered by various event sources and notifies appropriate complex events
according to the user specified queries.

This project was started as a research project initiated at University of Moratuwa, Sri Lanka,
and now being improved by WSO2 Inc.


Features Supported
------------------
 - Filter
    - Uses stream handlers to filter events
 - Join
    - Supports only upto two streams at a time
    - Match operation triggering can be configured (making "left" or "right" or both streams to trigger)
 - Aggregation
    - By default shipped with Avg, Sum , Min, Max, etc
    - Supports Custom Aggregations via the plugable architecture
 - Group by
    - Supports Group by based on more than one attribute
    - Supported for all type if queries
 - Having
    - Supported for all type if queries
 - Stream handlers
    - Supports multiple handlers in a row per stream
    - By default shipped with  Filter and Window
    - Default implementations to windows are: Time window, Time Batch window, Length window, etc
    - Supports Custom Stream handlers via the plugable architecture
 - Conditions and Expressions
    - Supporitng condition and expression evalutation
    - Conditions supported are: and, or, not, ==,!=, >=, >, <=, <, and arithmetic operations
    - Atributes supported are: boolean, string, int, long, float, double, object
 - Pattern processing
    - Identifies pattern occurrences within streams
    - Supports "every" conditions
    - Can process two stream at a time via "and" and "or" conditions (currently only works on two simple streams)
    - Can collect events, with min and max limit, using "collect" condition (currently only works on a simple stream)
 - Sequence processing
    - Identifies continuous sequences with in streams
    - Supports "or" conditions on streams (currently only works on two simple streams)
    - Supports zero to many, one to many, and zero to one  (currently only works on a simple stream)
 - Event Tables
    - Support for using historical data in realtime processing
    - Can process with the in-memory or RDBMS based data collection
 - Query Language
    - SQL like query languege 
    - Implemented on Antlr
    - Supports Query, Stream Definition and Query Plan compilation

System Requirements
-------------------

1. Minimum memory - 1 GB
2. Processor      - Pentium 800MHz or equivalent at minimum
3. Java SE Development Kit 1.7 or higher
4. To build Siddhi CEP from the Source distribution, it is necessary that you have
   JDK 1.7 version or later and Maven 3.0.4 or later

## How to Contribute
* Please report issues at [CEP JIRA] (https://wso2.org/jira/browse/CEP)
* Send your bug fixes pull requests to [master branch] (https://github.com/wso2/carbon-event-processing/tree/master) 

## Contact us
WSO2 Carbon developers can be contacted via the mailing lists:

* Carbon Developers List : dev@wso2.org
* Carbon Architecture List : architecture@wso2.org

####We welcome your feedback and contribution.

Siddhi CEP Team


