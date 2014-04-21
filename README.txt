====================================================================================================
                        Siddhi Complex Event Processing Engine 1.0.0 M1
====================================================================================================

Siddhi CEP 1.0.0 - Milestone 1 Release Notes

Date : 9th July 2012

This milestone is available at : http://svn.wso2.org/repos/wso2/people/suho/siddhi/m1/siddhi-2.0.0.zip

This source code is available at : http://svn.wso2.org/repos/wso2/trunk/commons/siddhi2


Siddhi CEP is a lightweight, easy-to-use Open Source Complex Event Processing
Engine (CEP) under  Apache Software License v2.0. Siddhi CEP processes
events which are triggered by various event sources and notifies appropriate complex events
according to the user specified queries.

This project was started as a research project initiated at University of Moratuwa, Sri Lanka,
and now being improved by WSO2 Inc.

Note: This release is NOT compatible with older versions, and its a complete revamp.
 This release can be tested end to end.
 Please check out the samples located in the $SIDDHI_HOME/samples directory to try out its basic features.

Main highlights of this release
===============================

Rewriting Siddhi
- This is to support the full set of CEP fictionalises
- Making its architecture extensible

Defining new Siddhi CEP Language
- Improved logical language to support CEP functions

Features Supported
==================
 - Filter
    - Uses stream handlers to filter events
 - Join
    - Supports only upto two streams at a time
    - Match operation triggering can be configured (making "left" or "right" or both streams to trigger)
 - Aggregation
    - By default shipped with Avg, Sum , Min, Max
    - Supports Custom Aggregations via the plugable architecture
 - Group by
    - Supports Group by based on more than one attribute
    - Supported for all type if queries
 - Having
    - Supported for all type if queries
 - Stream handlers
    - Supports multiple handlers in a row per stream
    - By default shipped with  Filter and Window
    - Default implementations to windows are: Time window, Time Batch window, Length window
    - Supports Custom Stream handlers via the plugable architecture
 - Conditions and Expressions
    - Implemented from scratch
    - Mvel2 support removed
    - Conditions supported are: and, or, not, true/false, ==,!=, >=, >, <=, <
    - Expressions supported are: boolean, string, int, long, float, double
 - Pattern processing
    - Identifies pattern occurrences within streams
    - Supports "every" conditions
    - Can process two stream at a time via "and" and "or" conditions (currently only works on two simple streams)
    - Can collect events, with min and max limit, using "collect" condition (currently only works on a simple stream)
 - Sequence processing
    - Identifies continuous sequences with in streams
    - Supports "or" conditions on streams (currently only works on two simple streams)
    - Supports zero to many, one to many, and zero to one  (currently only works on a simple stream)
 - Query Language
    - Implemented on Antlr
    - Supports Query, Stream Definition and Query Plan compilation

System Requirements
===================

1. Minimum memory - 1 GB
2. Processor      - Pentium 800MHz or equivalent at minimum
3. Java SE Development Kit 1.6.0_21 or higher
4. To build Siddi CEP from the Source distribution, it is necessary that you have
   JDK 1.6 or higher version and Maven 2.1.0 or later


Support
=======

Support is provided by WSO2 Inc. dev@wso2.org


--------------------------------

We welcome your feedback and would love to hear your thoughts on this release of Siddhi.

Siddhi Team


