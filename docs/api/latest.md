# API Docs - v5.1.31

## Core

### and *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the results of AND operation for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> and(<BOOL> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be AND operation.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from cscStream#window.lengthBatch(10)
select and(isFraud) as isFraudTransaction
insert into alertStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the result for AND operation of isFraud values as a boolean value for event chunk expiry by window length batch.</p>
<p></p>
### avg *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Calculates the average for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<DOUBLE> avg(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that need to be averaged.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream#window.timeBatch
 select avg(temp) as avgTemp
 insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">avg(temp) returns the average temp value for all the events based on their arrival and expiry.</p>
<p></p>
### count *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the count of all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<LONG> count()
<LONG> count(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This function accepts one parameter. It can belong to any one of the available types.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream#window.timeBatch(10 sec)
select count() as count
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return the count of all the events for time batch in 10 seconds.</p>
<p></p>
### distinctCount *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This returns the count of distinct occurrences for a given arg.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<LONG> distinctCount(<INT|LONG|DOUBLE|FLOAT|STRING> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The object for which the number of distinct occurences needs to be counted.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select distinctcount(pageID) as count
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">distinctcount(pageID) for the following output returns '3' when the available values are as follows.<br>&nbsp;"WEB_PAGE_1"<br>&nbsp;"WEB_PAGE_1"<br>&nbsp;"WEB_PAGE_2"<br>&nbsp;"WEB_PAGE_3"<br>&nbsp;"WEB_PAGE_1"<br>&nbsp;"WEB_PAGE_2"<br>&nbsp;The three distinct occurences identified are 'WEB_PAGE_1', 'WEB_PAGE_2', and 'WEB_PAGE_3'.</p>
<p></p>
### max *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the maximum value for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT> max(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be compared to find the maximum value.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream#window.timeBatch(10 sec)
select max(temp) as maxTemp
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">max(temp) returns the maximum temp value recorded for all the events based on their arrival and expiry.</p>
<p></p>
### maxForever *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is the attribute aggregator to store the maximum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT> maxForever(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be compared to find the maximum value.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from inputStream
select maxForever(temp) as max
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">maxForever(temp) returns the maximum temp value recorded for all the events throughout the lifetime of the query.</p>
<p></p>
### min *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the minimum value for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT> min(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be compared to find the minimum value.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from inputStream
select min(temp) as minTemp
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">min(temp) returns the minimum temp value recorded for all the events based on their arrival and expiry.</p>
<p></p>
### minForever *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is the attribute aggregator to store the minimum value for a given attribute throughout the lifetime of the query regardless of any windows in-front.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT> minForever(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be compared to find the minimum value.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from inputStream
select minForever(temp) as max
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">minForever(temp) returns the minimum temp value recorded for all the events throughoutthe lifetime of the query.</p>
<p></p>
### or *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the results of OR operation for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> or(<BOOL> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be OR operation.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from cscStream#window.lengthBatch(10)
select or(isFraud) as isFraudTransaction
insert into alertStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the result for OR operation of isFraud values as a boolean value for event chunk expiry by window length batch.</p>
<p></p>
### stdDev *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the calculated standard deviation for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<DOUBLE> stdDev(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that should be used to calculate the standard deviation.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from inputStream
select stddev(temp) as stdTemp
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">stddev(temp) returns the calculated standard deviation of temp for all the events based on their arrival and expiry.</p>
<p></p>
### sum *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the sum for all the events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<LONG|DOUBLE> sum(<INT|LONG|DOUBLE|FLOAT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The value that needs to be summed.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from inputStream
select sum(volume) as sumOfVolume
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the sum of volume values as a long value for each event arrival and expiry.</p>
<p></p>
### unionSet *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#aggregate-function">(Aggregate Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Union multiple sets. <br>&nbsp;This attribute aggregator maintains a union of sets. The given input set is put into the union set and the union set is returned.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<OBJECT> unionSet(<OBJECT> set)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">set</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The java.util.Set object that needs to be added into the union set.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from stockStream 
select createSet(symbol) as initialSet 
insert into initStream 

from initStream#window.timeBatch(10 sec) 
select unionSet(initialSet) as distinctSymbols 
insert into distinctStockStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">distinctStockStream will return the set object which contains the distinct set of stock symbols received during a sliding window of 10 seconds.</p>
<p></p>
### UUID *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Generates a UUID (Universally Unique Identifier).</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<STRING> UUID()
```

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from TempStream
select convert(roomNo, 'string') as roomNo, temp, UUID() as messageID
insert into RoomTempStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will converts a room number to string, introducing a message ID to each event asUUID() returns a34eec40-32c2-44fe-8075-7f4fde2e2dd8<br><br>from TempStream<br>select convert(roomNo, 'string') as roomNo, temp, UUID() as messageID<br>insert into RoomTempStream;</p>
<p></p>
### cast *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Converts the first parameter according to the cast.to parameter. Incompatible arguments cause Class Cast exceptions if further processed. This function is used with map extension that returns attributes of the object type. You can use this function to cast the object to an accurate and concrete type.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> cast(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> to.be.caster, <STRING> cast.to)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">to.be.caster</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the attribute to be casted.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">cast.to</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">A string constant parameter expressing the cast to type using one of the following strings values: int, long, float, double, string, bool.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select symbol as name, cast(temp, 'double') as temp
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will cast the fooStream temp field value into 'double' format.</p>
<p></p>
### coalesce *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the value of the first input parameter that is not null, and all input parameters have to be on the same type.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> coalesce(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg, <INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This function accepts one or more parameters. They can belong to any one of the available types. All the specified parameters should be of the same type.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select coalesce('123', null, '789') as value
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns first null value 123.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select coalesce(null, 76, 567) as value
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns first null value 76.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
from fooStream
select coalesce(null, null, null) as value
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns null as there are no notnull values.</p>
<p></p>
### convert *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Converts the first input parameter according to the convertedTo parameter.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT|STRING|BOOL> convert(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> to.be.converted, <STRING> converted.to)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">to.be.converted</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the value to be converted.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">converted.to</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">A string constant parameter to which type the attribute need to be converted  using one of the following strings values: 'int', 'long', 'float', 'double', 'string', 'bool'.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select convert(temp, 'double') as temp
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will convert fooStream temp value into 'double'.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select convert(temp, 'int') as temp
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will convert fooStream temp value into 'int' (value = "convert(45.9, 'int') returns 46").</p>
<p></p>
### createSet *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Includes the given input parameter in a java.util.HashSet and returns the set. </p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<OBJECT> createSet(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL> input)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">input</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The input that needs to be added into the set.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from stockStream 
select createSet(symbol) as initialSet 
insert into initStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">For every incoming stockStream event, the initStream stream will produce a set object having only one element: the symbol in the incoming stockStream.</p>
<p></p>
### currentTimeMillis *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the current timestamp of siddhi application in milliseconds.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<LONG> currentTimeMillis()
```

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select symbol as name, currentTimeMillis() as eventTimestamp 
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will extract current siddhi application timestamp.</p>
<p></p>
### default *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks if the 'attribute' parameter is null and if so returns the value of the 'default' parameter</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> default(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> attribute, <INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> default)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">attribute</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The attribute that could be null.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">default</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The default value that will be used when 'attribute' parameter is null</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from TempStream
select default(temp, 0.0) as temp, roomNum
insert into StandardTempStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will replace TempStream's temp attribute with default value if the temp is null.</p>
<p></p>
### eventTimestamp *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the timestamp of the processed/passed event.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<LONG> eventTimestamp()
<LONG> eventTimestamp(<OBJECT> event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">event</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Event reference.</p></td>
        <td style="vertical-align: top">Current Event</td>
        <td style="vertical-align: top">OBJECT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from FooStream
select symbol as name, eventTimestamp() as eventTimestamp 
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Extracts current event's timestamp.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from FooStream as f join FooBarTable as fb
select fb.symbol as name, eventTimestamp(f) as eventTimestamp 
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Extracts FooStream event's timestamp.</p>
<p></p>
### ifThenElse *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Evaluates the 'condition' parameter and returns value of the 'if.expression' parameter if the condition is true, or returns value of the 'else.expression' parameter if the condition is false. Here both 'if.expression' and 'else.expression' should be of the same type.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> ifThenElse(<BOOL> condition, <INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> if.expression, <INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> else.expression)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">condition</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the if then else condition value.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">if.expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the value to be returned if the value of the condition parameter is true.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">else.expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies the value to be returned if the value of the condition parameter is false.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@info(name = 'query1')
from sensorEventStream
select sensorValue, ifThenElse(sensorValue>35,'High','Low') as status
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns High if sensorValue = 50.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@info(name = 'query1')
from sensorEventStream
select sensorValue, ifThenElse(voltage < 5, 0, 1) as status
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns 1 if voltage= 12.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@info(name = 'query1')
from userEventStream
select userName, ifThenElse(password == 'admin', true, false) as passwordState
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns  passwordState as true if password = admin.</p>
<p></p>
### instanceOfBoolean *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the parameter is an instance of Boolean or not.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> instanceOfBoolean(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The parameter to be checked.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select instanceOfBoolean(switchState) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return true if the value of switchState is true.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select instanceOfBoolean(value) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">if the value = 32 then this will returns false as the value is not an instance of the boolean.</p>
<p></p>
### instanceOfDouble *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the parameter is an instance of Double or not.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> instanceOfDouble(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The parameter to be checked.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select instanceOfDouble(value) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return true if the value field format is double ex : 56.45.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select instanceOfDouble(switchState) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">if the switchState = true then this will returns false as the value is not an instance of the double.</p>
<p></p>
### instanceOfFloat *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the parameter is an instance of Float or not.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> instanceOfFloat(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The parameter to be checked.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select instanceOfFloat(value) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return true if the value field format is float ex : 56.45f.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select instanceOfFloat(switchState) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">if the switchState = true then this will returns false as the value is an instance of the boolean not a float.</p>
<p></p>
### instanceOfInteger *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the parameter is an instance of Integer or not.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> instanceOfInteger(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The parameter to be checked.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select instanceOfInteger(value) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return true if the value field format is integer.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select instanceOfInteger(switchState) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">if the switchState = true then this will returns false as the value is an instance of the boolean not a long.</p>
<p></p>
### instanceOfLong *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the parameter is an instance of Long or not.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> instanceOfLong(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The parameter to be checked.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select instanceOfLong(value) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return true if the value field format is long ex : 56456l.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select instanceOfLong(switchState) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">if the switchState = true then this will returns false as the value is an instance of the boolean not a long.</p>
<p></p>
### instanceOfString *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Checks whether the parameter is an instance of String or not.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<BOOL> instanceOfString(<INT|LONG|DOUBLE|FLOAT|STRING|BOOL|OBJECT> arg)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The parameter to be checked.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT<br>STRING<br>BOOL<br>OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from fooStream
select instanceOfString(value) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return true if the value field format is string ex : 'test'.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from fooStream
select instanceOfString(switchState) as state
insert into barStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">if the switchState = true then this will returns false as the value is an instance of the boolean not a string.</p>
<p></p>
### maximum *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the maximum value of the input parameters.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT> maximum(<INT|LONG|DOUBLE|FLOAT> arg, <INT|LONG|DOUBLE|FLOAT> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This function accepts one or more parameters. They can belong to any one of the available types. All the specified parameters should be of the same type.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@info(name = 'query1') from inputStream
select maximum(price1, price2, price3) as max
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the maximum value of the input parameters price1, price2, price3.</p>
<p></p>
### minimum *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the minimum value of the input parameters.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT|LONG|DOUBLE|FLOAT> minimum(<INT|LONG|DOUBLE|FLOAT> arg, <INT|LONG|DOUBLE|FLOAT> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">arg</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This function accepts one or more parameters. They can belong to any one of the available types. All the specified parameters should be of the same type.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>DOUBLE<br>FLOAT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@info(name = 'query1') from inputStream
select maximum(price1, price2, price3) as max
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the minimum value of the input parameters price1, price2, price3.</p>
<p></p>
### sizeOfSet *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#function">(Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Returns the size of an object of type java.util.Set.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
<INT> sizeOfSet(<OBJECT> set)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">set</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The set object. This parameter should be of type java.util.Set. A set object may be created by the 'set' attribute aggregator in Siddhi. </p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">OBJECT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from stockStream 
select initSet(symbol) as initialSet 
insert into initStream; 

;from initStream#window.timeBatch(10 sec) 
select union(initialSet) as distinctSymbols 
insert into distinctStockStream; 

from distinctStockStream 
select sizeOfSet(distinctSymbols) sizeOfSymbolSet 
insert into sizeStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">The sizeStream stream will output the number of distinct stock symbols received during a sliding window of 10 seconds.</p>
<p></p>
### pol2Cart *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-function">(Stream Function)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The pol2Cart function calculating the cartesian coordinates x & y for the given theta, rho coordinates and adding them as new attributes to the existing events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
pol2Cart(<DOUBLE> theta, <DOUBLE> rho)
pol2Cart(<DOUBLE> theta, <DOUBLE> rho, <DOUBLE> z)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">theta</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The theta value of the coordinates.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">rho</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The rho value of the coordinates.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">z</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">z value of the cartesian coordinates.</p></td>
        <td style="vertical-align: top">If z value is not given, drop the third parameter of the output.</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from PolarStream#pol2Cart(theta, rho)
select x, y 
insert into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return cartesian coordinates (4.99953024681082, 0.06853693328228748) for theta: 0.7854 and rho: 5.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from PolarStream#pol2Cart(theta, rho, 3.4)
select x, y, z 
insert into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will return cartesian coordinates (4.99953024681082, 0.06853693328228748, 3.4)for theta: 0.7854 and rho: 5 and z: 3.4.</p>
<p></p>
### log *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#stream-processor">(Stream Processor)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Logs the message on the given priority with or without the processed event.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
log()
log(<STRING> log.message)
log(<BOOL> is.event.logged)
log(<STRING> log.message, <BOOL> is.event.logged)
log(<STRING> priority, <STRING> log.message)
log(<STRING> priority, <STRING> log.message, <BOOL> is.event.logged)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">priority</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The priority/type of this log message (INFO, DEBUG, WARN, FATAL, ERROR, OFF, TRACE).</p></td>
        <td style="vertical-align: top">INFO</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">log.message</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This message will be logged.</p></td>
        <td style="vertical-align: top"><siddhi app name> :</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">is.event.logged</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">To log the processed event.</p></td>
        <td style="vertical-align: top">true</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
from FooStream#log()
select *
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Logs events with SiddhiApp name message prefix on default log level INFO.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
from FooStream#log("Sample Event :")
select *
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Logs events with the message prefix "Sample Event :" on default log level INFO.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
from FooStream#log("DEBUG", "Sample Event :", true)
select *
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Logs events with the message prefix "Sample Event :" on log level DEBUG.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
from FooStream#log("Event Arrived", false)
select *
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">For each event logs a message "Event Arrived" on default log level INFO.</p>
<p></p>
<span id="example-5" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 5</span>
```
from FooStream#log("Sample Event :", true)
select *
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Logs events with the message prefix "Sample Event :" on default log level INFO.</p>
<p></p>
<span id="example-6" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 6</span>
```
from FooStream#log(true)
select *
insert into BarStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Logs events with on default log level INFO.</p>
<p></p>
### batch *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A window that holds an incoming events batch. When a new set of events arrives, the previously arrived old events will be expired. Batch window can be used to aggregate events that comes in batches. If it has the parameter length specified, then batch window process the batch as several chunks.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
batch()
batch(<INT> window.length)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.length</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The length of a chunk</p></td>
        <td style="vertical-align: top">If length value was not given it assign 0 as length and process the whole batch as once</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream consumerItemStream (itemId string, price float)

from consumerItemStream#window.batch()
select price, str:groupConcat(itemId) as itemIds
group by price
insert into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will output comma separated items IDs that have the same price for each incoming batch of events.</p>
<p></p>
### cron *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This window outputs the arriving events as and when they arrive, and resets (expires) the window periodically based on the given cron expression.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
cron(<STRING> cron.expression)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">cron.expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The cron expression that resets the window.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream InputEventStream (symbol string, price float, volume int);

@info(name = 'query1')
from InputEventStream#cron('*/5 * * * * ?')
select symbol, sum(price) as totalPrice 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This let the totalPrice to gradually increase and resets to zero as a batch every 5 seconds.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream StockEventStream (symbol string, price float, volume int)
define window StockEventWindow (symbol string, price float, volume int) cron('*/5 * * * * ?');

@info(name = 'query0')
from StockEventStream
insert into StockEventWindow;

@info(name = 'query1')
from StockEventWindow 
select symbol, sum(price) as totalPrice
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">The defined window will let the totalPrice to gradually increase and resets to zero as a batch every 5 seconds.</p>
<p></p>
### delay *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A delay window holds events for a specific time period that is regarded as a delay period before processing them.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
delay(<INT|LONG|TIME> window.delay)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.delay</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time period (specified in sec, min, ms) for which  the window should delay the events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define window delayWindow(symbol string, volume int) delay(1 hour);
define stream PurchaseStream(symbol string, volume int);
define stream DeliveryStream(symbol string);
define stream OutputStream(symbol string);

@info(name='query1') 
from PurchaseStream
select symbol, volume
insert into delayWindow;

@info(name='query2') 
from delayWindow join DeliveryStream
on delayWindow.symbol == DeliveryStream.symbol
select delayWindow.symbol
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, purchase events that arrive in the 'PurchaseStream' stream are directed to a delay window. At any given time, this delay window holds purchase events that have arrived within the last hour. These purchase events in the window are matched by the 'symbol' attribute, with delivery events that arrive in the 'DeliveryStream' stream. This monitors whether the delivery of products is done with a minimum delay of one hour after the purchase.</p>
<p></p>
### expression *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A sliding window that dynamically shrink and grow based on the <code>expression</code>, it holds events that satisfies the given <code>expression</code>, when they aren't, they are evaluated from the <code>first</code> (oldest) to the <code>last</code> (latest/current) and expired from the oldest until the <code>expression</code> is satisfied.<br>**Note**: All the events in window are reevaluated only when the given <code>expression</code> is changed.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
expression(<STRING> expression)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The expression to retain events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@info(name = 'query1')
from StockEventWindow#window.expression('count()<=20')
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will retain last 20 events in a sliding manner.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@info(name = 'query1')
from StockEventWindow#window.expression(
       'sum(price) < 100 and eventTimestamp(last) - eventTimestamp(first) < 3000')
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will retain the latest events having their sum(price) &lt; 100, and the <code>last</code> and <code>first</code> events are within 3 second difference.</p>
<p></p>
### expressionBatch *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A batch window that dynamically shrink and grow based on the <code>expression</code>, it holds events until the <code>expression</code> is satisfied, and expires all when the <code>expression</code> is not satisfied.When a string is passed as the <code>expression</code> it is evaluated from the <code>first</code> (oldest) to the <code>last</code> (latest/current).<br>**Note**: All the events in window are reevaluated only when the given <code>expression</code> is changed.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
expressionBatch(<STRING|BOOL> expression)
expressionBatch(<STRING|BOOL> expression, <BOOL> include.triggering.event)
expressionBatch(<STRING|BOOL> expression, <BOOL> include.triggering.event, <BOOL> stream.current.event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The expression to retain events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING<br>BOOL</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">include.triggering.event</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Include the event triggered the expiry in to the current event batch.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">stream.current.event</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Let the window stream the current events out as and when they arrive to the window while expiring them in batches.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@info(name = 'query1')
from StockEventWindow#window.expressionBatch('count()<=20')
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Retain and output 20 events at a time as batch.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@info(name = 'query1')
from StockEventWindow#window.expressionBatch(
       'sum(price) < 100 and eventTimestamp(last) - eventTimestamp(first) < 3000')
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Retain and output events having their sum(price) &lt; 100, and the <code>last</code> and <code>first</code> events are within 3 second difference as a batch.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@info(name = 'query1')
from StockEventWindow#window.expressionBatch(
       'last.symbol==first.symbol')
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Output events as a batch when a new symbol type arrives.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
@info(name = 'query1')
from StockEventWindow#window.expressionBatch(
       'flush', true)
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Output events as a batch when a flush attribute becomes <code>true</code>, the output batch will also contain the triggering event.</p>
<p></p>
<span id="example-5" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 5</span>
```
@info(name = 'query1')
from StockEventWindow#window.expressionBatch(
       'flush', false, true)
select symbol, sum(price) as price
insert into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Arriving events are emitted as soon as they are arrived, and the retained events are expired when flush attribute becomes <code>true</code>, and the output batch will not contain the triggering event.</p>
<p></p>
### externalTime *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A sliding time window based on external time. It holds events that arrived during the last windowTime period from the external timestamp, and gets updated on every monotonically increasing timestamp.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
externalTime(<LONG> timestamp, <INT|LONG|TIME> window.time)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">timestamp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time which the window determines as current time and will act upon. The value of this parameter should be monotonically increasing.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">window.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The sliding time period for which the window should hold events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define window cseEventWindow (symbol string, price float, volume int) externalTime(eventTime, 20 sec) output expired events;

@info(name = 'query0')
from cseEventStream
insert into cseEventWindow;

@info(name = 'query1')
from cseEventWindow
select symbol, sum(price) as price
insert expired events into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">processing events arrived within the last 20 seconds from the eventTime and output expired events.</p>
<p></p>
### externalTimeBatch *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A batch (tumbling) time window based on external time, that holds events arrived during windowTime periods, and gets updated for every windowTime.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
externalTimeBatch(<LONG> timestamp, <INT|LONG|TIME> window.time)
externalTimeBatch(<LONG> timestamp, <INT|LONG|TIME> window.time, <INT|LONG|TIME> start.time)
externalTimeBatch(<LONG> timestamp, <INT|LONG|TIME> window.time, <INT|LONG|TIME> start.time, <INT|LONG|TIME> timeout)
externalTimeBatch(<LONG> timestamp, <INT|LONG|TIME> window.time, <INT|LONG|TIME> start.time, <INT|LONG|TIME> timeout, <BOOL> replace.with.batchtime)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">timestamp</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time which the window determines as current time and will act upon. The value of this parameter should be monotonically increasing.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">window.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The batch time period for which the window should hold events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">start.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">User defined start time. This could either be a constant (of type int, long or time) or an attribute of the corresponding stream (of type long). If an attribute is provided, initial value of attribute would be considered as startTime.</p></td>
        <td style="vertical-align: top">Timestamp of first event</td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">timeout</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Time to wait for arrival of new event, before flushing and giving output for events belonging to a specific batch.</p></td>
        <td style="vertical-align: top">System waits till an event from next batch arrives to flush current batch</td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">replace.with.batchtime</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This indicates to replace the expired event timeStamp as the batch end timeStamp</p></td>
        <td style="vertical-align: top">System waits till an event from next batch arrives to flush current batch</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define window cseEventWindow (symbol string, price float, volume int) externalTimeBatch(eventTime, 1 sec) output expired events;
@info(name = 'query0')
from cseEventStream
insert into cseEventWindow;
@info(name = 'query1')
from cseEventWindow
select symbol, sum(price) as price
insert expired events into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will processing events that arrive every 1 seconds from the eventTime.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define window cseEventWindow (symbol string, price float, volume int) externalTimeBatch(eventTime, 20 sec, 0) output expired events;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will processing events that arrive every 1 seconds from the eventTime. Starts on 0th millisecond of an hour.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
define window cseEventWindow (symbol string, price float, volume int) externalTimeBatch(eventTime, 2 sec, eventTimestamp, 100) output expired events;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will processing events that arrive every 2 seconds from the eventTim. Considers the first event's eventTimestamp value as startTime. Waits 100 milliseconds for the arrival of a new event before flushing current batch.</p>
<p></p>
### <s>frequent *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*</s>
<p><i>Deprecated</i></p>
<p></p>
<p style="word-wrap: break-word;margin: 0;">This window returns the latest events with the most frequently occurred value for a given attribute(s). Frequency calculation for this window processor is based on Misra-Gries counting algorithm.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
frequent(<INT> event.count)
frequent(<INT> event.count, <STRING> attribute)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">event.count</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of most frequent events to be emitted to the stream.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">attribute</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The attributes to group the events. If no attributes are given, the concatenation of all the attributes of the event is considered.</p></td>
        <td style="vertical-align: top">The concatenation of all the attributes of the event is considered.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@info(name = 'query1')
from purchase[price >= 30]#window.frequent(2)
select cardNo, price
insert all events into PotentialFraud;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the 2 most frequent events.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@info(name = 'query1')
from purchase[price >= 30]#window.frequent(2, cardNo)
select cardNo, price
insert all events into PotentialFraud;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will returns the 2 latest events with the most frequently appeared card numbers.</p>
<p></p>
### length *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A sliding length window that holds the last 'window.length' events at a given time, and gets updated for each arrival and expiry.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
length(<INT> window.length)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.length</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of events that should be included in a sliding length window.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define window StockEventWindow (symbol string, price float, volume int) length(10) output all events;

@info(name = 'query0')
from StockEventStream
insert into StockEventWindow;
@info(name = 'query1')

from StockEventWindow
select symbol, sum(price) as price
insert all events into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will process last 10 events in a sliding manner.</p>
<p></p>
### lengthBatch *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A batch (tumbling) length window that holds and process a number of events as specified in the window.length.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
lengthBatch(<INT> window.length)
lengthBatch(<INT> window.length, <BOOL> stream.current.event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.length</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of events the window should tumble.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">stream.current.event</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Let the window stream the current events out as and when they arrive to the window while expiring them in batches.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream InputEventStream (symbol string, price float, volume int);

@info(name = 'query1')
from InputEventStream#lengthBatch(10)
select symbol, sum(price) as price 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This collect and process 10 events as a batch and output them.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream InputEventStream (symbol string, price float, volume int);

@info(name = 'query1')
from InputEventStream#lengthBatch(10, true)
select symbol, sum(price) as sumPrice 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This window sends the arriving events directly to the output letting the <code>sumPrice</code> to increase gradually, after every 10 events it clears the window as a batch and resets the <code>sumPrice</code> to zero.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
define stream InputEventStream (symbol string, price float, volume int);
define window StockEventWindow (symbol string, price float, volume int) lengthBatch(10) output all events;

@info(name = 'query0')
from InputEventStream
insert into StockEventWindow;

@info(name = 'query1')
from StockEventWindow
select symbol, sum(price) as price
insert all events into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This uses an defined window to process 10 events  as a batch and output all events.</p>
<p></p>
### <s>lossyFrequent *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*</s>
<p><i>Deprecated</i></p>
<p></p>
<p style="word-wrap: break-word;margin: 0;">This window identifies and returns all the events of which the current frequency exceeds the value specified for the supportThreshold parameter.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
lossyFrequent(<DOUBLE> support.threshold)
lossyFrequent(<DOUBLE> support.threshold, <DOUBLE> error.bound)
lossyFrequent(<DOUBLE> support.threshold, <DOUBLE> error.bound, <STRING> attribute)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">support.threshold</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The support threshold value.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">error.bound</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The error bound value.</p></td>
        <td style="vertical-align: top">`support.threshold`/10</td>
        <td style="vertical-align: top">DOUBLE</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">attribute</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The attributes to group the events. If no attributes are given, the concatenation of all the attributes of the event is considered.</p></td>
        <td style="vertical-align: top">The concatenation of all the attributes of the event is considered.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream purchase (cardNo string, price float);
define window purchaseWindow (cardNo string, price float) lossyFrequent(0.1, 0.01);
@info(name = 'query0')
from purchase[price >= 30]
insert into purchaseWindow;
@info(name = 'query1')
from purchaseWindow
select cardNo, price
insert all events into PotentialFraud;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">lossyFrequent(0.1, 0.01) returns all the events of which the current frequency exceeds 0.1, with an error bound of 0.01.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream purchase (cardNo string, price float);
define window purchaseWindow (cardNo string, price float) lossyFrequent(0.3, 0.05, cardNo);
@info(name = 'query0')
from purchase[price >= 30]
insert into purchaseWindow;
@info(name = 'query1')
from purchaseWindow
select cardNo, price
insert all events into PotentialFraud;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">lossyFrequent(0.3, 0.05, cardNo) returns all the events of which the cardNo attributes frequency exceeds 0.3, with an error bound of 0.05.</p>
<p></p>
### session *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Holds events that belong to a session. Events belong to a specific session are identified by a session key, and a session gap is determines the time period after which the session is considered to be expired. To have meaningful aggregation on session windows, the events need to be aggregated based on session key via a <code>group by</code> clause.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
session(<INT|LONG|TIME> session.gap)
session(<INT|LONG|TIME> session.gap, <STRING> session.key)
session(<INT|LONG|TIME> session.gap, <STRING> session.key, <INT|LONG|TIME> allowed.latency)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">session.gap</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time period after which the session is considered to be expired.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">session.key</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The session identification attribute. Used to group events belonging to a specific session.</p></td>
        <td style="vertical-align: top">default-key</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">allowed.latency</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time period for which the session window is valid after the expiration of the session, to accept late event arrivals. This time period should be less than the <code>session.gap</code> parameter.</p></td>
        <td style="vertical-align: top">0</td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream PurchaseEventStream (user string, item_number int, price float, quantity int);

@info(name='query1) 
from PurchaseEventStream#window.session(5 sec, user) 
select user, sum(quantity) as totalQuantity, sum(price) as totalPrice 
group by user 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">From the events arriving at the PurchaseEventStream, a session window with 5 seconds session gap is processed based on 'user' attribute as the session group identification key. All events falling into the same session are aggregated based on <code>user</code> attribute, and outputted to the OutputStream.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream PurchaseEventStream (user string, item_number int, price float, quantity int);

@info(name='query2) 
from PurchaseEventStream#window.session(5 sec, user, 2 sec) 
select user, sum(quantity) as totalQuantity, sum(price) as totalPrice 
group by user 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">From the events arriving at the PurchaseEventStream, a session window with 5 seconds session gap is processed based on 'user' attribute as the session group identification key. This session window is kept active for 2 seconds after the session expiry to capture late (out of order) event arrivals. If the event timestamp falls in to the last session the session is reactivated. Then all events falling into the same session are aggregated based on <code>user</code> attribute, and outputted to the OutputStream.</p>
<p></p>
### sort *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This window holds a batch of events that equal the number specified as the windowLength and sorts them in the given order.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
sort(<INT> window.length, <STRING|DOUBLE|INT|LONG|FLOAT|LONG> attribute)
sort(<INT> window.length, <STRING|DOUBLE|INT|LONG|FLOAT|LONG> attribute, <STRING> order, <STRING> ...)
sort(<INT> window.length, <STRING|DOUBLE|INT|LONG|FLOAT|LONG> attribute, <STRING> order, <STRING|DOUBLE|INT|LONG|FLOAT|LONG> attribute, <STRING|DOUBLE|INT|LONG|FLOAT|LONG> ...)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.length</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The size of the window length.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">attribute</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The attribute that should be checked for the order.</p></td>
        <td style="vertical-align: top">The concatenation of all the attributes of the event is considered.</td>
        <td style="vertical-align: top">STRING<br>DOUBLE<br>INT<br>LONG<br>FLOAT<br>LONG</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">order</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The order define as "asc" or "desc".</p></td>
        <td style="vertical-align: top">asc</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream cseEventStream (symbol string, price float, volume long);
define window cseEventWindow (symbol string, price float, volume long) sort(2,volume, 'asc');
@info(name = 'query0')
from cseEventStream
insert into cseEventWindow;
@info(name = 'query1')
from cseEventWindow
select volume
insert all events into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">sort(5, price, 'asc') keeps the events sorted by price in the ascending order. Therefore, at any given time, the window contains the 5 lowest prices.</p>
<p></p>
### time *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A sliding time window that holds events that arrived during the last windowTime period at a given time, and gets updated for each event arrival and expiry.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
time(<INT|LONG|TIME> window.time)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The sliding time period for which the window should hold events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define window cseEventWindow (symbol string, price float, volume int) time(20) output all events;
@info(name = 'query0')
from cseEventStream
insert into cseEventWindow;
@info(name = 'query1')
from cseEventWindow
select symbol, sum(price) as price
insert all events into outputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This will processing events that arrived within the last 20 milliseconds.</p>
<p></p>
### timeBatch *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A batch (tumbling) time window that holds and process events that arrive during 'window.time' period as a batch.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
timeBatch(<INT|LONG|TIME> window.time)
timeBatch(<INT|LONG|TIME> window.time, <INT|LONG> start.time)
timeBatch(<INT|LONG|TIME> window.time, <BOOL> stream.current.event)
timeBatch(<INT|LONG|TIME> window.time, <INT|LONG> start.time, <BOOL> stream.current.event)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The batch time period in which the window process the events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">start.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This specifies an offset in milliseconds in order to start the window at a time different to the standard time.</p></td>
        <td style="vertical-align: top">Timestamp of first event</td>
        <td style="vertical-align: top">INT<br>LONG</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">stream.current.event</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Let the window stream the current events out as and when they arrive to the window while expiring them in batches.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream InputEventStream (symbol string, price float, volume int);

@info(name = 'query1')
from InputEventStream#timeBatch(20 sec)
select symbol, sum(price) as price 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This collect and process incoming events as a batch every 20 seconds and output them.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
define stream InputEventStream (symbol string, price float, volume int);

@info(name = 'query1')
from InputEventStream#timeBatch(20 sec, true)
select symbol, sum(price) as sumPrice 
insert into OutputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This window sends the arriving events directly to the output letting the <code>sumPrice</code> to increase gradually and on every 20 second interval it clears the window as a batch resetting the <code>sumPrice</code> to zero.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
define stream InputEventStream (symbol string, price float, volume int);
define window StockEventWindow (symbol string, price float, volume int) timeBatch(20 sec) output all events;

@info(name = 'query0')
from InputEventStream
insert into StockEventWindow;

@info(name = 'query1')
from StockEventWindow
select symbol, sum(price) as price
insert all events into OutputStream ;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">This uses an defined window to process events arrived every 20 seconds as a batch and output all events.</p>
<p></p>
### timeLength *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#window">(Window)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">A sliding time window that, at a given time holds the last window.length events that arrived during last window.time period, and gets updated for every event arrival and expiry.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
timeLength(<INT|LONG|TIME> window.time, <INT> window.length)
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">window.time</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The sliding time period for which the window should hold events.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT<br>LONG<br>TIME</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">window.length</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The number of events that should be be included in a sliding length window..</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream cseEventStream (symbol string, price float, volume int);
define window cseEventWindow (symbol string, price float, volume int) timeLength(2 sec, 10);
@info(name = 'query0')
from cseEventStream
insert into cseEventWindow;
@info(name = 'query1')
from cseEventWindow select symbol, price, volume
insert all events into outputStream;
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">window.timeLength(2 sec, 10) holds the last 10 events that arrived during last 2 seconds and gets updated for every event arrival and expiry.</p>
<p></p>
## Sink

### inMemory *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">In-memory sink publishes events to In-memory sources that are subscribe to the same topic to which the sink publishes. This provides a way to connect multiple Siddhi Apps deployed under the same Siddhi Manager (JVM). Here both the publisher and subscriber should have the same event schema (stream definition) for successful data transfer.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="inMemory", topic="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">topic</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Event are delivered to allthe subscribers subscribed on this topic.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='inMemory', topic='Stocks', @map(type='passThrough'))
define stream StocksStream (symbol string, price float, volume long);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Here the <code>StocksStream</code> uses inMemory sink to emit the Siddhi events to all the inMemory sources deployed in the same JVM and subscribed to the topic <code>Stocks</code>.</p>
<p></p>
### log *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">(Sink)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">This is a sink that can be used as a logger. This will log the output events in the output stream with user specified priority and a prefix</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(type="log", priority="<STRING>", prefix="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">priority</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This will set the logger priority i.e log level. Accepted values are INFO, DEBUG, WARN, FATAL, ERROR, OFF, TRACE</p></td>
        <td style="vertical-align: top">INFO</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">prefix</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This will be the prefix to the output message. If the output stream has event [2,4] and the prefix is given as "Hello" then the log will show "Hello : [2,4]"</p></td>
        <td style="vertical-align: top">default prefix will be <Siddhi App Name> : <Stream Name></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='log', prefix='My Log', priority='DEBUG') 
define stream BarStream (symbol string, price float, volume long)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example BarStream uses log sink and the prefix is given as My Log. Also the priority is set to DEBUG.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@sink(type='log', priority='DEBUG') 
define stream BarStream (symbol string, price float, volume long)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example BarStream uses log sink and the priority is set to DEBUG. User has not specified prefix so the default prefix will be in the form &lt;Siddhi App Name&gt; : &lt;Stream Name&gt;</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@sink(type='log', prefix='My Log') 
define stream BarStream (symbol string, price float, volume long)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example BarStream uses log sink and the prefix is given as My Log. User has not given a priority so it will be set to default INFO.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
@sink(type='log') 
define stream BarStream (symbol string, price float, volume long)
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example BarStream uses log sink. The user has not given prefix or priority so they will be set to their default values.</p>
<p></p>
## Sinkmapper

### passThrough *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">(Sink Mapper)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Pass-through mapper passed events (Event[]) through without any mapping or modifications.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@sink(..., @map(type="passThrough")
```

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type='inMemory', @map(type='passThrough'))
define stream BarStream (symbol string, price float, volume long);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In the following example BarStream uses passThrough outputmapper which emit Siddhi event directly without any transformation into sink.</p>
<p></p>
## Source

### inMemory *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">In-memory source subscribes to a topic to consume events which are published on the same topic by In-memory sinks. This provides a way to connect multiple Siddhi Apps deployed under the same Siddhi Manager (JVM). Here both the publisher and subscriber should have the same event schema (stream definition) for successful data transfer.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="inMemory", topic="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">topic</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Subscribes to the events sent on the given topic.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='inMemory', topic='Stocks', @map(type='passThrough'))
define stream StocksStream (symbol string, price float, volume long);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">Here the <code>StocksStream</code> uses inMemory source to consume events published on the topic <code>Stocks</code> by the inMemory sinks deployed in the same JVM.</p>
<p></p>
## Sourcemapper

### passThrough *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">(Source Mapper)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">Pass-through mapper passed events (Event[]) through without any mapping or modifications.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(..., @map(type="passThrough")
```

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='tcp', @map(type='passThrough'))
define stream BarStream (symbol string, price float, volume long);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example BarStream uses passThrough inputmapper which passes the received Siddhi event directly without any transformation into source.</p>
<p></p>
