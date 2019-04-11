\# Siddhi 5.x User Guide

## System Requirements
1. **Memory**   - 128 MB (minimum), 500 MB (recommended), higher memory might be needed based on in-memory data stored for processing
2. **Cores**    - 2 cores (recommended), use lower number of cores after testing Siddhi Apps for performance
3. **JDK**      - 8 or 11
4. To build Siddhi from the Source distribution, it is necessary that you have JDK version 8 or 11 and Maven 3.0.4 or later

## Using Siddhi in various environments

### **Using Siddhi as a Java library**

Siddhi can be used as a library in any Java program (including OSGi runtimes) just by adding Siddhi and its extension jars as dependencies.

* Find a sample Siddhi project that's implemented as a Java program using Maven [here](https://github.com/suhothayan/siddhi-sample), this can be used as a reference for any based implementation.

* Following are the mandatory dependencies that need to be added to the Maven `pom.xml` file (or to the class path).

```xml
   <dependency>
     <groupId>io.siddhi</groupId>
     <artifactId>siddhi-core</artifactId>
     <version>5.x.x</version>
   </dependency>
   <dependency>
     <groupId>io.siddhi</groupId>
     <artifactId>siddhi-query-api</artifactId>
     <version>5.x.x</version>
   </dependency>
   <dependency>
     <groupId>io.siddhi</groupId>
     <artifactId>siddhi-query-compiler</artifactId>
     <version>5.x.x</version>
   </dependency>
   <dependency>
     <groupId>io.siddhi</groupId>
     <artifactId>siddhi-annotations</artifactId>
     <version>5.x.x</version>
   </dependency>   
```

* Sample Java class using Siddhi is as follows.

<script src="https://gist.github.com/suhothayan/0c81a843101601e82a7175b7add2aa4d.js"></script>

### **Using Siddhi as Local Micro Service**

Siddhi can also run a standalone program by passing the SiddhiApps and required configurations to it.

* Download the the latest Siddhi distribution from [Github releases](https://github.com/siddhi-io/distribution/releases).
* Unzip the siddhi-runner-x.x.x.zip
* Start SiddhiApps with the runner config by executing the following commands from the distribution directory<br/>
  Linux/Mac : `./bin/runner.sh -Dapps=<siddhi-file> -Dconfig=<config-yaml-file>`<br/>
  Windows : `bin\runner.bat -Dapps=<siddhi-file> -Dconfig=<config-yaml-file>`

!!! Tip "Running Multiple SiddhiApps in one runner."
    To run multiple SiddhiApps in one runtime, have all SiddhiApps in a directory and pass its location through `-Dapps` parameter as follows,<br/>
    `-Dapps=<siddhi-apps-directory>`

!!! Note "Always use **absolute path** for SiddhiApps and runner configs."
    Providing absolute path of SiddhiApp file, or directory in `-Dapps` parameter, and when providing the Siddhi runner config yaml on `-Dconfig` parameter while starting Siddhi runner.


**Samples**

####Running Siddhi App

Following SiddhiApp collects events via HTTP and logs the number of events arrived during last 15 seconds.  

<script src="https://gist.github.com/suhothayan/8b0471cbe86a0089db5d6afb1c46f765.js"></script>

<ul>
    <li>Copy the above SiddhiApp, and create the SiddhiApp file <code>CountOverTime.siddhi</code>.</li>
    <li>Run the SiddhiApp by executing following commands from the distribution directory
        <ul>
            <li>Linux/Mac :
            <pre style="white-space:pre-wrap;">./bin/runner.sh -Dapps=<absolute-siddhi-file-path>/CountOverTime.siddhi</pre>
            </li>
            <li>Windows :
            <pre style="white-space:pre-wrap;">bin\runner.bat -Dapps=<absolute-siddhi-file-path>\CountOverTime.siddhi</pre>
            </li>
        </ul>
    </li>
    <li>Test the SiddhiApp by calling the HTTP endpoint using curl or Postman as follows
        <ul>
            <li><b>Publish events with curl command:</b><br/>
            Publish few json to the http endpoint as follows,
<pre style="white-space:pre-wrap;">
curl -X POST http://localhost:8006/production \
  --header "Content-Type:application/json" \
  -d '{"event":{"name":"Cake","amount":20.12}}'
</pre>
            </li>
            <li><b>Publish events with Postman:</b>
              <ol>
                <li>Install 'Postman' application from Chrome web store</li>
                <li>Launch the application</li>
                <li>Make a 'Post' request to 'http://localhost:8006/production' endpoint. Set the Content-Type to <code>'application/json'</code> and set the request body in json format as follows,
<pre>
{
  "event": {
    "name": "Cake",
    "amount": 20.12
  }
}</pre>
                </li>
              </ol>
            </li>
        </ul>
    </li>
    <li>Runner logs the total count on the console. Note, how the count increments with every event sent.
<pre style="white-space:pre-wrap;">
[2019-04-11 13:36:03,517]  INFO {io.siddhi.core.stream.output.sink.LogSink} - CountOverTime : TotalCountStream : Event{timestamp=1554969963512, data=[1], isExpired=false}
[2019-04-11 13:36:10,267]  INFO {io.siddhi.core.stream.output.sink.LogSink} - CountOverTime : TotalCountStream : Event{timestamp=1554969970267, data=[2], isExpired=false}
[2019-04-11 13:36:41,694]  INFO {io.siddhi.core.stream.output.sink.LogSink} - CountOverTime : TotalCountStream : Event{timestamp=1554970001694, data=[1], isExpired=false}
</pre>
    </li>
</ul>

####Running with runner config
When running SiddhiApps users can optionally provide a config yaml to Siddhi runner to manage configurations such as state persistence, databases connections and secure vault.

Following SiddhiApp collects events via HTTP and store them in H2 Database.

<script src="https://gist.github.com/suhothayan/2413a6f886219befe736bf4447af02de.js"></script>

The runner config can by configured with the relevant datasource information and passed when starting the runner

<script src="https://gist.github.com/suhothayan/015ae003d4ba8d4aeaadc19e4c9516fd.js"></script>

<ul>
    <li>Copy the above SiddhiApp, & config yaml, and create corresponding the SiddhiApp file <code>ConsumeAndStore.siddhi</code> and <code>TestDb.yaml</code> files.</li>
    <li>Run the SiddhiApp by executing following commands from the distribution directory
        <ul>
            <li>Linux/Mac :
            <pre style="white-space:pre-wrap;">./bin/runner.sh -Dapps=<absolute-siddhi-file-path>/ConsumeAndStore.siddhi \
  -Dconfig=<absolute-config-yaml-path>/TestDb.yaml</pre>
            </li>
            <li>Windows :
            <pre style="white-space:pre-wrap;">bin\runner.sh -Dapps=<absolute-siddhi-file-path>\ConsumeAndStore.siddhi ^
  -Dconfig=<absolute-config-yaml-path>\TestDb.yaml</pre>
            </li>
        </ul>
    </li>
    <li>Test the SiddhiApp by calling the HTTP endpoint using curl or Postman as follows
        <ul>
            <li><b>Publish events with curl command:</b><br/>
            Publish few json to the http endpoint as follows,
<pre style="white-space:pre-wrap;">
curl -X POST http://localhost:8006/production \
  --header "Content-Type:application/json" \
  -d '{"event":{"name":"Cake","amount":20.12}}'
</pre>
            </li>
            <li><b>Publish events with Postman:</b>
              <ol>
                <li>Install 'Postman' application from Chrome web store</li>
                <li>Launch the application</li>
                <li>Make a 'Post' request to 'http://localhost:8006/production' endpoint. Set the Content-Type to <code>'application/json'</code> and set the request body in json format as follows,
<pre>
{
  "event": {
    "name": "Cake",
    "amount": 20.12
  }
}</pre>
                </li>
              </ol>
            </li>
        </ul>
    </li>
    <li>Query Siddhi Store APIs to retrieve 10 records from the table.
        <ul>
            <li><b>Query stored events with curl command:</b><br/>
            Publish few json to the http endpoint as follows,
<pre style="white-space:pre-wrap;">
curl -X POST https://localhost:9443/stores/query \
  -H "content-type: application/json" \
  -u "admin:admin" \
  -d '{"appName" : "ConsumeAndStore", "query" : "from ProductionTable select * limit 10;" }' -k
</pre>
            </li>
            <li><b>Query stored events with Postman:</b>
              <ol>
                <li>Install 'Postman' application from Chrome web store</li>
                <li>Launch the application</li>
                <li>Make a 'Post' request to 'https://localhost:9443/stores/query' endpoint. Set the Content-Type to <code>'application/json'</code> and set the request body in json format as follows,
<pre>
{
  "appName" : "ConsumeAndStore",
  "query" : "from ProductionTable select * limit 10;"
}</pre>
                </li>
              </ol>
            </li>
        </ul>
    </li>
    <li>The results of the query will be as follows,
<pre style="white-space:pre-wrap;">
{
  "records":[
    ["Cake",20.12]
  ]
}</pre>
    </li>
</ul>

####Running with environmental/system variables
Templating SiddhiApps allows users to provide environment/system variables to siddhiApps at runtime. This can help users to migrate SiddhiApps from one environment to another (E.g from dev, test and to prod).

Following templated SiddhiApp collects events via HTTP, filters them based on `amount` greater than a given threshold value, and only sends the filtered events via email.

Here the `THRESHOLD` value, and `TO_EMAIL` are templated in the `TemplatedFilterAndEmail.siddhi` SiddhiApp.

<script src="https://gist.github.com/suhothayan/47b6300f629463e63b2d5be78d6e45b4.js"></script>

The runner config is configured with a gmail account to send email messages in `EmailConfig.yaml` by templating sending `EMAIL_ADDRESS`, `EMAIL_USERNAME` and `EMAIL_PASSWORD`.   

<script src="https://gist.github.com/suhothayan/3f36f8827d379925a8cca68dd78b9a8e.js"></script>

<ul>
    <li>Copy the above SiddhiApp, & config yaml, and create corresponding the SiddhiApp file <code>TemplatedFilterAndEmail.siddhi</code> and <code>EmailConfig.yaml</code> files.</li>
    
    <li>Set environment variables by running following in the termial Siddhi is about to run: 
         <pre style="white-space:pre-wrap;">
export THRESHOLD=20
export TO_EMAIL=<to email address>
export EMAIL_ADDRESS=<gmail address>
export EMAIL_USERNAME=<gmail username>
export EMAIL_PASSWORD=<gmail password>
         </pre>
        Or they can also be passed as system variables by adding <code>-DTHRESHOLD=20 -DTO_EMAIL=<to email address> -DEMAIL_ADDRESS=<gmail address> -DEMAIL_USERNAME=<gmail username> -DEMAIL_PASSWORD=<gmail password></code>
        to the end of the runner startup script.
    </li>
        <li>Run the SiddhiApp by executing following commands from the distribution directory
        <ul>
            <li>Linux/Mac :
            <pre style="white-space:pre-wrap;">
./bin/runner.sh -Dapps=<absolute-file-path>/TemplatedFilterAndEmail.siddhi \
  -Dconfig=<absolute-config-yaml-path>/EmailConfig.yaml</pre>
            </li>
            <li>Windows :
            <pre style="white-space:pre-wrap;">
bin\runner.bat -Dapps=<absolute-file-path>\TemplatedFilterAndEmail.siddhi ^
  -Dconfig=<absolute-config-yaml-path>\EmailConfig.yaml</pre>
            </li>
        </ul>
    </li>
    <li>Test the SiddhiApp by calling the HTTP endpoint using curl or Postman as follows
        <ul>
            <li><b>Publish events with curl command:</b><br/>
            Publish few json to the http endpoint as follows,
<pre style="white-space:pre-wrap;">
curl -X POST http://localhost:8006/production \
  --header "Content-Type:application/json" \
  -d '{"event":{"name":"Cake","amount":2000.0}}'
</pre>
            </li>
            <li><b>Publish events with Postman:</b>
              <ol>
                <li>Install 'Postman' application from Chrome web store</li>
                <li>Launch the application</li>
                <li>Make a 'Post' request to 'http://localhost:8006/production' endpoint. Set the Content-Type to <code>'application/json'</code> and set the request body in json format as follows,
<pre>
{
  "event": {
    "name": "Cake",
    "amount": 2000.0
  }
}</pre>
                </li>
              </ol>
            </li>
        </ul>
    </li>
    <li>Check the <code>to.email</code> for the published email message, which will look as follows,
<pre style="white-space:pre-wrap;">
Subject : High Cake production!

Hi, 

High production of Cake, with amount 2000.0 identified. 

For more information please contact production department. 

Thank you</pre>
    </li>
</ul>

### **Using Siddhi as Docker Micro Service**

WIP

### **Using Siddhi as Kubernetes Micro Service**

WIP

### **Using Siddhi as a Python Library**

WIP
