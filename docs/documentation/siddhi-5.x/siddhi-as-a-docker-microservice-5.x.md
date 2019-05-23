# Siddhi 5.x as a Docker Microservice

This section provides information on running [Siddhi Apps](../user-guide-introduction-5.x/#siddhi-application) on Docker. 

Siddhi Microservice can run one or more Siddhi Applications with required system configurations.
Here, the Siddhi application (`.siddhi` file) contains stream processing logic and the necessary system configurations can be passed via the Siddhi configuration `.yaml` file. 

Steps to Run Siddhi Docker Microservice is as follows.

* Pull the the latest Siddhi Runner image from [Siddhiio Docker Hub](https://hub.docker.com/u/siddhiio).
```
docker pull siddhiio/siddhi-runner-alpine:latest
```
* Start SiddhiApps with the runner config by executing the following docker command.<br/>
```
docker run -it -v <local-siddhi-file-path>:<siddhi-file-mount-path> -v <local-conf-file-path>:<conf-file-mount-path> siddhiio/siddhi-runner-alpine:latest -Dapps=<siddhi-file-mount-path> -Dconfig=<conf-file-mount-path>
```
E.g.,
```
docker run -it -v /home/me/siddhi-apps:/apps -v /home/me/siddhi-configs:/configs siddhiio/siddhi-runner-alpine:latest -Dapps=/apps/Foo.siddhi -Dconfig=/configs/siddhi-config.yaml
```

!!! Tip "Running multiple SiddhiApps in one runner instance."
    To run multiple SiddhiApps in one runtime instance, have all SiddhiApps in a directory, mount the directory and pass its location through `-Dapps` parameter as follows,<br/>
    `-Dapps=<siddhi-apps-directory>`

!!! Note "Always use **absolute path** for SiddhiApps and runner configs."
    Providing absolute path of SiddhiApp file, or directory in `-Dapps` parameter, and when providing the Siddhi runner config yaml on `-Dconfig` parameter while starting Siddhi runner.

!!! Tip "Siddhi Tooling"
    You can also use the powerful [Siddhi Editor](../quckstart-5.x/#3-using-siddhi-for-the-first-time) to implement and test steam processing applications. 

!!! Info "Configuring Siddhi"
    To configure databases, extensions, authentication, periodic state persistence, and statistics for Siddhi as Docker Microservice refer [Siddhi Config Guide](../config-guide-5.x/). 

## Samples

###Running Siddhi App

Following SiddhiApp collects events via HTTP and logs the number of events arrived during last 15 seconds.  

<script src="https://gist.github.com/suhothayan/8b0471cbe86a0089db5d6afb1c46f765.js"></script>

<ul>
    <li>Copy the above SiddhiApp, and create the SiddhiApp file <code>CountOverTime.siddhi</code>.</li>
    <li>Run the SiddhiApp by executing following commands from the distribution directory
        <ul>
            <li>
            <pre style="white-space:pre-wrap;">docker run -it -p 8006:8006 -v &lt;local-absolute-siddhi-file-path&gt;/CountOverTime.siddhi:/apps/CountOverTime.siddhi siddhiio/siddhi-runner-alpine -Dapps=/apps/CountOverTime.siddhi
</pre>
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

### Running with runner config
When running SiddhiApps users can optionally provide a config yaml to Siddhi runner to manage configurations such as state persistence, databases connections and secure vault.

Following SiddhiApp collects events via HTTP and store them in H2 Database.

<script src="https://gist.github.com/suhothayan/2413a6f886219befe736bf4447af02de.js"></script>

The runner config can be configured with the relevant datasource information and passed when starting the runner

<script src="https://gist.github.com/suhothayan/015ae003d4ba8d4aeaadc19e4c9516fd.js"></script>

<ul>
    <li>Copy the above SiddhiApp, & config yaml, and create corresponding the SiddhiApp file <code>ConsumeAndStore.siddhi</code> and <code>TestDb.yaml</code> files.</li>
    <li>Run the SiddhiApp by executing following command
        <ul>
            <li>
             <pre style="white-space:pre-wrap;">docker run -it -p 8006:8006 -p 9443:9443 -v &lt;local-absolute-siddhi-file-path&gt;/ConsumeAndStore.siddhi:/apps/ConsumeAndStore.siddhi -v &lt;local-absolute-config-yaml-path&gt;/TestDb.yaml:/conf/TestDb.yaml siddhiio/siddhi-runner-alpine -Dapps=/apps/ConsumeAndStore.siddhi -Dconfig=/conf/TestDb.yaml</pre>
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

### Running with environmental/system variables

Templating SiddhiApps allows users to provide environment/system variables to siddhiApps at runtime. This can help users to migrate SiddhiApps from one environment to another (E.g from dev, test and to prod).

Following templated SiddhiApp collects events via HTTP, filters them based on `amount` greater than a given threshold value, and only sends the filtered events via email.

Here the `THRESHOLD` value, and `TO_EMAIL` are templated in the `TemplatedFilterAndEmail.siddhi` SiddhiApp.

<script src="https://gist.github.com/suhothayan/47b6300f629463e63b2d5be78d6e45b4.js"></script>

The runner config is configured with a gmail account to send email messages in `EmailConfig.yaml` by templating sending `EMAIL_ADDRESS`, `EMAIL_USERNAME` and `EMAIL_PASSWORD`.   

<script src="https://gist.github.com/suhothayan/3f36f8827d379925a8cca68dd78b9a8e.js"></script>

<ul>
    <li>Copy the above SiddhiApp, & config yaml, and create corresponding the SiddhiApp file <code>TemplatedFilterAndEmail.siddhi</code> and <code>EmailConfig.yaml</code> files.</li>
    
    <li>Set the below environment variables by passing them during the docker run command: 
         <pre style="white-space:pre-wrap;">
 THRESHOLD=20
 TO_EMAIL=&lt;to email address&gt; 
 EMAIL_ADDRESS=&lt;gmail address&gt;
 EMAIL_USERNAME=&lt;gmail username&gt;
 EMAIL_PASSWORD=&lt;gmail password&gt;</pre>
        Or they can also be passed as system variables by adding them to the end of the docker run command .
        <pre style="white-space:pre-wrap;">-DTHRESHOLD=20 -DTO_EMAIL=&gt;to email address&gt; -DEMAIL_ADDRESS=&lt;gmail address&gt; 
 -DEMAIL_USERNAME=&lt;gmail username&gt; -DEMAIL_PASSWORD=&lt;gmail password&gt;</pre>

    </li>
        <li>Run the SiddhiApp by executing following command.
        <ul>
            <li>
            <pre style="white-space:pre-wrap;">docker run -it -p 8006:8006 -v &lt;local-absolute-siddhi-file-path&gt;/TemplatedFilterAndEmail.siddhi:/apps/TemplatedFilterAndEmail.siddhi -v &lt;local-absolute-config-yaml-path&gt;/EmailConfig.yaml:/conf/EmailConfig.yaml -e THRESHOLD=20 -e TO_EMAIL=&lt;to email address&gt; -e EMAIL_ADDRESS=&lt;gmail address&gt; -e EMAIL_USERNAME=&lt;gmail username&gt; -e EMAIL_PASSWORD=&lt;gmail password&gt; siddhiio/siddhi-runner-alpine -Dapps=/apps/TemplatedFilterAndEmail.siddhi -Dconfig=/conf/EmailConfig.yaml </pre>

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
