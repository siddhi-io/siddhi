# Siddhi 5.x as a Kubernetes Microservice

This section provides information on running [Siddhi Apps](../user-guide-introduction-5.x/#siddhi-application) natively in Kubernetes via Siddhi Kubernetes Operator. 

Siddhi can be configured in `SiddhiProcess` kind and passed to the CRD for deployment.
Here, the Siddhi applications containing stream processing logic can be written inline in `SiddhiProcess` yaml or passed as `.siddhi` files via configmaps. `SiddhiProcess` yaml can also be configured with the necessary system configurations. 

## Prerequisites 

* A Kubernetes cluster v1.10.11 or higher. 

    1. [Minikube](https://github.com/kubernetes/minikube#installation)
    2. [Google Kubernetes Engine(GKE) Cluster](https://console.cloud.google.com/)
    3. [Docker for Mac](https://docs.docker.com/docker-for-mac/install/)
    4. Or any other Kubernetes cluster
   
* Admin privileges to install Siddhi operator  

!!! Note "Minikube"
    Siddhi operator automatically creates NGINX ingress. Therefore it to work we can either enable ingress on Minikube using the following command.
    <pre>
    minikube addons enable ingress
    </pre>
    or disable Siddhi operator's [automatically ingress creation](#deploy-siddhi-apps-without-ingress-creation).

!!! Note "Google Kubernetes Engine (GKE) Cluster"
    To install Siddhi operator, you have to give cluster admin permission to your account. In order to do that execute the following command (by replacing "your-address@email.com" with your account email address). 
    <pre>kubectl create clusterrolebinding user-cluster-admin-binding \ 
            --clusterrole=cluster-admin --user=your-address@email.com
    </pre>  
    
!!! Note "Docker for Mac"
    Siddhi operator automatically creates NGINX ingress. Therefore it to work we can either enable ingress on Docker for mac following the official [documentation](https://kubernetes.github.io/ingress-nginx/deploy/#docker-for-mac)
    or disable Siddhi operator's [automatically ingress creation](#deploy-siddhi-apps-without-ingress-creation).
    
## Install Siddhi Operator

To install the Siddhi Kubernetes operator run the following commands.

```
kubectl apply -f https://github.com/siddhi-io/siddhi-operator/releases/download/v0.1.1/prerequisites.yaml
kubectl apply -f https://github.com/siddhi-io/siddhi-operator/releases/download/v0.1.1/siddhi-operator.yaml
```
You can verify the installation by making sure the following deployments are running in your Kubernetes cluster. 

```
$ kubectl get deployment

NAME              DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
siddhi-operator   1         1         1            1           1m
siddhi-parser     1         1         1            1           1m

```

## Deploy and run Siddhi App

Siddhi applications can be deployed on Kubernetes using the Siddhi operator. 

Here we will creating a very simple Siddhi stream processing application that consumes events via HTTP, filers the input events on the type 'monitored' and logs the output on the console.
This can be created using a SiddhiProcess YAML file as given below.

<script src="https://gist.github.com/BuddhiWathsala/f029d671f4f6d7719dce59500b970815.js"></script>

!!! Tip "Siddhi Tooling"
    You can also use the powerful [Siddhi Editor](../quckstart-5.x/#3-using-siddhi-for-the-first-time) to implement and test steam processing applications. 

!!! Info "Configuring Siddhi"
    To configure databases, extensions, authentication, periodic state persistence, and statistics for Siddhi as Kubernetes Microservice refer [Siddhi Config Guide](../config-guide-5.x/). 

To deploy the above Siddhi app in your Kubernetes cluster, copy to a YAML file with name `monitor-app.yaml` and execute the following command.

```
kubectl create -f <absolute-yaml-file-path>/monitor-app.yaml
```

!!! Note "tls secret" 
    Within the SiddhiProcess, a tls secret named `siddhi-tls` is configured. If a Kubernetes secret with the same name does not exist in the Kubernetes cluster, the NGINX will ignore it and use a self-generated certificate. Configuring a secret will be necessary for calling HTTPS endpoints, refer [deploy and run Siddhi apps with HTTPS](#deploy-and-run-siddhi-app-with-https) section for more details. 

If the `monitor-app` is deployed successfully, the created SiddhiProcess, deployment, service, and ingress can be viewed as follows.

```
$ kubectl get SiddhiProcesses
NAME          AGE
monitor-app   2m

$ kubectl get deployment
NAME              DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
monitor-app       1         1         1            1           1m
siddhi-operator   1         1         1            1           1m
siddhi-parser     1         1         1            1           1m

$ kubectl get service
NAME              TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)          AGE
kubernetes        ClusterIP      10.96.0.1        <none>        443/TCP          10d
monitor-app       ClusterIP      10.101.242.132   <none>        8280/TCP         1m
siddhi-operator   ClusterIP      10.111.138.250   <none>        8383/TCP         1m
siddhi-parser     LoadBalancer   10.102.172.142   <pending>     9090:31830/TCP   1m

$ kubectl get ingress
NAME      HOSTS     ADDRESS     PORTS     AGE
siddhi    siddhi    10.0.2.15   80, 443   1m

```

**_Invoke Siddhi Applications_**

To invoke the Siddhi App, first obtain the external IP of the ingress load balancer using `kubectl get ingress` command as follows.
```
$ kubectl get ingress
NAME      HOSTS     ADDRESS     PORTS     AGE
siddhi    siddhi    10.0.2.15   80, 443   1m
``` 
Then, add the host `siddhi` and related external IP (`ADDRESS`) to the `/etc/hosts` file in your machine.

!!! Note "Minikube"
    For Minikube, you have to use Minikube IP as the external IP. Hence, run `minikube ip` command to get the IP of the Minikube cluster.
    
!!! Note "Docker for Mac"
    For Docker for Mac, you have to use `0.0.0.0` as the external IP.    

Use the following CURL command to send events to `monitor-app` deployed in Kubernetes.

```
curl -X POST \
  https://siddhi/monitor-app/8280/example \
  -H 'Content-Type: application/json' \
  -d '{
	"type": "monitored",
	"deviceID": "001",
	"power": 341
  }' -k

```

_Note: Here `-k` option is used to turn off curl's verification of the certificate._ 

**_View Siddhi Process Logs_**

Since the output of `monitor-app` is logged, you can see the output by monitoring the associated pod's logs. 
 
To find the `monitor-app` pod use the `kubectl get pods` command. This will list down all the deployed pods. 

```
$ kubectl get pods

NAME                               READY     STATUS    RESTARTS   AGE
monitor-app-7f8584875f-krz6t       1/1       Running   0          2m
siddhi-operator-8589c4fc69-6xbtx   1/1       Running   0          2m
siddhi-parser-64d4cd86ff-pfq2s     1/1       Running   0          2m
```

Here, the pod starting with the SiddhiProcess name (in this case `monitor-app-`) is the pod we need to monitor.

To view the logs, run the `kubectl logs <pod name>` command. 
This will show all the Siddhi process logs, along with the filtered output events as given below.

```
$ kubectl logs monitor-app-7f8584875f-krz6t
[2019-04-20 04:04:02,216]  INFO {org.wso2.extension.siddhi.io.http.source.HttpSourceListener} - Event input has paused for http://0.0.0.0:8280/example
[2019-04-20 04:04:02,235]  INFO {org.wso2.extension.siddhi.io.http.source.HttpSourceListener} - Event input has resume for http://0.0.0.0:8280/example
[2019-04-20 04:05:29,741]  INFO {io.siddhi.core.stream.output.sink.LogSink} - LOGGER : Event{timestamp=1555733129736, data=[monitored, 001, 341], isExpired=false}
```

## Get Siddhi process status

### List Siddhi processes

List the Siddhi process using the `kubectl get sps` or `kubectl get SiddhiProcesses` commands as follows.

```
$ kubectl get sps
NAME          AGE
monitor-app   2m

$ kubectl get SiddhiProcesses
NAME          AGE
monitor-app   2m
```

### View Siddhi process configs

Get the Siddhi process configuration details using `kubectl describe sp` command as follows.

```
$ kubectl describe sp monitor-app

Name:         monitor-app
Namespace:    default
Labels:       <none>
Annotations:  <none>
API Version:  siddhi.io/v1alpha1
Kind:         SiddhiProcess
Metadata:
  Creation Timestamp:  2019-04-18T18:05:39Z
  Generation:          1
  Resource Version:    497702
  Self Link:           /apis/siddhi.io/v1alpha1/namespaces/default/siddhiprocesses/monitor-app
  UID:                 92b2293b-6204-11e9-996c-0800279e6dba
Spec:
  Env:
    Name:   RECEIVER_URL
    Value:  http://0.0.0.0:8280/example
    Name:   BASIC_AUTH_ENABLED
    Value:  false
  Pod:
    Image:      siddhiio/siddhi-runner-alpine
    Image Tag:  0.1.0
  Query:        @App:name("MonitorApp")
@App:description("Description of the plan") 

@sink(type='log', prefix='LOGGER')
@source(type='http', receiver.url='${RECEIVER_URL}', basic.auth.enabled='${BASIC_AUTH_ENABLED}', @map(type='json'))
define stream DevicePowerStream (type string, deviceID string, power int);


define stream MonitorDevicesPowerStream(deviceID string, power int);

@info(name='monitored-filter')
from DevicePowerStream[type == 'monitored']
select deviceID, power
insert into MonitorDevicesPowerStream;

  Siddhi . Runner . Configs:  state.persistence:
  enabled: true
  intervalInMin: 5
  revisionsToKeep: 2
  persistenceStore: io.siddhi.distribution.core.persistence.FileSystemPersistenceStore
  config:
    location: siddhi-app-persistence

Status:
  Nodes:   <nil>
  Status:  Running
Events:    <none>

```

Get the Siddhi process YAML using `kubectl get sp` command as follows.

```
$ kubectl get sp monitor-app -o yaml

apiVersion: siddhi.io/v1alpha1
kind: SiddhiProcess
metadata:
  creationTimestamp: 2019-04-18T18:05:39Z
  generation: 1
  name: monitor-app
  namespace: default
  resourceVersion: "497702"
  selfLink: /apis/siddhi.io/v1alpha1/namespaces/default/siddhiprocesses/monitor-app
  uid: 92b2293b-6204-11e9-996c-0800279e6dba
spec:
  env:
  - name: RECEIVER_URL
    value: http://0.0.0.0:8280/example
  - name: BASIC_AUTH_ENABLED
    value: "false"
  pod:
    image: siddhiio/siddhi-runner-alpine
    imageTag: 0.1.0
  query: "@App:name(\"MonitorApp\")\n@App:description(\"Description of the plan\")
    \n\n@sink(type='log', prefix='LOGGER')\n@source(type='http', receiver.url='${RECEIVER_URL}',
    basic.auth.enabled='${BASIC_AUTH_ENABLED}', @map(type='json'))\ndefine stream
    DevicePowerStream (type string, deviceID string, power int);\n\n\ndefine stream
    MonitorDevicesPowerStream(deviceID string, power int);\n\n@info(name='monitored-filter')\nfrom
    DevicePowerStream[type == 'monitored']\nselect deviceID, power\ninsert into MonitorDevicesPowerStream;\n"
  siddhi.runner.configs: |
    state.persistence:
      enabled: true
      intervalInMin: 5
      revisionsToKeep: 2
      persistenceStore: io.siddhi.distribution.core.persistence.FileSystemPersistenceStore
      config:
        location: siddhi-app-persistence
status:
  nodes: null
  status: Running

```

### View Siddhi process logs

To view the Siddhi process logs, first get the Siddhi process pods using the `kubectl get pods` command as follows.  

```
$ kubectl get pods

NAME                               READY     STATUS    RESTARTS   AGE
monitor-app-7f8584875f-krz6t       1/1       Running   0          2m
siddhi-operator-8589c4fc69-6xbtx   1/1       Running   0          2m
siddhi-parser-64d4cd86ff-pfq2s     1/1       Running   0          2m
```

Then to retrieve the Siddhi process logs, run `kubectl logs <pod name>` command. Here `<pod name>` should be replaced with the name of the pod that starts with the relevant SiddhiProcess's name. 
A sample output logs is of this command is as follows.

```
$ kubectl logs monitor-app-7f8584875f-krz6t
JAVA_HOME environment variable is set to /opt/java/openjdk
CARBON_HOME environment variable is set to /home/siddhi_user/siddhi-runner-0.1.0
RUNTIME_HOME environment variable is set to /home/siddhi_user/siddhi-runner-0.1.0/wso2/runner
Picked up JAVA_TOOL_OPTIONS: -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap
[2019-04-20 3:58:57,734]  INFO {org.wso2.carbon.launcher.extensions.OSGiLibBundleDeployerUtils updateOSGiLib} - Successfully updated the OSGi bundle information of Carbon Runtime: runner  
osgi> [2019-04-20 03:59:00,208]  INFO {org.wso2.carbon.config.reader.ConfigFileReader} - Default deployment configuration updated with provided custom configuration file monitor-app-deployment.yaml
[2019-04-20 03:59:01,551]  INFO {org.wso2.msf4j.internal.websocket.WebSocketServerSC} - All required capabilities are available of WebSocket service component is available.
[2019-04-20 03:59:01,584]  INFO {org.wso2.carbon.metrics.core.config.model.JmxReporterConfig} - Creating JMX reporter for Metrics with domain 'org.wso2.carbon.metrics'
[2019-04-20 03:59:01,609]  INFO {org.wso2.msf4j.analytics.metrics.MetricsComponent} - Metrics Component is activated
[2019-04-20 03:59:01,614]  INFO {org.wso2.carbon.databridge.agent.internal.DataAgentDS} - Successfully deployed Agent Server 
[2019-04-20 03:59:02,219]  INFO {io.siddhi.distribution.core.internal.ServiceComponent} - Periodic state persistence started with an interval of 5 using io.siddhi.distribution.core.persistence.FileSystemPersistenceStore
[2019-04-20 03:59:02,229]  INFO {io.siddhi.distribution.event.simulator.core.service.CSVFileDeployer} - CSV file deployer initiated.
[2019-04-20 03:59:02,233]  INFO {io.siddhi.distribution.event.simulator.core.service.SimulationConfigDeployer} - Simulation config deployer initiated.
[2019-04-20 03:59:02,279]  INFO {org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiverServiceComponent} - org.wso2.carbon.databridge.receiver.binary.internal.Service Component is activated
[2019-04-20 03:59:02,312]  INFO {org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver} - Started Binary SSL Transport on port : 9712
[2019-04-20 03:59:02,321]  INFO {org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver} - Started Binary TCP Transport on port : 9612
[2019-04-20 03:59:02,322]  INFO {org.wso2.carbon.databridge.receiver.thrift.internal.ThriftDataReceiverDS} - Service Component is activated
[2019-04-20 03:59:02,344]  INFO {org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver} - Thrift Server started at 0.0.0.0
[2019-04-20 03:59:02,356]  INFO {org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver} - Thrift SSL port : 7711
[2019-04-20 03:59:02,363]  INFO {org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver} - Thrift port : 7611
[2019-04-20 03:59:02,449]  INFO {org.wso2.msf4j.internal.MicroservicesServerSC} - All microservices are available
[2019-04-20 03:59:02,516]  INFO {org.wso2.transport.http.netty.contractimpl.listener.ServerConnectorBootstrap$HttpServerConnector} - HTTP(S) Interface starting on host 0.0.0.0 and port 9090
[2019-04-20 03:59:02,520]  INFO {org.wso2.transport.http.netty.contractimpl.listener.ServerConnectorBootstrap$HttpServerConnector} - HTTP(S) Interface starting on host 0.0.0.0 and port 9443
[2019-04-20 03:59:03,068]  INFO {io.siddhi.distribution.core.internal.StreamProcessorService} - Periodic State persistence enabled. Restoring last persisted state of MonitorApp
[2019-04-20 03:59:03,075]  INFO {org.wso2.transport.http.netty.contractimpl.listener.ServerConnectorBootstrap$HttpServerConnector} - HTTP(S) Interface starting on host 0.0.0.0 and port 8280
[2019-04-20 03:59:03,077]  INFO {org.wso2.extension.siddhi.io.http.source.HttpConnectorPortBindingListener} - HTTP source 0.0.0.0:8280 has been started
[2019-04-20 03:59:03,084]  INFO {io.siddhi.distribution.core.internal.StreamProcessorService} - Siddhi App MonitorApp deployed successfully
[2019-04-20 03:59:03,093]  INFO {org.wso2.carbon.kernel.internal.CarbonStartupHandler} - Siddhi Runner Distribution started in 5.941 sec
[2019-04-20 04:04:02,216]  INFO {org.wso2.extension.siddhi.io.http.source.HttpSourceListener} - Event input has paused for http://0.0.0.0:8280/example
[2019-04-20 04:04:02,235]  INFO {org.wso2.extension.siddhi.io.http.source.HttpSourceListener} - Event input has resume for http://0.0.0.0:8280/example
[2019-04-20 04:05:29,741]  INFO {io.siddhi.core.stream.output.sink.LogSink} - LOGGER : Event{timestamp=1555733129736, data=[monitored, 001, 341], isExpired=false}
```

## Deploy and run Siddhi App using configmaps

Siddhi operator allows you to deploy Siddhi app configurations via configmaps instead of just adding them inline. Through this you can also run multiple Siddhi Apps in a single SiddhiProcess. 

This can be done by passing the configmaps containing Siddhi app files to the SiddhiProcess's apps configuration as follows. 

```
apps:
  - config-map-name1
  - config-map-name2
```
**Sample on deploying and running Siddhi Apps via configmaps**

Here we will creating a very simple Siddhi application as follows, that consumes events via HTTP, filers the input events on type 'monitored' and logs the output on the console.

<script src="https://gist.github.com/BuddhiWathsala/8687a2b73bb003a8ae7bcf3d3f63b78e.js"></script>

!!! Tip "Siddhi Tooling"
    You can also use the powerful [Siddhi Editor](../quckstart-5.x/#3-using-siddhi-for-the-first-time) to implement and test steam processing applications. 

Save the above Siddhi App file as `MonitorApp.siddhi`, and use this file to create a Kubernetes config map with the name `monitor-app-cm`.
This can be achieved by running the following command. 

```
kubectl create configmap monitor-app-cm --from-file=<absolute-file-path>/MonitorApp.siddhi
```

The created config map can be added to SiddhiProcess YAML under the `apps` entry as follows. 

<script src="https://gist.github.com/BuddhiWathsala/45019cf093226e4858c931e62e04233f.js"></script>

Save the YAML file as `monitor-app.yaml`, and use the following command to deploy the SiddhiProcess.

```
kubectl create -f <absolute-yaml-file-path>/monitor-app.yaml
```

!!! Note "Using a config, created from a directory containing multiple Siddhi files"
    SiddhiProcess's `apps` configuration also supports a config map that is created from a directory containing multiple Siddhi files.
    Use `kubectl create configmap siddhi-apps --from-file=<DIRECTORY_PATH>` command to create a config map from a directory.


**_Invoke Siddhi Applications_**

To invoke the Siddhi Apps, first obtain the external IP of the ingress load balancer using `kubectl get ingress` command as follows.
```
$ kubectl get ingress
NAME      HOSTS     ADDRESS     PORTS     AGE
siddhi    siddhi    10.0.2.15   80, 443   1m
``` 
Then, add the host `siddhi` and related external IP (`ADDRESS`) to the `/etc/hosts` file in your machine.

!!! Note "Minikube"
    For Minikube, you have to use Minikube IP as the external IP. Hence, run `minikube ip` command to get the IP of the Minikube cluster.

Use the following CURL command to send events to `monitor-app` deployed in Kubernetes.

```
curl -X POST \
  https://siddhi/monitor-app/8280/example \
  -H 'Content-Type: application/json' \
  -d '{
	"type": "monitored",
	"deviceID": "001",
	"power": 341
    }' -k
```

_Note: Here `-k` option is used to turn off curl's verification of the certificate._ 


**_View Siddhi Process Logs_**

Since the output of `monitor-app` is logged, you can see the output by monitoring the associated pod's logs. 
 
To find the `monitor-app` pod use the `kubectl get pods` command. This will list down all the deployed pods. 

```
$ kubectl get pods

NAME                               READY     STATUS    RESTARTS   AGE
monitor-app-7f8584875f-krz6t       1/1       Running   0          2m
siddhi-operator-8589c4fc69-6xbtx   1/1       Running   0          2m
siddhi-parser-64d4cd86ff-pfq2s     1/1       Running   0          2m
```

Here, the pod starting with the SiddhiProcess name (in this case `monitor-app-`) is the pod we need to monitor.

To view the logs, run the `kubectl logs <pod name>` command. 
This will show all the Siddhi process logs, along with the filtered output events as given below.

```
$ kubectl logs monitor-app-7f8584875f-krz6t
[2019-04-20 04:04:02,216]  INFO {org.wso2.extension.siddhi.io.http.source.HttpSourceListener} - Event input has paused for http://0.0.0.0:8280/example
[2019-04-20 04:04:02,235]  INFO {org.wso2.extension.siddhi.io.http.source.HttpSourceListener} - Event input has resume for http://0.0.0.0:8280/example
[2019-04-20 04:05:29,741]  INFO {io.siddhi.core.stream.output.sink.LogSink} - LOGGER : Event{timestamp=1555733129736, data=[monitored, 001, 341], isExpired=false}
```

## Deploy Siddhi Apps without Ingress creation

By default, Siddhi operator creates an NGINX ingress and exposes your HTTP/HTTPS through that ingress. If you need to disable automatic ingress creation, you have to change the `AUTO_INGRESS_CREATION` value in the Siddhi `operator.yaml` file to `false` or `null` as below.

<script src="https://gist.github.com/BuddhiWathsala/c1cadcf9828cfaf46bb909f30497e4ab.js"></script>

## Deploy and run Siddhi App with HTTPS 

Configuring tls will allow Siddhi ingress NGINX to expose HTTPS endpoints of your Siddhi Apps. To do so, created a Kubernetes secret and add that to the SiddhiProcess's `tls` configuration as following. 

```
tls:
  ingressSecret: siddhi-tls
```

**Sample on deploying and running Siddhi App with HTTPS**

First, you need to create a certificate using the following commands. For more details about the certificate creation refers [this](https://github.com/kubernetes/ingress-nginx/blob/master/docs/user-guide/tls.md#tls-secrets).

```
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout siddhi.key -out siddhi.crt -subj "/CN=siddhi/O=siddhi"
```

After that, create a kubernetes secret called `siddhi-tls`, which we intended to add to the TLS configurations using the following command.

```
kubectl create secret tls siddhi-tls --key siddhi.key --cert siddhi.crt
```

The created secret then need to be added to the created SiddhiProcess's `tls` configuration as following. 

<script src="https://gist.github.com/BuddhiWathsala/f029d671f4f6d7719dce59500b970815.js"></script>

When this is done Siddhi operator will now enable TLS support via the NGINX ingress, and you will be able to access all the HTTPS endpoints.

**_Invoke Siddhi Applications_**

You can use now send the events to following HTTPS endpoint.

```
https://siddhi/monitor-app/8280/example 
```
Further, you can use the following CURL command to send a request to the deployed siddhi applications via HTTPS.

```
curl --cacert siddhi.crt -X POST \
  https://siddhi/monitor-app/8280/example \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "monitored",
    "deviceID": "001",
    "power": 341
    }'
```

**_View Siddhi Process Logs_**

The output logs show the event that you sent using the previous CURL command.

```
$ kubectl get pods

NAME                               READY     STATUS    RESTARTS   AGE
monitor-app-667c97c898-rrtfs       1/1       Running   0          2m
siddhi-operator-79dcc45959-fkk4d   1/1       Running   0          3m
siddhi-parser-64d4cd86ff-k8b87     1/1       Running   0          3m

$ kubectl logs monitor-app-667c97c898-rrtfs

JAVA_HOME environment variable is set to /opt/java/openjdk
CARBON_HOME environment variable is set to /home/siddhi_user/siddhi-runner-0.1.0
RUNTIME_HOME environment variable is set to /home/siddhi_user/siddhi-runner-0.1.0/wso2/runner
Picked up JAVA_TOOL_OPTIONS: -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap
[2019-05-06 5:36:54,894]  INFO {org.wso2.carbon.launcher.extensions.OSGiLibBundleDeployerUtils updateOSGiLib} - Successfully updated the OSGi bundle information of Carbon Runtime: runner  
osgi> [2019-05-06 05:36:57,692]  INFO {org.wso2.msf4j.internal.websocket.WebSocketServerSC} - All required capabilities are available of WebSocket service component is available.
[2019-05-06 05:36:57,749]  INFO {org.wso2.carbon.metrics.core.config.model.JmxReporterConfig} - Creating JMX reporter for Metrics with domain 'org.wso2.carbon.metrics'
[2019-05-06 05:36:57,779]  INFO {org.wso2.msf4j.analytics.metrics.MetricsComponent} - Metrics Component is activated
[2019-05-06 05:36:57,784]  INFO {org.wso2.carbon.databridge.agent.internal.DataAgentDS} - Successfully deployed Agent Server 
[2019-05-06 05:36:58,292]  INFO {io.siddhi.distribution.event.simulator.core.service.CSVFileDeployer} - CSV file deployer initiated.
[2019-05-06 05:36:58,295]  INFO {io.siddhi.distribution.event.simulator.core.service.SimulationConfigDeployer} - Simulation config deployer initiated.
[2019-05-06 05:36:58,331]  INFO {org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiverServiceComponent} - org.wso2.carbon.databridge.receiver.binary.internal.Service Component is activated
[2019-05-06 05:36:58,342]  INFO {org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver} - Started Binary SSL Transport on port : 9712
[2019-05-06 05:36:58,343]  INFO {org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver} - Started Binary TCP Transport on port : 9612
[2019-05-06 05:36:58,343]  INFO {org.wso2.carbon.databridge.receiver.thrift.internal.ThriftDataReceiverDS} - Service Component is activated
[2019-05-06 05:36:58,360]  INFO {org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver} - Thrift Server started at 0.0.0.0
[2019-05-06 05:36:58,369]  INFO {org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver} - Thrift SSL port : 7711
[2019-05-06 05:36:58,371]  INFO {org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver} - Thrift port : 7611
[2019-05-06 05:36:58,466]  INFO {org.wso2.msf4j.internal.MicroservicesServerSC} - All microservices are available
[2019-05-06 05:36:58,567]  INFO {org.wso2.transport.http.netty.contractimpl.listener.ServerConnectorBootstrap$HttpServerConnector} - HTTP(S) Interface starting on host 0.0.0.0 and port 9090
[2019-05-06 05:36:58,574]  INFO {org.wso2.transport.http.netty.contractimpl.listener.ServerConnectorBootstrap$HttpServerConnector} - HTTP(S) Interface starting on host 0.0.0.0 and port 9443
[2019-05-06 05:36:59,091]  INFO {org.wso2.transport.http.netty.contractimpl.listener.ServerConnectorBootstrap$HttpServerConnector} - HTTP(S) Interface starting on host 0.0.0.0 and port 8280
[2019-05-06 05:36:59,092]  INFO {org.wso2.extension.siddhi.io.http.source.HttpConnectorPortBindingListener} - HTTP source 0.0.0.0:8280 has been started
[2019-05-06 05:36:59,093]  INFO {io.siddhi.distribution.core.internal.StreamProcessorService} - Siddhi App MonitorApp deployed successfully
[2019-05-06 05:36:59,100]  INFO {org.wso2.carbon.kernel.internal.CarbonStartupHandler} - Siddhi Runner Distribution started in 4.710 sec
[2019-05-06 05:39:33,804]  INFO {io.siddhi.core.stream.output.sink.LogSink} - LOGGER : Event{timestamp=1557121173802, data=[monitored, 001, 341], isExpired=false}
```
