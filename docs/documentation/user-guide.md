#User Guide

## **Using the Siddhi in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use Siddhi in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* All <a target="_blank" href="https://wso2.github.io/siddhi/extensions/">Siddhi extensions</a> are shipped by default with WSO2 Stream Processor.

* Refer the <a target="_blank" href="https://docs.wso2.com/display/SP400/Quick+Start+Guide">WSO2 SP Quick Start Guide</a> for more information.
 
## **Using Siddhi as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-quckstart-4.0/#using-siddhi-as-a-java-library">java library</a>**

* To embed Siddhi as a java library in your project add the following Siddhi libraries as maven dependencies to the `pom.xml` file.

```xml
    <dependency>
      <groupId>org.wso2.siddhi</groupId>
      <artifactId>siddhi-core</artifactId>
      <version>4.x.x</version>
    </dependency>
    <dependency>
      <groupId>org.wso2.siddhi</groupId>
      <artifactId>siddhi-query-api</artifactId>
      <version>4.x.x</version>
    </dependency>
    <dependency>
      <groupId>org.wso2.siddhi</groupId>
      <artifactId>siddhi-query-compiler</artifactId>
      <version>4.x.x</version>
    </dependency>
    <dependency>
      <groupId>org.wso2.siddhi</groupId>
      <artifactId>siddhi-annotations</artifactId>
      <version>4.x.x</version>
    </dependency>    
```

Add the following repository configuration to the same file.
  
```xml
   <repositories>
     <repository>
         <id>wso2.releases</id>
         <name>WSO2 Repository</name>
         <url>http://maven.wso2.org/nexus/content/repositories/releases/</url>
         <releases>
             <enabled>true</enabled>
             <updatePolicy>daily</updatePolicy>
             <checksumPolicy>ignore</checksumPolicy>
         </releases>
     </repository>
   </repositories>
```

* The required dependencies can be downloaded from [here](http://maven.wso2.org/nexus/content/groups/wso2-public/org/wso2/siddhi/)
* Refer the [Siddhi Quick Start Guide](https://wso2.github.io/siddhi/documentation/siddhi-quckstart-4.0/#using-siddhi-as-a-java-library) for more how to setup and run a Siddhi Application.
 
## System Requirements
1. Minimum memory - 500 MB (based on in-memory data stored for processing)
2. Processor      - Pentium 800MHz or equivalent at minimum
3. Java SE Development Kit 1.8 (1.7 for 3.x version)
4. To build Siddhi from the Source distribution, it is necessary that you have
   JDK 1.8 version (1.7 for 3.x version) or later and Maven 3.0.4 or later
