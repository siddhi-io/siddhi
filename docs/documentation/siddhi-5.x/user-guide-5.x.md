# Siddhi 5.x User Guide

## System Requirements
1. **Memory**   - 128 MB (minimum), 500 MB (recommended), higher memory might be needed based on in-memory data stored for processing
2. **Cores**    - 2 cores (recommended), use lower number of cores after testing Siddhi Apps for performance 
3. **JDK**      - 8 or 11
4. To build Siddhi from the Source distribution, it is necessary that you have JDK version 8 or 11 and Maven 3.0.4 or later

## Using Siddhi in various environments 

### **Using Siddhi as a Java library**

* Find a sample Siddhi project that's implemented as Java application using Maven [here](https://github.com/suhothayan/siddhi-sample), you can use that as a reference for your implementation.

* It contains following dependencies in its `pom.xml` file.

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
  
* And contains the following Siddhi application in its main method. 

<script src="https://gist.github.com/suhothayan/0c81a843101601e82a7175b7add2aa4d.js"></script>

### **Using Siddhi as Local Micro Service**

WIP

### **Using Siddhi as Docker Micro Service**

WIP

### **Using Siddhi as kubernetes Micro Service**

WIP

### **Using Siddhi as a Python Library**

WIP
