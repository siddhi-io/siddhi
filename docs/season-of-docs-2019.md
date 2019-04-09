# Google Season of Docs 2019


Siddhi project is excited to participate in the first Google Season of Docs program as a mentoring organization to foster 
open source collaboration with technical writers on Siddhi documentation. We are pleased to introduce the below 
project proposals to encourage you to participate and become a contributor in the Siddhi open source community.

If you are interested in working on any of the below projects and if you need further information, please feel free to 
contact the mentors listed under each proposal below, or write to us via siddhi-dev@googlegroups.com .

Happy documenting with Google Season of Docs!

***

## Proposal 1: CI/CD Guideline for Siddhi Stream Processor

### Description
Siddhi is an open source cloud native stream processing engine [1]. The Siddhi engine is integrated with various other 
open source and proprietary projects such as Apache Eagle [1], Apache Flink [2], UBER [3] and Punch Platform [4] and etc. 
Other than that, the Siddhi engine also plays the primary role in various WSO2 products such as WSO2 Stream Processor, 
WSO2 Data Analytics Server, WSO2 API Manager (Traffic Manager component),  etc.

Siddhi Query Language is an SQL-like query language that defines a logic for analyzing high volumes of data generated 
in high speed and in real time. Siddhi documentation [2] currently describes the Siddhi architecture, main concepts, 
features, the Siddhi grammar, and how to use it.

However, at present, we lack proper documentation to explain the complete life cycle of the CI-CD process. We need 
detailed and comprehensive documentation that explains this process. When writing this documentation, you are also 
expected to try out the product and gather some user experience around it and make sure that it is captured properly 
in the documentation.

### Deliverables
A comprehensive document that explains the complete CI/CD pipeline. This should specifically cover areas related to 
Docker-based deployment.

### Skills Needed

- A reasonable knowledge of how to work with Github
- A good command of English
- A clear understanding of the CI/CD pipeline

### References
[1] - [Siddhi Github repository](https://github.com/siddhi-io)

[2] - [Siddhi query guide](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/) -  The applicant 
can use this to understand the Siddhi Concepts.

[3] - [Writing Tips](https://writing-fundamentals-guide.readthedocs.io/en/latest/) - This guide contains the 
documentation guidelines we follow at WSO2.

[4] - [Microsoft Style Guide](https://docs.microsoft.com/en-us/style-guide/welcome/) - Download this guide to study 
globally recognized styling standards.

### Possible Mentors

- Sriskandarajah  Suhothayan (suho@wso2.com)
- Chiran Fernando (chiran@wso2.com)
- Rukshani Weerasinha (rukshani@wso2.com)


## Proposal 2: Documentation on Event Driven Approaches

### Description
Siddhi is an open source cloud native stream processing engine [1]. The Siddhi engine is integrated with various other 
open source and proprietary projects such as Apache Eagle [1], Apache Flink [2], UBER [3], Punch Platform [4], etc. 
Other than that, the Siddhi engine also plays the primary role in various WSO2 products such as WSO2 Stream Processor, 
WSO2 Data Analytics Server, WSO2 API Manager (Traffic Manager component), etc.

Siddhi Query Language is an SQL-like query language that defines a logic for analyzing high volumes of data generated 
in high speed and in real time. Siddhi documentation [2] currently describes the Siddhi architecture, main concepts, 
features, the Siddhi grammar, and how to use it.

We could perform extensive operations for streaming data integration purposes in Siddhi such as CDC 
(Change Data Capture), throttling and notification manager. Even though there are few resources covering these areas, 
there is no  end-end to documentation that explains these use cases. We are expecting the applicant to understand these 
use cases, come up with some good examples, and write a comprehensive guide around the topics mentioned.

### Deliverables
A comprehensive document explains the below streaming data integration use cases.
- Chance Data Capture (CDC)
- Throttling
- Notification Manager

### Skills Needed

- A reasonable knowledge of how to work with Github
- A good command of English
- A basic understanding of CDC, throttling and notifications

### References
[1] - [Siddhi Github repository](https://github.com/siddhi-io)

[2] - [Siddhi query guide](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/) -  The applicant 
can use this to understand the Siddhi Concepts.

[3] - [Writing Tips](https://writing-fundamentals-guide.readthedocs.io/en/latest/) - This guide contains the 
documentation guidelines we follow at WSO2.

[4] - [Microsoft Style Guide](https://docs.microsoft.com/en-us/style-guide/welcome/) - Download this guide to study 
globally recognized styling standards.

### Possible Mentors

- Mohandarshan Vivekanandalingam (mohan@wso2.com)
- Damith Wickramasinghe (damithn@wso2.com)
- Rukshani Weerasinha (rukshani@wso2.com)

## Proposal 3: Guide on Siddhi Stream Processor Monitoring

### Description
Siddhi is an open source cloud native stream processing engine [1]. The Siddhi engine is integrated with various other 
open source and proprietary projects such as Apache Eagle [1], Apache Flink [2], UBER [3], Punch Platform [4],  etc. 
Other than that, the Siddhi engine also plays the primary role in various WSO2 products such as WSO2 Stream Processor, 
WSO2 Data Analytics Server, WSO2 API Manager (Traffic Manager component), etc.

Siddhi Query Language is an SQL-like query language that defines a logic for analyzing high volumes of data generated 
in high speed and in real time. Siddhi documentation [2] currently describes the Siddhi architecture, main concepts, 
features, the Siddhi grammar, and how to use it.

Monitoring the live Siddhi Stream Processor engine is one of the key requirements of the users. At present, this 
requirement is addressed via Prometheus that monitors Siddhi Stream Processor, allowing users to  understand the
 real live environment and take necessary actions based on the load and the TPS of the system. Currently, we need 
 end to end documentation explaining these monitoring capabilities. 

### Deliverables
A comprehensive document that explains the monitoring capabilities of Siddhi Stream Processor. This documentation 
should cover the integration use cases with Prometheus. 

### Skills Needed

- A reasonable knowledge of how to work with Github
- A good command of English
- Basic knowledge of Prometheus [5]

### References
[1] - [Siddhi Github repository](https://github.com/siddhi-io)

[2] - [Siddhi query guide](https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/) -  The applicant 
can use this to understand the Siddhi Concepts.

[3] - [Writing Tips](https://writing-fundamentals-guide.readthedocs.io/en/latest/) - This guide contains the 
documentation guidelines we follow at WSO2.

[4] - [Microsoft Style Guide](https://docs.microsoft.com/en-us/style-guide/welcome/) - Download this guide to study 
globally recognized styling standards.

[5] - [Prometheus](https://prometheus.io/)

### Possible Mentors

- Tishan Dahanayakage (tishan@wso2.com)
- Niveathika Rajendran (niveathika@wso2.com )
- Rukshani Weerasinha (rukshani@wso2.com)
