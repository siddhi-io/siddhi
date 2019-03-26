# How to Contribute

Siddhi is a java library that listens to events from data streams, detects complex conditions described via a **Streaming
 SQL language**, and triggers actions. It performs both **_Stream Processing_** and **_Complex Event Processing_**.

We are an open-source project under Apache license and the work of hundreds of contributors.

We always appreciate and welcome your help. You can contribute to siddhi in various ways; please find them below.


## Communicating with the team

[Siddhi-Dev Google Group](https://groups.google.com/forum/#!forum/siddhi-dev) Group is the main Siddhi project discussion forum for developers.

Users can you [Siddhi-User Google Group](https://groups.google.com/forum/#!forum/siddhi-user) to raise any queries and get some help to achieve their usecases.

[StackOverflow](https://stackoverflow.com/questions/tagged/siddhi) is also can be used to get some support, and GitHub for issues and code repositories.


## Reporting Issues

If you are unsure whether you have found a bug, please consider searching existing issues in github and asking in Siddhi-Dev Google Group.

To file non-security issues:

1. Click the **Issues** tab in the github repository,

2. Click the **New Issue** button,

3. Fill out all sections in the issue template and submit.


## Contributing Code

### Accepting Contributor License Agreement (CLA)

Before you submit your first contribution please accept our Contributor License Agreement (CLA) here. When you send your first Pull Request (PR), GitHub will ask you to accept the CLA.

There is no need to do this before you send your first PR.

Subsequent PRs will not require CLA acceptance.

If for some (unlikely) reason at any time CLA changes, you will get presented with the new CLA text on your first PR after the change.

### Obtaining the Source Code and Building the Project

- Get a clone or download source from [Github](https://github.com/siddhi-io/siddhi.git)
- Prerequisites: JDK 1.8.x or JDK 11.x.x and Maven 3.5.x version (Install [Maven](https://maven.apache.org/install.html))
- Run the Maven command `mvn clean install` from the Siddhi root directory:

  Command | Description
  --- | ---
  `mvn clean install` | Build and install the artifacts into the local repository.
  `mvn clean install -Dmaven.test.skip=true` | Build and install the artifacts into the local repository, without running any of the unit tests.

### Setting up the Developer Environment

You can use any of your preferred IDEs (eg: IntelliJ IDEA, Eclipse.. ). Make sure, that your IDE is configured with proper JDK and Maven settings. Import the source code in your IDE and do necessary code changes.
Then add necessary unit tests with respect to your changes. Finally, build the complete Siddhi project with tests and commit the changes to your Github folk once build is successful.

### Commit the Changes
We follow these commit message requirements:

* Separate subject from body with a blank line
* Limit the subject line to 50 characters
* Capitalize the subject line
* Do not end the subject line with a period
* Use the imperative mood in the subject line
* Wrap the body at 72 characters
* Use the body to explain what and why vs. how

Please find details at: [https://chris.beams.io/posts/git-commit/](https://chris.beams.io/posts/git-commit/)


## Proposing changes/improvements to Siddhi

Start with the discussion in the [Siddhi-Dev Google Group](https://groups.google.com/forum/#!forum/siddhi-dev).

Once there is enough consensus around the proposal, you will likely be asked to file an Issue in GitHub and label it as Proposal, to continue the discussion on details there.