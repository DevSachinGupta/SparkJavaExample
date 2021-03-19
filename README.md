<!-- PROJECT SHIELDS -->
<!--
*** I'm using markdown "reference style" links for readability.
*** Reference links are enclosed in brackets [ ] instead of parentheses ( ).
*** See the bottom of this document for the declaration of the reference variables
*** for contributors-url, forks-url, etc. This is an optional, concise syntax you may use.
*** https://www.markdownguide.org/basic-syntax/#reference-style-links
-->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]

<!-- PROJECT LOGO -->
<br />
<p align="center">
  <a href="https://github.com/DevSachinGupta/SparkJavaExample" align="center">
    <img src="images/spark1.png" alt="Logo" width="250" height="150">
	<img src="images/plus.png" alt="Logo" width="80" height="80" style="margin-bottom: 2%;">
	<img src="images/java1.png" alt="Logo" width="250" height="150">
  </a>

  <h3 align="center">SparkJavaExample</h3>

  <p align="center">
    Basically java based Apache Spark Examples.
    <br />
    <a href="https://github.com/DevSachinGupta/SparkJavaExample"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/DevSachinGupta/SparkJavaExample">View Demo</a>
    ·
    <a href="https://github.com/DevSachinGupta/SparkJavaExample/issues">Report Bug</a>
    ·
    <a href="https://github.com/DevSachinGupta/SparkJavaExample/issues">Request Feature</a>
  </p>
</p>

<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgements">Acknowledgements</a></li>
  </ol>
</details>


## About The Project

This contains Java based spark examples.

Tried using JavaRDD to read csv file and parsing.

## Built With

* [Java 8](https://www.oracle.com/in/java/technologies/javase/javase-jdk8-downloads.html) - The application development technology
* [Apache Spark](https://spark.apache.org/) - The platform to run the spark
* [Maven](https://maven.apache.org/) - Dependency Management


## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.


### Prerequisites

What things you need to install the software and how to install them

* Java 8
	1. On Windows follow the Installer.
	2. On Ubuntu
	```
		sudo apt install openjdk-8-jdk
	```
* Apache Spark
	Download spark from https://spark.apache.org/
		In this we are using spark 3.1.1 prebuilt for hadoop 3.2
	
	Untar files to folder
	```
		SPARK_HOME=<Path-to-spark-extracted-folder>
		HADOOP_HOME=<Path-to-spark-extracted-folder>
	```
* Eclipse
	Download eclipse from https://www.eclipse.org/downloads/


	
### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why

```
Give an example
```

### And coding style tests

Explain what these tests test and why

```
Give an example
```

## Deployment

```
spark-submit --class "me.sachingupta.sparkexamples.Main" --deploy-mode client --master local\[\*\] target/sparkexamples-0.0.1-SNAPSHOT.jar
```

Add additional notes about how to deploy this on a live system


## Contributing

Please read [CONTRIBUTING.md](https://gist.github.com/PurpleBooth/b24679402957c63ec426) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/DevSachinGupta/SparkJavaExample/tags). 

## Authors

* **Sachin Gupta** - *Initial work* - [PurpleBooth](https://github.com/DevSachinGupta)

See also the list of [contributors](https://github.com/DevSachinGupta/SparkJavaExample/graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/DevSachinGupta/SparkJavaExample.svg?style=for-the-badge
[contributors-url]: https://github.com/DevSachinGupta/SparkJavaExample/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/DevSachinGupta/SparkJavaExample.svg?style=for-the-badge
[forks-url]: https://github.com/DevSachinGupta/SparkJavaExample/network/members
[stars-shield]: https://img.shields.io/github/stars/DevSachinGupta/SparkJavaExample.svg?style=for-the-badge
[stars-url]: https://github.com/DevSachinGupta/SparkJavaExample/stargazers
[issues-shield]: https://img.shields.io/github/issues/DevSachinGupta/SparkJavaExample.svg?style=for-the-badge
[issues-url]: https://github.com/DevSachinGupta/SparkJavaExample/issues
[license-shield]: https://img.shields.io/github/license/DevSachinGupta/SparkJavaExample.svg?style=for-the-badge
[license-url]: https://github.com/DevSachinGupta/SparkJavaExample/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/DevSachinGupta
[product-screenshot]: images/screenshot.png