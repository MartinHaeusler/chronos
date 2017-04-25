<img src="https://github.com/MartinHaeusler/chronos/blob/master/readmeResources/logo_chronosphere.png" width="400">

About ChronoSphere
==================

ChronoSphere is a [Model Repository](https://www.researchgate.net/profile/Juri_Rocco/publication/275637197_Collaborative_Repositories_in_Model-Driven_Engineering_Software_Technology/links/5540a98c0cf2320416ed0fd2.pdf) for models in the [Ecore](https://wiki.eclipse.org/Ecore) format defined in the [Eclipse Modeling Framework](http://www.eclipse.org/modeling/emf/) (EMF).

Core Features
=============

* Efficient storage of Ecore models with up to several 100,000 elements.
* Model versioning with commit-granularity.
* Lightweight Branching
* Model indexing and model-level query support.
* Developer-friendly API
* Dependency management via Maven / Gradle. No Eclipse / OSGi environment needed.
* Full support for dynamic Ecore (no source code generation required).
* Based on [ChronoGraph](https://github.com/MartinHaeusler/chronos/tree/master/org.chronos.chronograph), a versioned graph database.


Getting Started
===============

ChronoSphere is available from the Maven Central repository.

## Installing with Maven
Add the following to the `<dependencies>` section in your `pom.xml`:

```xml
<dependency>
  	<groupId>com.github.martinhaeusler</groupId>
  	<artifactId>org.chronos.chronosphere</artifactId>
  	<version>0.5.7</version>
 </dependency>
```

## Installing with Gradle
Add the following line to the `dependencies` section in your `build.gradle` file:

```groovy
compile group: 'com.github.martinhaeusler', name: 'org.chronos.chronosphere', version: '0.5.7'
```

In case your `build.gradle` file does not already reference the Maven Central Repository, you also have to add the following to your `build.gradle`:

```groovy
repositories {
    mavenCentral()
}
```

