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
* Dependency management via Maven / Gradle. No Eclipse or OSGi environment needed.
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
## Starting a new instance

ChronoSphere employs an API design which we like to call a *Forward API*. It is designed to make the best possible use of code completion in an IDE, such as Eclipse, IntelliJ IDEA, Netbeans, or others. The key concept is to start with a simple object, and the rest of the API unfolds via code completion.

Let's create a new instance of ChronoSphere. Following the Forward API principle, you start simple - with the `ChronoSphere` interface. Code completion will reveal the static `FACTORY` field. From there, it's a fluent builder pattern:
   
```java
    ChronoSphere repository = ChronoSphere.FACTORY.create().inMemoryRepository().build();
    
    // remember to close this repository once you are done working with it.
    repository.close();
```

Note that the builder pattern employed above has many other options, and several different backends are supported. For a list of supported backends, please check the code completion.

After starting up a ChronoSphere instance, you should check the registered `EPackage`s:

```java
    Set<EPackage> ePackages = sphere.getEPackageManager().getRegisteredEPackages();
    if(ePackages.isEmpty()){
        // no EPackages are registered, add the EPackage(s) you want to work with.
        sphere.getEPackageManager().registerOrUpdateEPackage(myEPackage);
    }
```

Please note that **no code generation is required** when working with ChronoSphere. The **preferred** way of interacting with Ecore in ChronoSphere is to use the *Reflective API* (e.g. `eObject.eGet(...)` and `eObject.eSet(...)`).

## Transactions

In order to perform actual work on a ChronoSphere instance, you need to make use of `Transaction`s. A transaction is a unit of work that will be executed on the repository according to the [ACID](https://en.wikipedia.org/wiki/ACID) properties. You also profit from the highest isolation level ("serializable", a.k.a. *snapshot isolation*), which means that parallel transactions will never interfere with each other in unpredictable ways.

To open a transaction on a ChronoSphere instance, call the `tx()` method, like so:

```java
    ChronoSphere repository = ...;
    try(ChronoSphereTransaction tx = repository.tx()){
        // perform work here
    }
```

The example above makes use of Java's [try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html) statement. It *automatically* closes the transaction for you. 

**/!\ Attention**: When a transaction is **closed** by the `try-with-resources` statement, any changes will be **rolled back** (i.e. undone), unless you call `tx.commit()` before reaching the end of the `try` block.

Let's add some actual EObjects to our repository:

```java
    ChronoSphere repository = ...;
    try(ChronoSphereTransaction tx = repository.tx()){
        EObject eObject = createSomeEObject();
        tx.attach(eObject);
        tx.commit();
    }
```

In the method `createSomeEObject` above, you can create virtually any `EObject` of your choice. As long as it is a syntactically valid `EObject` and adheres to the Ecore contract, it will work with ChronoSphere. Afterwards, we call `attach` in order to add this `EObject` to our repository. There are several overloads for `attach`, e.g. one that accepts `Iterable`s of `EObject`s for your convenience, in case that you want to add multiple elements at once. **Don't forget to call `commmit` to save your changes!**
