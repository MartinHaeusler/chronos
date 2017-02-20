<img src="https://github.com/MartinHaeusler/chronos/blob/master/readmeResources/logo_chronograph.png" width="400">

About ChronoGraph
=================

ChronoGraph is one of the major components of [Chronos](https://github.com/MartinHaeusler/chronos). It is a versioned graph database and an implementation of the [Apache TinkerPop AP](https://tinkerpop.apache.org/). As with all Chronos Components, ChronoGraph is written in 100% pure Java and should run in any environment supported by JRE 1.8 or later.


Feature Highlights
==================

 - TinkerPop OLTP Graph Database with transparent system time versioning support
 - Full ACID (isolation level "serializable")
 - Easy-to-use API
 - Works on five backends: Chunked (recommended), in-memory (for easy unit testing), [TUPL](https://github.com/cojen/Tupl) (fast local store), [MapDB](http://www.mapdb.org/) (maintaind) and SQL (experimental)
 - Temporal secondary indexing
 - Full Gremlin support
 - Temporal entry caching
 - Temporal query caching
 - Lightweight branching support
 - Incremental commits for loading large batches of data
 - Equal performance on all timestamps (no performance penalty for queries on historical data)
 - Writing DB dumps into a configurable plain-text format (XML), and reading them again
 - Easy configuration via `*.properties` files or code
 - No background threads, no `static` magic, instantiate the database as often as needed within the same JVM
 - Thoroughly unit-tested
 - Full JavaDoc on the public API

Getting Started
===============

ChronoGraph is currently available from the Sonatype Snapshots repository.

## Installing with Maven
Add the following to the `<dependencies>` section in your `pom.xml`:

```xml
<dependency>
  	<groupId>com.github.martinhaeusler</groupId>
  	<artifactId>org.chronos.chronograph</artifactId>
  	<version>0.5.2-SNAPSHOT</version>
 </dependency>
```

In case your `pom.xml` does not reference the Sonatype Snapshots repository already, you also have to add the following to the `<repositories>` section:

```xml
  <repositories>
  	<repository>
        <id>Sonatype</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
        <releases>
           <enabled>true</enabled>
        </releases>
        <snapshots>
          <enabled>true</enabled>
        </snapshots>
     </repository>
  </repositories>
```

## Installing with Gradle
Add the following line to the `dependencies` section in your `build.gradle` file:

```groovy
compile group: 'com.github.martinhaeusler', name: 'org.chronos.chronograph', version: '0.5.2-SNAPSHOT'
```

In case your `build.gradle` file does not already reference the Sonatype Snapshots repository, you also have to add the following to your `build.gradle`:

```groovy
repositories {
    maven {
    	url "https://oss.sonatype.org/content/repositories/snapshots/"
    }
}
```

## Running some basic examples

ChronoGraph employs an API design which we like to call a *Forward API*. It is designed to make the best possible use of code completion in an IDE, such as Eclipse, IntelliJ IDEA, Netbeans, or others. The key concept is to start with a simple object, and the rest of the API unfolds via code completion.

Let's create a new instance of ChronoGraph. Following the Forward API principle, you start simple - with the `ChronoGraph` interface. Code completion will reveal the static `FACTORY` field. From there, it's a fluent builder pattern:
   
```java
   ChronoGraph graph = ChronoGraph.FACTORY.create().inMemoryGraph().build();
```
The builder above has several different options, depending on your chosen backend. Check your code completion and JavaDoc for details. With this new instance, we can create a transaction (ChronoGraph is fully ACID compliant) and add some data through plain-old TinkerPop API calls:

```java
   ChronoGraph graph = ChronoGraph.FACTORY.create().inMemoryGraph().build();
   graph.tx.open(); // opening transactions also happens implicitly, but the preferred way is to do it explicitly.
   Vertex vMe = graph.addVertex("kind", "Person", "firstName", "Martin", "lastName", "Haeusler");
   Vertex uibk = graph.addVertex("kind", "University", "name", "University of Innsbruck");
   vMe.addEdge("location", uibk);
   graph.tx().commit();
```

So far, so normal. However, let's modify something...

```java
   graph.tx.open();
   // delete my previous location
   vMe.edges(Direction.OUT, "location").forEachRemaining(edge -> edge.remove());
   // add a new location...
   Vertex home = graph.addVertex("kind", "House", "name", "home");
   // ... and relocate me
   vMe.addEdge("location", home);
   graph.tx().commit();
```

Now, what ChronoGraph can do (among many other things) is to ask the basic question: where have I been?


```java
  Iterator<Long> changeTimestamps = graph.getVertexHistory(vMe);
  List<String> locations = new ArrayList<>();
  changeTimestamps.forEachRemaining(t -> {
     // open a transaction on the given timestamp
     g.tx().open(t);
     try{
       // check where I have been at timestamp 't' (note: we are querying the old graph version here!)
       vMe.vertices(Direction.OUT, "location").forEachRemaining(v -> locations.add(v.value("name")));
     }finally{
       // close the transaction again
       g.tx().close();
     }
  })
  // print it
  System.out.println(locations);  // prints "home" and "University of Innsbruck"
```

This example is just a very basic one to get you started. All temporal features of ChronoGraph are available directly via methods on the `ChronoGraph` interface. All other features work as usual with the Gremlin API.

Frequently Asked Questions
==========================

### Is it stable?
The public API of ChronoGraph is quite stable and not very likely to change drastically any time soon. However, the **persistence format is subject to change without prior notice**. ChronoGraph is a research project after all.

### Which property value types are supported?
Note that the `value` in `vertex.property(key, value)` can be *any* Java object. The only constraint is that it must be (de-)serializable by the [Kryo Serializer](https://github.com/EsotericSoftware/kryo) which is employed internally. Usually, this means that:
 - An object is serializable if it has a default constructor, and no final fields, and...
 - ... only refers to other objects if they are serializable as well.

Any object that follows the Java Beans pattern is fair game here, as well as any primitive Java type (e.g. `int`, `float`, `double`...), primitive wrapper class (e.g. `Integer`, `Float`, ...), most collections (e.g. `java.util.HashSet`, `java.util.ArrayList`...) and many other classes in the JDK (including `String`, `java.util.Date`, and many more). However, we **strongly recommend** to keep it simple and stick to primitives and collections of primitives whenever possible to avoid trouble.

In general, `null` is **never a valid value**. Delete the property instead.

### What about licensing?
ChronoDB is provided as-is under the aGPLv3 license, just like all other [Chronos](https://github.com/MartinHaeusler/chronos) components (unless noted explicitly)
