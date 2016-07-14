<img src="https://github.com/MartinHaeusler/chronos/blob/master/readmeResources/chronoDBLogo.png" width="400">

About ChronoDB
==============

ChronoDB is one of the major components of [Chronos](https://github.com/MartinHaeusler/chronos). It is a key-value store with versioning and associated temporal features. As with all Chronos Components, ChronoDB is written in 100% pure Java and should run in any environment supported by JRE 1.8 or later.

Getting Started
===============

ChronoDB will very soon be available via maven. Until then, clone the repository and run `gradlew.build` in the project root directory.
*(The maven coordinates will be shown here as soon as it is available on the repository)*

ChronoDB employs an API design which we like to call a *Forward API*. It is designed to make the best possible use of code completion in an IDE, such as Eclipse, IntelliJ IDEA, Netbeans, or others. The key concept is to start with a simple object, and the rest of the API unfolds via code completion.

Let's create a new instance of ChronoDB. Following the Forward API principle, you start simple - with the `ChronoDB` interface. Code completion will reveal the static `FACTORY` field. From there, it's a fluent builder pattern:
   
   ```java
   ChronoDB db = ChronoDB.FACTORY.create().inMemoryDatabase().build();
   ```
    
With this new instance, we can create a transaction (ChronoDB is fully ACID compliant) and add some data:

    ChronoDBTransaction tx = db.tx();
    tx.put("Hello", "World);       // writes to the default keyspace
    tx.put("Math", "Pi", 3.1415);  // writes to the "Math" keyspace
    tx.commit();

Note that the `value` in `tx.put(key, value)` can be *any* Java object. The only constraint is that it must be (de-)serializable by the [Kryo Serializer](https://github.com/EsotericSoftware/kryo) which is employed internally. Usually, this means that:
 - An object is serializable if it has a default constructor, and no final fields, and...
 - ... only refers to other objects if they are serializable as well.

Any object that follows the Java Beans pattern is fair game here, as well as any primitive Java type (e.g. `int`, `float`, `double`...), primitive wrapper class (e.g. `Integer`, `Float`, ...), most collections (e.g. `java.util.HashSet`, `java.util.ArrayList`...) and many other classes in the JDK (including `String`, `java.util.Date`, and many more). However, we **strongly recommend** to keep it simple and stick to primitives and collections of primitives whenever possible to avoid trouble.

The mindful reader may have witnessed that the code above looks like a regular key-value store. There is nothing "temporal" about it. Indeed, the versioning in ChronoDB is fully transparent to the client programmer. Unless you explicitly **want** to deal with the temporal aspects yourself (e.g. for temporal queries), the ChronoDB API will take care of it for you.




Frequently Asked Questions
==========================

**Is it stable?**
The public API of ChronoDB is quite stable and not very likely to change drastically any time soon. However, the **persistence format is subject to change without prior notice**. ChronoDB is a research project after all.
