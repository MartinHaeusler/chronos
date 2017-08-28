package org.chronos.benchmarks.chronosphere.socialnetwork;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest.DontRunWithBackend;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.query.Order;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Statistic;
import org.chronos.common.test.utils.TimeStatistics;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(PerformanceTest.class)
@DontRunWithBackend({ ChronosBackend.INMEMORY, ChronosBackend.JDBC, ChronosBackend.MAPDB, ChronosBackend.TUPL })
public class SocialNetworkBenchmark extends AllChronoSphereBackendsTest {

	@Test
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "100000")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
	@InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "100")
	@InstantiateChronosWith(property = ChronoDBConfiguration.ENABLE_BLIND_OVERWRITE_PROTECTION, value = "false")
	@InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
	@InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
	public void socialNetworkBenchmark() throws Exception {
		final List<EPackage> ePackages;
		{
			String path = "ecoremodels/socialnetwork.ecore";
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream(path);
			String ecoreContents = IOUtils.toString(stream, StandardCharsets.UTF_8);
			ePackages = EMFUtils.readEPackagesFromXMI(ecoreContents);
		}
		ChronoSphereInternal sphere = this.getChronoSphere();
		sphere.getEPackageManager().registerOrUpdateEPackages(ePackages);
		try (ChronoSphereTransaction tx = sphere.tx()) {
			EAttribute firstName = tx.getEAttributeByQualifiedName("socialnetwork::user::Person#firstName");
			sphere.getIndexManager().createIndexOn(firstName);
			tx.commit();
		}
		System.out.println("SocialNetwork Ecore metamodel loaded.");

		final String xmiContent;
		{
			String path = "ecoremodels/socialnetwork200k.xmi";
			InputStream stream = this.getClass().getClassLoader().getResourceAsStream(path);
			xmiContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
		}
		System.out.println("XMI loaded to string.");
		try (ChronoSphereTransaction tx = sphere.tx()) {
			Set<EPackage> packs = tx.getEPackages();
			List<EObject> eObjectsFromXMI = EMFUtils.readEObjectsFromXMI(xmiContent, packs);
			System.out.println("EObjects loaded from XMI");
			long timeBeforeLoad = System.currentTimeMillis();
			tx.attach(eObjectsFromXMI);
			tx.commit();
			long timeAfterLoad = System.currentTimeMillis();
			System.out.println("Loading the 200k SocialNetwork XMI took " + (timeAfterLoad - timeBeforeLoad) + "ms.");
		}

		Statistic ymkStats = new Statistic();
		Statistic yflStats = new Statistic();
		Statistic myaStats = new Statistic();
		Statistic ubnStats = new Statistic();

		System.out.println("BEGINNING WARMUP");

		// warmup
		for (int i = 0; i < 5; i++) {
			this.runYouMayAlsoKnowBenchmark(sphere);
			this.runYourFriendsAlsoLiked(sphere);
			this.runMyActivities(sphere);
			this.runUserByName(sphere);
		}

		System.out.println("WARMUP COMPLETE");

		// actual benchmark runs
		for (int i = 0; i < 10; i++) {
			ymkStats.addSample(this.runYouMayAlsoKnowBenchmark(sphere));
			yflStats.addSample(this.runYourFriendsAlsoLiked(sphere));
			myaStats.addSample(this.runMyActivities(sphere));
			ubnStats.addSample(this.runUserByName(sphere));
		}

		// print the results
		System.out.println("YMK: " + new TimeStatistics(ymkStats).toCSV());
		System.out.println("YFL: " + new TimeStatistics(yflStats).toCSV());
		System.out.println("MYA: " + new TimeStatistics(myaStats).toCSV());
		System.out.println("UBN: " + new TimeStatistics(ubnStats).toCSV());

		System.out.println("YMK Samples: " + new TimeStatistics(ymkStats).getRuntimes());
		System.out.println("YFL Samples: " + new TimeStatistics(yflStats).getRuntimes());
		System.out.println("MYA Samples: " + new TimeStatistics(myaStats).getRuntimes());
		System.out.println("UBN Samples: " + new TimeStatistics(ubnStats).getRuntimes());

		// System.out.println("PRESS ENTER TO EXIT");
		// try (Scanner scanner = new Scanner(System.in)) {
		// scanner.nextLine();
		// }
	}

	private long runYouMayAlsoKnowBenchmark(final ChronoSphereInternal sphere) {
		try (ChronoSphereTransaction tx = sphere.tx()) {
			EClass person = tx.getEClassBySimpleName("Person");
			EReference friends = EMFUtils.getEReference(person, "friends");
			List<EObject> persons = tx.find().startingFromInstancesOf(person).toList();
			Collections.shuffle(persons);
			TimeStatistics statistics = new TimeStatistics();
			long sum = 0;
			for (int i = 0; i < 10_000; i++) {
				EObject p = persons.get(i);
				statistics.beginRun();
				Set<EObject> result = tx.find()
						//
						.startingFromEObject(p)
						//
						.eGet(friends).named("myfriends")
						//
						.eGet(friends).except("myfriends")
						//
						.toSet();
				statistics.endRun();
				sum += result.size();
				// System.out.println("YMK: " + EMFUtils.eGet(p, "firstName") + " " + EMFUtils.eGet(p, "lastName") + "
				// :: "
				// + result.size());
			}
			System.out.println("YOU MAY ALSO KNOW");
			System.out.println("   Time: " + statistics.getTotalTime() + "ms");
			System.out.println("   Hits: " + sum);
			return statistics.getTotalTime();
		}
	}

	private long runYourFriendsAlsoLiked(final ChronoSphereInternal sphere) {
		try (ChronoSphereTransaction tx = sphere.tx()) {
			EClass person = tx.getEClassBySimpleName("Person");
			EReference friends = EMFUtils.getEReference(person, "friends");
			EClass reaction = tx.getEClassBySimpleName("Reaction");
			EReference author = EMFUtils.getEReference(reaction, "author");
			EAttribute type = EMFUtils.getEAttribute(reaction, "type");
			EEnum reactionType = (EEnum) tx.getEClassifierBySimpleName("ReactionType");
			EEnumLiteral reactionTypeLike = reactionType.getEEnumLiteral("Like");
			EReference post = EMFUtils.getEReference(reaction, "post");

			List<EObject> persons = tx.find().startingFromInstancesOf(person).toList();
			Collections.shuffle(persons);
			TimeStatistics statistics = new TimeStatistics();
			long sum = 0;
			for (int i = 0; i < 20; i++) {
				EObject p = persons.get(i);
				statistics.beginRun();
				Set<EObject> postsIReactedTo = tx.find()
						//
						.startingFromEObject(p).
						//
						eGetInverse(author).isInstanceOf(reaction)
						//
						.eGet(post).toSet();
				Set<EObject> result = tx.find()
						//
						.startingFromEObject(p)
						//
						.eGet(friends)
						//
						.eGetInverse(author).isInstanceOf(reaction)
						//
						.has(type, reactionTypeLike)
						//
						.eGet(post)
						//
						.except(postsIReactedTo).toSet();
				statistics.endRun();
				sum += result.size();
				// System.out.println("YFL: " + EMFUtils.eGet(p, "firstName") + " " + EMFUtils.eGet(p, "lastName") + "
				// :: "
				// + result.size());
			}
			System.out.println("YOUR FRIENDS ALSO LIKE");
			System.out.println("   Time: " + statistics.getTotalTime() + "ms");
			System.out.println("   Hits: " + sum);
			return statistics.getTotalTime();
		}
	}

	private long runMyActivities(final ChronoSphere sphere) {
		try (ChronoSphereTransaction tx = sphere.tx()) {
			EClass person = tx.getEClassBySimpleName("Person");
			EClass reaction = tx.getEClassBySimpleName("Reaction");
			EAttribute date = EMFUtils.getEAttribute(reaction, "date");
			EReference author = EMFUtils.getEReference(reaction, "author");

			List<EObject> persons = tx.find().startingFromInstancesOf(person).toList();
			Collections.shuffle(persons);
			TimeStatistics statistics = new TimeStatistics();
			long sum = 0;
			for (int i = 0; i < 200; i++) {
				EObject p = persons.get(i);
				statistics.beginRun();
				Set<EObject> result = tx.find()
						//
						.startingFromEObject(p)
						//
						.eGetInverse(author)
						//
						.orderBy(date, Order.DESC).toSet();

				statistics.endRun();
				// System.out.println("MYA: " + EMFUtils.eGet(p, "firstName") + " " + EMFUtils.eGet(p, "lastName") + "
				// :: "
				// + result.size());
				sum += result.size();
			}
			System.out.println("MY ACTIVITIES");
			System.out.println("   Time: " + statistics.getTotalTime() + "ms");
			System.out.println("   Hits: " + sum);
			return statistics.getTotalTime();
		}
	}

	private long runUserByName(final ChronoSphere sphere) {
		try (ChronoSphereTransaction tx = sphere.tx()) {
			EClass person = tx.getEClassBySimpleName("Person");
			EAttribute eaFirstName = EMFUtils.getEAttribute(person, "firstName");
			long timeBefore = System.currentTimeMillis();
			Set<EObject> johns = tx.find().startingFromEObjectsWith(eaFirstName, "John").toSet();
			long timeAfter = System.currentTimeMillis();
			System.out.println("FIND BY FIRST NAME");
			System.out.println("   Time: " + (timeAfter - timeBefore) + "ms");
			System.out.println("   Hits: " + johns.size());
			return timeAfter - timeBefore;
		}
	}

}
