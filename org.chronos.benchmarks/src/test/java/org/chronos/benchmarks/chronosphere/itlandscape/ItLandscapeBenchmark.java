package org.chronos.benchmarks.chronosphere.itlandscape;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.chronos.benchmarks.util.BenchmarkUtils;
import org.chronos.chronodb.api.Order;
import org.chronos.chronodb.internal.api.ChronoDBConfiguration;
import org.chronos.chronodb.internal.util.ChronosBackend;
import org.chronos.chronodb.test.base.AllBackendsTest.DontRunWithBackend;
import org.chronos.chronodb.test.base.InstantiateChronosWith;
import org.chronos.chronograph.internal.api.configuration.ChronoGraphConfiguration;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.query.Direction;
import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.utils.Statistic;
import org.chronos.common.test.utils.TimeStatistics;
import org.eclipse.emf.ecore.*;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Category(PerformanceTest.class)
@DontRunWithBackend({ChronosBackend.INMEMORY, ChronosBackend.JDBC, ChronosBackend.MAPDB, ChronosBackend.TUPL})
public class ItLandscapeBenchmark extends AllChronoSphereBackendsTest {


    // =================================================================================================================
    // ROOT CAUSE ANALYSIS
    // =================================================================================================================


    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "500000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
    @InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
    public void rootCauseAnalysis() throws Exception {
        ChronoSphereInternal sphere = this.setUpChronoSphereITLandscape();

        System.gc();
        System.gc();
        System.gc();

        // Root Cause Analysis
        Statistic rcaStats = new Statistic();

        System.out.println("BEGINNING WARMUP");

        // warmup
        for (int i = 0; i < 10; i++) {
            this.runRootCauseAnalysis(sphere);
        }

        System.out.println("WARMUP COMPLETE");

        // actual benchmark runs
        for (int i = 0; i < 10; i++) {
            rcaStats.addSample(this.runRootCauseAnalysis(sphere));
        }

        // print the results
        System.out.println("RCA: " + new TimeStatistics(rcaStats).toCSV());
        System.out.println("RCA Samples: " + new TimeStatistics(rcaStats).getRuntimes());
    }


    private long runRootCauseAnalysis(final ChronoSphereInternal sphere) {
        try (ChronoSphereTransaction tx = sphere.tx()) {
            EClass service = tx.getEClassBySimpleName("Service");
            EClass virtualHost = tx.getEClassBySimpleName("VirtualHost");
            EClass physicalMachine = tx.getEClassBySimpleName("PhysicalMachine");
            EClass application = tx.getEClassBySimpleName("Application");
            EReference hostRunsOn = EMFUtils.getEReference(virtualHost, "runsOn");
            EReference appRunsOn = EMFUtils.getEReference(application, "runsOn");

            EReference dependsOn = EMFUtils.getEReference(service, "dependsOn");
            List<EObject> services = tx.find().startingFromInstancesOf(service).toList();
            Collections.shuffle(services);
            TimeStatistics statistics = new TimeStatistics();
            long sum = 0;
            for (int i = 0; i < 1000; i++) {
                EObject s = services.get(i);
                statistics.beginRun();
                Set<EObject> result = tx.find()
                    .startingFromEObject(s)
                    .eGet(dependsOn).eGet(appRunsOn)
                    .closure(hostRunsOn).isInstanceOf(physicalMachine, false)
                    .toSet();
                statistics.endRun();
                sum += result.size();
            }
            System.out.println("ROOT CAUSE ANALYSIS");
            System.out.println("   Time: " + statistics.getTotalTime() + "ms");
            System.out.println("   Hits: " + sum);
            return statistics.getTotalTime();
        }
    }


    // =================================================================================================================
    // IMPACT ANALYSIS
    // =================================================================================================================

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "500000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
    @InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
    public void impactAnalysis() throws Exception {
        ChronoSphereInternal sphere = this.setUpChronoSphereITLandscape();

        System.gc();
        System.gc();
        System.gc();

        // Impact Analysis
        Statistic imaStats = new Statistic();

        System.out.println("BEGINNING WARMUP");

        // warmup
        for (int i = 0; i < 10; i++) {
            this.runImpactAnalysis(sphere);
        }

        System.out.println("WARMUP COMPLETE");

        // actual benchmark runs
        for (int i = 0; i < 10; i++) {
            imaStats.addSample(this.runImpactAnalysis(sphere));
        }

        // print the results
        System.out.println("IMA: " + new TimeStatistics(imaStats).toCSV());
        System.out.println("IMA Samples: " + new TimeStatistics(imaStats).getRuntimes());
    }

    private long runImpactAnalysis(final ChronoSphereInternal sphere) {
        try (ChronoSphereTransaction tx = sphere.tx()) {
            EClass service = tx.getEClassBySimpleName("Service");
            EClass virtualHost = tx.getEClassBySimpleName("VirtualHost");
            EClass physicalMachine = tx.getEClassBySimpleName("PhysicalMachine");
            EClass application = tx.getEClassBySimpleName("Application");
            EReference hostRunsOn = EMFUtils.getEReference(virtualHost, "runsOn");
            EReference appRunsOn = EMFUtils.getEReference(application, "runsOn");

            EReference dependsOn = EMFUtils.getEReference(service, "dependsOn");
            List<EObject> machines = tx.find().startingFromInstancesOf(physicalMachine).toList();
            Collections.shuffle(machines);
            TimeStatistics statistics = new TimeStatistics();
            long sum = 0;
            for (int i = 0; i < 10; i++) {
                EObject s = machines.get(i);
                statistics.beginRun();
                Set<EObject> result = tx.find()
                    .startingFromEObject(s)
                    // TODO: this query only considers App-[runsOn]->VirtualHost-[runsOn]->PhysicalMachine.
                    // This query currently ignores the [App]-[runsOn]->PhysicalMachine relationship.
                    .closure(hostRunsOn, Direction.INCOMING)
                    .eGetInverse(appRunsOn)
                    .eGetInverse(dependsOn)
                    .toSet();
                statistics.endRun();
                sum += result.size();
            }
            System.out.println("IMPACT ANALYSIS");
            System.out.println("   Time: " + statistics.getTotalTime() + "ms");
            System.out.println("   Hits: " + sum);
            return statistics.getTotalTime();
        }
    }

    // =================================================================================================================
    // FIND BY NAME
    // =================================================================================================================


    private static final List<String> PHYSICAL_MACHINE_BASE_NAMES =
        ImmutableList.of(
            "IBM Power S822",
            "IBM Power S814",
            "IBM Power S824",
            "IBM Power 710",
            "IBM Power 720",
            "Lenovo x440",
            "Lenovo ThinkSystem SN550",
            "Lenovo ThinkSystem SN850",
            "Lenovo ThinkSystem SR950",
            "Lenovo ThinkSystem SR860",
            "Lenovo ThinkSystem x880",
            "Lenovo ThinkSystem x480",
            "HPE ProLiant BL460c",
            "HPE ProLiant BL660c",
            "HPE ProLiant WS460c",
            "HPE ProLiant DL385",
            "HPE ProLiant XL190r"
        );

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "500000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
    @InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
    public void findByName() throws Exception {
        ChronoSphereInternal sphere = this.setUpChronoSphereITLandscape();

        // Find By Name
        Statistic fbnStats = new Statistic();

        System.out.println("BEGINNING WARMUP");

        // warmup
        for (int i = 0; i < 10; i++) {
            this.runFindByName(sphere);
        }

        System.out.println("WARMUP COMPLETE");

        // actual benchmark runs
        for (int i = 0; i < 10; i++) {
            fbnStats.addSample(this.runFindByName(sphere));
        }

        // print the results
        System.out.println("FBN: " + new TimeStatistics(fbnStats).toCSV());
        System.out.println("FBN Samples: " + new TimeStatistics(fbnStats).getRuntimes());
    }

    private long runFindByName(final ChronoSphereInternal sphere) {
        try (ChronoSphereTransaction tx = sphere.tx()) {
            EClass physicalMachine = tx.getEClassBySimpleName("PhysicalMachine");
            EAttribute name = EMFUtils.getEAttribute(physicalMachine, "name");
            TimeStatistics statistics = new TimeStatistics();
            long sum = 0;
            for (int i = 0; i < 100; i++) {
                String randomName = BenchmarkUtils.getRandomEntryOf(PHYSICAL_MACHINE_BASE_NAMES);
                int number = BenchmarkUtils.randomBetween(0, 100);
                randomName += " " + number;
                statistics.beginRun();
                Set<EObject> result = tx.find()
                    .startingFromInstancesOf(physicalMachine)
                    .has(name, randomName)
                    .toSet();
                statistics.endRun();
                sum += result.size();
            }
            System.out.println("FIND BY NAME");
            System.out.println("   Time: " + statistics.getTotalTime() + "ms");
            System.out.println("   Hits: " + sum);
            return statistics.getTotalTime();
        }
    }

    // =================================================================================================================
    // ASSETS OVER TIME
    // =================================================================================================================

    @Test
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHING_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.CACHE_MAX_SIZE, value = "500000")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_ENABLED, value = "true")
    @InstantiateChronosWith(property = ChronoDBConfiguration.QUERY_CACHE_MAX_SIZE, value = "100")
    @InstantiateChronosWith(property = ChronoDBConfiguration.COMMIT_CONFLICT_RESOLUTION_STRATEGY, value = "OVERWRITE_WITH_SOURCE")
    @InstantiateChronosWith(property = ChronoDBConfiguration.DUPLICATE_VERSION_ELIMINATION_MODE, value = "off")
    @InstantiateChronosWith(property = ChronoGraphConfiguration.TRANSACTION_CHECK_ID_EXISTENCE_ON_ADD, value = "false")
    public void assetsOverTime() throws Exception {
        ChronoSphereInternal sphere = this.setUpChronoSphereITLandscape();

        int additionsPerDay = 300;
        int deletionsPerDay = 50;

        List<String> eObjectIdsInHead = sphere.tx()
            .find()
            .startingFromAllEObjects()
            .toStream()
            .map(it -> (ChronoEObject) it)
            .map(ChronoEObject::getId)
            .distinct()
            .collect(Collectors.toList());

        long minTimestamp = sphere.getNow();

        // simulate one year of changes
        System.out.println("Simulating 1 year of changes...");
        for (int day = 1; day <= 365; day++) {
            ChronoSphereTransaction dayTransaction = sphere.tx();
            EClass physicalMachine = dayTransaction.getEClassBySimpleName("PhysicalMachine");
            EClass virtualMachine = dayTransaction.getEClassBySimpleName("VirtualMachine");
            EClass service = dayTransaction.getEClassBySimpleName("Service");
            EClass cluster = dayTransaction.getEClassBySimpleName("Cluster");
            EClass application = dayTransaction.getEClassBySimpleName("Application");
            List<EClass> eClasses = Lists.newArrayList(physicalMachine, virtualMachine, service, cluster, application);
            List<String> newEObjectIds = Lists.newArrayList();
            // create the elements
            for (int addition = 0; addition < additionsPerDay; addition++) {
                EClass eClass = BenchmarkUtils.getRandomEntryOf(eClasses);
                ChronoEObject eObject = (ChronoEObject) dayTransaction.createAndAttach(eClass);
                newEObjectIds.add(eObject.getId());
            }
            // delete some other elements
            for (int deletion = 0; deletion < deletionsPerDay; deletion++) {
                String randomId = BenchmarkUtils.getRandomEntryOf(eObjectIdsInHead);
                ChronoEObject eObject = dayTransaction.getEObjectById(randomId);
                eObjectIdsInHead.remove(eObject.getId());
                dayTransaction.delete(eObject, false);
            }
            eObjectIdsInHead.addAll(newEObjectIds);
            dayTransaction.commit();
            System.out.println("Completed Simulation of Day " + day);
        }

        // assets over time
        Statistic aotStats = new Statistic();

        System.out.println("BEGINNING WARMUP");

        // warmup
        for (int i = 0; i < 10; i++) {
            this.runAssetsOverTime(sphere, i, minTimestamp);
        }

        System.out.println("WARMUP COMPLETE");

        // actual benchmark runs
        for (int i = 0; i < 10; i++) {
            aotStats.addSample(this.runAssetsOverTime(sphere, i + 5, minTimestamp));
        }

        // print the results
        System.out.println("AOT: " + new TimeStatistics(aotStats).toCSV());
        System.out.println("AOT Samples: " + new TimeStatistics(aotStats).getRuntimes());
    }

    private long runAssetsOverTime(ChronoSphere sphere, int commitIndexOffset, long minTimestamp) {
        long now = sphere.getNow();
        TimeStatistics statistics = new TimeStatistics();

        long sum = 0;
        // retrieve the last 365 commits
        List<Long> commits = Lists.newArrayList(sphere.getCommitTimestampsPaged(minTimestamp, now, 365, 0, Order.ASCENDING));
        for (int month = 0; month < 12; month++) {
            int commitIndex = month * 30 + commitIndexOffset;
            long commitTimestamp = commits.get(commitIndex);
            try (ChronoSphereTransaction tx = sphere.tx(commitTimestamp)) {
                EClass service = tx.getEClassBySimpleName("Service");
                statistics.beginRun();
                long count = tx.find().startingFromInstancesOf(service).count();
                statistics.endRun();
                sum += count;
            }
        }

        // print the results
        System.out.println("ASSETS OVER TIME");
        System.out.println("   Time: " + statistics.getTotalTime() + "ms");
        System.out.println("   Hits: " + sum);
        return statistics.getTotalTime();
    }

    // =================================================================================================================
    // HELPER METHODS
    // =================================================================================================================

    private ChronoSphereInternal setUpChronoSphereITLandscape() throws IOException {
        final List<EPackage> ePackages;
        {
            String path = "ecoremodels/itlandscape.ecore";
            InputStream stream = this.getClass().getClassLoader().getResourceAsStream(path);
            String ecoreContents = IOUtils.toString(stream, StandardCharsets.UTF_8);
            ePackages = EMFUtils.readEPackagesFromXMI(ecoreContents);
        }
        ChronoSphereInternal sphere = this.getChronoSphere();
        sphere.getEPackageManager().registerOrUpdateEPackages(ePackages);
        try (ChronoSphereTransaction tx = sphere.tx()) {
            EAttribute name = tx.getEAttributeByQualifiedName("itlandscape::Element#name");
            sphere.getIndexManager().createIndexOn(name);
            tx.commit();
        }
        System.out.println("IT-Landscape Ecore metamodel loaded.");

        final String xmiContent;
        {
            String path = "ecoremodels/ITLandscape200k.xmi";
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
            System.out.println("Loading the 200k ITLandscape XMI took " + (timeAfterLoad - timeBeforeLoad) + "ms.");
        }
        return sphere;
    }


}
