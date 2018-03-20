package org.chronos.chronosphere.test.query;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.chronosphere.testmodels.instance.JohnDoeFamilyModel;
import org.chronos.chronosphere.testmodels.meta.PersonMetamodel;
import org.chronos.chronosphere.testutils.ChronoSphereTestUtils;
import org.eclipse.emf.ecore.*;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.chronos.chronosphere.api.query.SubQuery.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class QueryAPITest extends AllChronoSphereBackendsTest {

    @Test
    public void canFindJohnsFriends() {
        ChronoSphere sphere = this.getChronoSphere();
        // create and register the EPackage for this test
        JohnDoeFamilyModel testModel = new JohnDoeFamilyModel();
        EPackage ePackage = testModel.getEPackage();
        sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

        // open a transaction
        ChronoSphereTransaction tx = sphere.tx();

        // attach all elements from the test model
        tx.attach(testModel);

        ChronoSphereTestUtils.assertCommitAssert(tx, transaction -> {
            Set<EObject> johnsFriends = transaction.find()
                // we start with all EObjects
                .startingFromAllEObjects()
                // we are interested only in persons
                .isInstanceOf("Person")
                // we are interested only in persons named John
                .has("firstName", "John")
                // ... that also have last name "Doe"
                .has("lastName", "Doe")
                // we get the friends of that person
                .eGet("friend")
                // and collect everything in a set of EObjects
                .asEObject().toSet();

            EObject johnParker = testModel.getJohnParker();
            EObject jackSmith = testModel.getJackSmith();

            assertEquals(Sets.newHashSet(johnParker, jackSmith), johnsFriends);
        });
    }

    @Test
    public void canFindSarahsParentsViaReferencingEObjects() {
        ChronoSphere sphere = this.getChronoSphere();
        // create and register the EPackage for this test
        JohnDoeFamilyModel testModel = new JohnDoeFamilyModel();
        EPackage ePackage = testModel.getEPackage();
        sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

        // open a transaction
        ChronoSphereTransaction tx = sphere.tx();

        // attach all elements from the test model
        tx.attach(testModel);

        ChronoSphereTestUtils.assertCommitAssert(tx, transaction -> {
            EObject sarahDoe = testModel.getSarahDoe();
            Set<EObject> sarahsParents = transaction.find()
                // we start with all EObjects
                .startingFromEObject(sarahDoe)
                // get the referencing eobjects (which should be her parents)
                .allReferencingEObjects().toSet();

            EObject johnDoe = testModel.getJohnDoe();
            EObject janeDoe = testModel.getJaneDoe();

            assertEquals(Sets.newHashSet(johnDoe, janeDoe), sarahsParents);
        });
    }

    @Test
    public void canFindAllPersonsWhoHaveChildren() {
        ChronoSphere sphere = this.getChronoSphere();
        // create and register the EPackage for this test
        JohnDoeFamilyModel testModel = new JohnDoeFamilyModel();
        EPackage ePackage = testModel.getEPackage();
        sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

        // open a transaction
        ChronoSphereTransaction tx = sphere.tx();

        // attach all elements from the test model
        tx.attach(testModel);

        ChronoSphereTestUtils.assertCommitAssert(tx, transaction -> {
            EClass person = (EClass) ePackage.getEClassifier("Person");
            Set<EObject> personsWithChildren = transaction.find().startingFromInstancesOf(person).named("parents")
                .eGet("child").back("parents").asEObject().toSet();

            EObject johnDoe = testModel.getJohnDoe();
            EObject janeDoe = testModel.getJaneDoe();

            assertEquals(Sets.newHashSet(johnDoe, janeDoe), personsWithChildren);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canFindFamilyOfJohnWithSubqueries() {
        ChronoSphere sphere = this.getChronoSphere();
        // create and register the EPackage for this test
        JohnDoeFamilyModel testModel = new JohnDoeFamilyModel();
        EPackage ePackage = testModel.getEPackage();
        sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

        // open a transaction
        ChronoSphereTransaction tx = sphere.tx();

        // attach all elements from the test model
        tx.attach(testModel);

        ChronoSphereTestUtils.assertCommitAssert(tx, transaction -> {
            EObject johnDoe = testModel.getJohnDoe();
            Set<EObject> queryResult = transaction.find()
                .startingFromEObject(johnDoe)
                .union(
                    eGet("child"),
                    eGet("married")
                ).asEObject()
                .toSet();

            // married to
            EObject janeDoe = testModel.getJaneDoe();
            // children
            EObject sarahDoe = testModel.getSarahDoe();

            Set<EObject> family = Sets.newHashSet(janeDoe, sarahDoe);

            assertEquals(family, queryResult);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canFindFamilyOfJohnWithSubqueries2() {
        ChronoSphere sphere = this.getChronoSphere();
        // create and register the EPackage for this test
        JohnDoeFamilyModel testModel = new JohnDoeFamilyModel();
        EPackage ePackage = testModel.getEPackage();
        sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

        // open a transaction
        ChronoSphereTransaction tx = sphere.tx();

        // attach all elements from the test model
        tx.attach(testModel);

        EReference child = tx.getEReferenceByQualifiedName("Person::Person#child");
        assertNotNull(child);
        EReference married = tx.getEReferenceByQualifiedName("Person::Person#married");
        assertNotNull(married);

        ChronoSphereTestUtils.assertCommitAssert(tx, transaction -> {
            EObject johnDoe = testModel.getJohnDoe();
            Set<EObject> queryResult = transaction.find()
                .startingFromEObject(johnDoe)
                .union(
                    eGet(child),
                    eGet(married)
                ).asEObject()
                .toSet();

            // married to
            EObject janeDoe = testModel.getJaneDoe();
            // children
            EObject sarahDoe = testModel.getSarahDoe();

            Set<EObject> family = Sets.newHashSet(janeDoe, sarahDoe);

            assertEquals(family, queryResult);
        });
    }

    @Test
    public void canCalculateTransitiveClosure() {
        ChronoSphere sphere = this.getChronoSphere();
        EPackage personEPackage = PersonMetamodel.createPersonEPackage();
        sphere.getEPackageManager().registerOrUpdateEPackage(personEPackage);

        { // setup
            // open a transaction
            ChronoSphereTransaction tx = sphere.tx();

            EClass person = tx.getEClassBySimpleName("Person");
            assertNotNull(person);
            EAttribute firstName = EMFUtils.getEAttribute(person, "firstName");
            assertNotNull(firstName);
            EReference friend = EMFUtils.getEReference(person, "friend");
            assertNotNull(friend);

            // create an instance model for testing.

            // the following four persons form a circle
            EObject p1 = tx.createAndAttach(person);
            p1.eSet(firstName, "p1");
            EObject p2 = tx.createAndAttach(person);
            p2.eSet(firstName, "p2");
            EObject p3 = tx.createAndAttach(person);
            p3.eSet(firstName, "p3");
            EObject p4 = tx.createAndAttach(person);
            p4.eSet(firstName, "p4");
            EMFUtils.eGetMany(p1, friend).add(p2);
            EMFUtils.eGetMany(p2, friend).add(p3);
            EMFUtils.eGetMany(p3, friend).add(p4);
            EMFUtils.eGetMany(p4, friend).add(p1);

            // the following objects form a cycle of size 2 with each of the
            // four original persons
            EObject p5 = tx.createAndAttach(person);
            p5.eSet(firstName, "p5");
            EObject p6 = tx.createAndAttach(person);
            p6.eSet(firstName, "p6");
            EObject p7 = tx.createAndAttach(person);
            p7.eSet(firstName, "p7");
            EObject p8 = tx.createAndAttach(person);
            p8.eSet(firstName, "p8");
            EMFUtils.eGetMany(p1, friend).add(p5);
            EMFUtils.eGetMany(p5, friend).add(p1);
            EMFUtils.eGetMany(p2, friend).add(p6);
            EMFUtils.eGetMany(p6, friend).add(p2);
            EMFUtils.eGetMany(p3, friend).add(p7);
            EMFUtils.eGetMany(p7, friend).add(p3);
            EMFUtils.eGetMany(p4, friend).add(p8);
            EMFUtils.eGetMany(p8, friend).add(p4);

            tx.commit();
        }

        { // test
            // open a new transaction
            ChronoSphereTransaction tx = sphere.tx();
            EClass person = tx.getEClassBySimpleName("Person");
            assertNotNull(person);
            EAttribute firstName = EMFUtils.getEAttribute(person, "firstName");
            assertNotNull(firstName);
            EReference friend = EMFUtils.getEReference(person, "friend");
            assertNotNull(friend);

            Set<EObject> startPersons = tx.find().startingFromInstancesOf(person).has(firstName, "p1").toSet();
            assertThat(startPersons.size(), is(1));

            EObject p1Reloaded = Iterables.getOnlyElement(startPersons);
            assertThat(p1Reloaded.eGet(firstName), is("p1"));

            // start the closure calculation
            List<EObject> eObjects = tx.find().startingFromEObject(p1Reloaded).closure(friend).toList();
            assertThat(eObjects.size(), is(7));
            List<String> names = eObjects.stream().map(eObj -> (String) eObj.eGet(firstName)).collect(Collectors.toList());
            assertTrue(names.contains("p2"));
            assertTrue(names.contains("p3"));
            assertTrue(names.contains("p4"));
            assertTrue(names.contains("p5"));
            assertTrue(names.contains("p6"));
            assertTrue(names.contains("p7"));
        }

    }

}
