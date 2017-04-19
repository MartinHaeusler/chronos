package org.chronos.chronosphere.test.query;

import static org.chronos.chronosphere.api.query.SubQuery.*;
import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.chronosphere.testmodels.instance.JohnDoeFamilyModel;
import org.chronos.chronosphere.testutils.ChronoSphereTestUtils;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.junit.Test;

import com.google.common.collect.Sets;

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
			Set<EObject> queryResult = transaction.find().startingFromEObject(johnDoe)
					.union(eGet("child"), eGet("married")).asEObject().toSet();

			// married to
			EObject janeDoe = testModel.getJaneDoe();
			// children
			EObject sarahDoe = testModel.getSarahDoe();

			Set<EObject> family = Sets.newHashSet(janeDoe, sarahDoe);

			assertEquals(family, queryResult);
		});
	}

}
