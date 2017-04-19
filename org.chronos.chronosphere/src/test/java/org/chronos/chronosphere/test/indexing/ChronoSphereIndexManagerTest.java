package org.chronos.chronosphere.test.indexing;

import static org.junit.Assert.*;

import java.util.Set;

import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.internal.api.ChronoSphereInternal;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.chronosphere.testmodels.instance.JohnDoeFamilyModel;
import org.chronos.chronosphere.testmodels.meta.PersonMetamodel;
import org.chronos.chronosphere.testutils.ChronoSphereTestUtils;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class ChronoSphereIndexManagerTest extends AllChronoSphereBackendsTest {

	@Test
	public void canCreateAndDropIndexOnEAttribute() {
		ChronoSphereInternal sphere = this.getChronoSphere();
		EPackage ePackage = PersonMetamodel.createPersonEPackage();
		EClass ecPerson = (EClass) ePackage.getEClassifier("Person");
		assertNotNull(ecPerson);
		EAttribute eaFirstName = (EAttribute) ecPerson.getEStructuralFeature("firstName");
		assertNotNull(eaFirstName);
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

		// check that the "first name" attribute is not indexed yet
		assertFalse(sphere.getIndexManager().existsIndexOn(eaFirstName));
		assertFalse(sphere.getIndexManager().isIndexDirty(eaFirstName));

		// create the index
		assertTrue(sphere.getIndexManager().createIndexOn(eaFirstName));

		// assert that the index exists now
		assertTrue(sphere.getIndexManager().existsIndexOn(eaFirstName));

		// the index should be dirty
		assertTrue(sphere.getIndexManager().isIndexDirty(eaFirstName));

		// reindex it
		sphere.getIndexManager().reindex(eaFirstName);

		// it should not be dirty anymore
		assertFalse(sphere.getIndexManager().isIndexDirty(eaFirstName));

		// drop the index
		assertTrue(sphere.getIndexManager().dropIndexOn(eaFirstName));

		// it should not be there anymore
		assertFalse(sphere.getIndexManager().existsIndexOn(eaFirstName));
	}

	@Test
	public void canQueryIndex() {
		ChronoSphereInternal sphere = this.getChronoSphere();
		// create the model and metamodel in-memory
		JohnDoeFamilyModel model = new JohnDoeFamilyModel();
		EPackage ePackage = model.getEPackage();
		// extract the EAttribute we want to index from the EPackage
		EClass ecPerson = (EClass) ePackage.getEClassifier("Person");
		assertNotNull(ecPerson);
		EAttribute eaFirstName = (EAttribute) ecPerson.getEStructuralFeature("firstName");
		assertNotNull(eaFirstName);
		// register the epackage
		sphere.getEPackageManager().registerOrUpdateEPackage(ePackage);

		// create the index
		sphere.getIndexManager().createIndexOn(eaFirstName);
		sphere.getIndexManager().reindex(eaFirstName);
		assertTrue(sphere.getIndexManager().existsIndexOn(eaFirstName));
		assertFalse(sphere.getIndexManager().isIndexDirty(eaFirstName));

		// attach the model
		ChronoSphereTransaction transaction = sphere.tx();
		transaction.attach(model);

		ChronoSphereTestUtils.assertCommitAssert(transaction, tx -> {
			Set<EObject> johns = tx.find().startingFromEObjectsWith(eaFirstName, "John").toSet();
			assertEquals(2, johns.size());
			assertTrue(johns.contains(model.getJohnDoe()));
			assertTrue(johns.contains(model.getJohnParker()));
		});
	}
}
