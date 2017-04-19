package org.chronos.chronosphere.test.evolution;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.api.MetaModelEvolutionContext;
import org.chronos.chronosphere.api.MetaModelEvolutionIncubator;
import org.chronos.chronosphere.api.exceptions.ElementCannotBeEvolvedException;
import org.chronos.chronosphere.api.exceptions.MetaModelEvolutionCanceledException;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.chronosphere.testmodels.instance.JohnDoeFamilyModel;
import org.chronos.chronosphere.testmodels.meta.PersonMetamodel;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.EcorePackage;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BasicMetamodelEvolutionTest extends AllChronoSphereBackendsTest {

	@Test
	public void canPerformSimpleMigrationWithIncubator() {
		ChronoSphere sphere = this.getChronoSphere();
		// prepare the epackage
		EPackage personEPackage = PersonMetamodel.createPersonEPackage();
		sphere.getEPackageManager().registerOrUpdateEPackage(personEPackage);
		// attach the EObjects
		try (ChronoSphereTransaction tx = sphere.tx()) {
			tx.attach(new JohnDoeFamilyModel());
			tx.commit();
		}

		long afterFirstInsert = sphere.getBranchManager().getMasterBranch().getNow();

		// check that the new EObjects have been stored successfully
		try (ChronoSphereTransaction tx = sphere.tx()) {
			EPackage ePackage = tx.getEPackageByNsURI(PersonMetamodel.PERSON_EPACKAGE_NS_URI);
			EClass personEClass = (EClass) ePackage.getEClassifier("Person");
			assertEquals(5, tx.find().startingFromInstancesOf(personEClass).count());
		}

		// create another copy of the person EPackage...
		EPackage evolvedPersonEPackage = PersonMetamodel.createPersonEPackage();
		// ... and evolve it to the next version
		evolvePersonEPackage(evolvedPersonEPackage);

		// create the incubator for this change
		MetaModelEvolutionIncubator incubator = new PersonMetamodelIncubator();
		// evolve the instance model
		sphere.getEPackageManager().evolveMetamodel(incubator, evolvedPersonEPackage);

		long afterMetamodelEvolution = sphere.getBranchManager().getMasterBranch().getNow();
		assertTrue(afterMetamodelEvolution > afterFirstInsert);

		// assert that the old version is still there and valid
		try (ChronoSphereTransaction tx = sphere.tx(afterFirstInsert)) {
			Set<EObject> eObjects = tx.find().startingFromAllEObjects().has("firstName", "John").has("lastName", "Doe")
					.toSet();
			assertEquals(1, eObjects.size());
			EObject johnDoe = Iterables.getOnlyElement(eObjects);
			assertNotNull(johnDoe.eClass().getEStructuralFeature("firstName"));
			assertNotNull(johnDoe.eClass().getEStructuralFeature("lastName"));
		}

		// assert that the new version exists and is valid
		try (ChronoSphereTransaction tx = sphere.tx(afterMetamodelEvolution)) {
			Set<EObject> eObjects = tx.find().startingFromAllEObjects().has("name", "John Doe").toSet();
			assertEquals(1, eObjects.size());
			EObject johnDoe = Iterables.getOnlyElement(eObjects);
			EClass person = johnDoe.eClass();
			EAttribute name = EMFUtils.getEAttribute(person, "name");
			assertNotNull(person.getEStructuralFeature("name"));
			assertNull(person.getEStructuralFeature("firstName"));
			assertNull(person.getEStructuralFeature("lastName"));
			EReference married = EMFUtils.getEReference(person, "married");
			assertNotNull(married);
			EObject janeDoe = (EObject) johnDoe.eGet(married);
			assertNotNull(janeDoe);
			assertEquals("Jane Doe", janeDoe.eGet(name));
		}

		// assert that the history of a migrated eobject is not cut at the migration step
		try (ChronoSphereTransaction tx = sphere.tx()) {
			Set<EObject> eObjects = tx.find().startingFromAllEObjects().has("name", "John Doe").toSet();
			assertEquals(1, eObjects.size());
			EObject johnDoe = Iterables.getOnlyElement(eObjects);
			Iterator<Long> johnsHistory = tx.getEObjectHistory(johnDoe);
			Iterator<Long> earlierHistory = Iterators.filter(johnsHistory,
					timestamp -> timestamp < afterMetamodelEvolution);
			assertTrue(Iterators.contains(earlierHistory, afterFirstInsert));
		}

	}

	private static void evolvePersonEPackage(final EPackage personEPackage) {
		EClass ecPerson = (EClass) personEPackage.getEClassifier("Person");
		EAttribute eaFirstName = (EAttribute) ecPerson.getEStructuralFeature("firstName");
		EAttribute eaLastName = (EAttribute) ecPerson.getEStructuralFeature("lastName");
		ecPerson.getEStructuralFeatures().remove(eaFirstName);
		ecPerson.getEStructuralFeatures().remove(eaLastName);
		EAttribute eaName = EcoreFactory.eINSTANCE.createEAttribute();
		eaName.setName("name");
		eaName.setLowerBound(0);
		eaName.setUpperBound(1);
		eaName.setEType(EcorePackage.Literals.ESTRING);
		ecPerson.getEStructuralFeatures().add(eaName);
	}

	private static class PersonMetamodelIncubator implements MetaModelEvolutionIncubator {

		@Override
		public EClass migrateClass(final EObject oldObject, final MetaModelEvolutionContext context)
				throws MetaModelEvolutionCanceledException, ElementCannotBeEvolvedException {
			// extract data from the old metamodel
			EPackage oldPersonEPackage = context.getOldEPackage(PersonMetamodel.PERSON_EPACKAGE_NS_URI);
			EClass oldPersonEClass = (EClass) oldPersonEPackage.getEClassifier("Person");
			// extract data from the new metamodel
			EPackage newPersonEPackage = context.getNewEPackage(PersonMetamodel.PERSON_EPACKAGE_NS_URI);
			EClass newPersonEClass = (EClass) newPersonEPackage.getEClassifier("Person");
			// choose the new class of our EObject based on the old class
			if (oldPersonEClass.isInstance(oldObject)) {
				return newPersonEClass;
			} else {
				throw new ElementCannotBeEvolvedException();
			}
		}

		@Override
		public void updateAttributeValues(final EObject oldObject, final EObject newObject,
				final MetaModelEvolutionContext context)
				throws MetaModelEvolutionCanceledException, ElementCannotBeEvolvedException {
			// create a lookup of values from the old version
			Map<String, Object> attributeValues = Maps.newHashMap();
			for (EAttribute eAttribute : oldObject.eClass().getEAllAttributes()) {
				attributeValues.put(eAttribute.getName(), oldObject.eGet(eAttribute));
			}
			// try to apply the values from the old version by-name to the new version when possible
			for (EAttribute eAttribute : newObject.eClass().getEAllAttributes()) {
				Object values = attributeValues.get(eAttribute.getName());
				if (values != null) {
					newObject.eSet(eAttribute, values);
				}
			}
			// explicitly set the new "name" attribute, based on "firstName" and "lastName" in
			// the old model
			EAttribute eaName = EMFUtils.getEAttribute(newObject.eClass(), "name");
			String newName = attributeValues.get("firstName") + " " + attributeValues.get("lastName");
			newObject.eSet(eaName, newName);
		}

		@Override
		@SuppressWarnings("unchecked")
		public void updateReferenceTargets(final EObject oldObject, final EObject newObject,
				final MetaModelEvolutionContext context)
				throws MetaModelEvolutionCanceledException, ElementCannotBeEvolvedException {
			for (EReference newEReference : newObject.eClass().getEAllReferences()) {
				EReference oldEReference = EMFUtils.getEReference(oldObject.eClass(), newEReference.getName());
				Object oldTargets = oldObject.eGet(oldEReference);
				Object newTargets = null;
				if (oldTargets != null) {
					if (oldEReference.isMany()) {
						// multiplicity-many
						Collection<EObject> oldTargetCollection = (Collection<EObject>) oldTargets;
						Collection<EObject> newTargetCollection = Lists.newArrayList();
						for (EObject target : oldTargetCollection) {
							EObject newTarget = context.getCorrespondingEObjectInNewModel(target);
							newTargetCollection.add(newTarget);
						}
						newTargets = newTargetCollection;
					} else {
						// multiplicity-one
						EObject oldTarget = (EObject) oldTargets;
						EObject newTarget = context.getCorrespondingEObjectInNewModel(oldTarget);
						newTargets = newTarget;
					}
				}
				if (newTargets != null) {
					newObject.eSet(newEReference, newTargets);
				}
			}
		}

	}
}
