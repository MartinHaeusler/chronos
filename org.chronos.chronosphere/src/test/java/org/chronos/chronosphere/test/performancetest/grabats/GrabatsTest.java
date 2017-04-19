package org.chronos.chronosphere.test.performancetest.grabats;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.test.base.AllChronoSphereBackendsTest;
import org.chronos.chronosphere.testmodels.meta.GrabatsMetamodel;
import org.chronos.common.logging.ChronoLogger;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

@Category(PerformanceTest.class)
public class GrabatsTest extends AllChronoSphereBackendsTest {

	@Test
	public void grabatsMetamodelFilesArePresent() {
		assertTrue(GrabatsMetamodel.createCFGEPackages().size() > 0);
		assertTrue(GrabatsMetamodel.createJDTASTEPackages().size() > 0);
		assertTrue(GrabatsMetamodel.createPGEPackages().size() > 0);
		assertTrue(GrabatsMetamodel.createQ1ViewEPackages().size() > 0);
	}

	@Test
	public void canRegisterGrabatsMetamodels() {
		ChronoSphere sphere = this.getChronoSphere();
		registerGrabatsMetamodels(sphere);
		try (ChronoSphereTransaction tx = sphere.tx()) {
			for (EPackage ePackage : GrabatsMetamodel.createAllEPackages()) {
				EPackage storedEPackage = tx.getEPackageByNsURI(ePackage.getNsURI());
				assertNotNull(storedEPackage);
				checkEPackageConsistency(storedEPackage);
			}
		}
	}

	@Test
	public void canLoadGrabatsSet0withStandardEcoreXMI() throws Exception {
		long timeBeforeXMIread = System.currentTimeMillis();
		InputStream stream = new GZIPInputStream(
				GrabatsTest.class.getClassLoader().getResourceAsStream("testinstancemodels/grabats/set0.xmi.gz"));
		String xmiContents = IOUtils.toString(stream);
		long timeAfterXMIread = System.currentTimeMillis();
		ChronoLogger
				.logInfo("Loaded GRABATS set0.xmi into a String in " + (timeAfterXMIread - timeBeforeXMIread) + "ms.");
		long timeBeforeBatchLoad = System.currentTimeMillis();
		List<EObject> eObjects = EMFUtils.readEObjectsFromXMI(xmiContents,
				Sets.newHashSet(GrabatsMetamodel.createAllEPackages()));
		long timeAfterBatchLoad = System.currentTimeMillis();
		ChronoLogger
				.logInfo("Loaded GRABATS set0.xmi into Ecore in " + (timeAfterBatchLoad - timeBeforeBatchLoad) + "ms.");
		int count = 0;
		for (EObject eObject : eObjects) {
			count += Iterators.size(eObject.eAllContents()) + 1;
		}
		ChronoLogger.log("GRABATS set0.xmi contains " + count + " EObjects.");
	}

	@Test
	public void canLoadGrabatsSet0withBatchLoad() throws Exception {
		ChronoSphere sphere = this.getChronoSphere();
		registerGrabatsMetamodels(sphere);
		long timeBeforeXMIread = System.currentTimeMillis();
		InputStream stream = new GZIPInputStream(
				GrabatsMetamodel.class.getClassLoader().getResourceAsStream("testinstancemodels/grabats/set0.xmi.gz"));
		String xmiContents = IOUtils.toString(stream);
		long timeAfterXMIread = System.currentTimeMillis();
		ChronoLogger
				.logInfo("Loaded GRABATS set0.xmi into a String in " + (timeAfterXMIread - timeBeforeXMIread) + "ms.");
		long timeBeforeBatchLoad = System.currentTimeMillis();
		sphere.batchInsertModelData(xmiContents);
		long timeAfterBatchLoad = System.currentTimeMillis();
		ChronoLogger.logInfo(
				"Loaded GRABATS set0.xmi into ChronoSphere in " + (timeAfterBatchLoad - timeBeforeBatchLoad) + "ms.");

	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private static void registerGrabatsMetamodels(final ChronoSphere sphere) {
		sphere.getEPackageManager().registerOrUpdateEPackages(GrabatsMetamodel.createAllEPackages());
	}

	private static void checkEPackageConsistency(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		for (EClassifier eClassifier : ePackage.getEClassifiers()) {
			if (eClassifier instanceof EClass == false) {
				continue;
			}
			assertNotNull(eClassifier.getEPackage());
			assertNotNull(eClassifier.getName());
			for (EReference feature : ((EClass) eClassifier).getEAllReferences()) {
				EClassifier eType = feature.getEType();
				if (eType instanceof EClass == false) {
					continue;
				}
				assertNotNull("FAIL: " + eClassifier.getEPackage().getNsURI() + " -> " + eClassifier.getName() + " -> "
						+ feature.getName() + " :: EType has no EPackage!", eType.getEPackage());
				assertNotNull("FAIL: " + eClassifier.getEPackage().getNsURI() + " -> " + eClassifier.getName() + " -> "
						+ feature.getName() + " :: EType has no Name!", eType.getName());
			}
		}
		for (EPackage subPackage : ePackage.getESubpackages()) {
			checkEPackageConsistency(subPackage);
		}
	}
}
