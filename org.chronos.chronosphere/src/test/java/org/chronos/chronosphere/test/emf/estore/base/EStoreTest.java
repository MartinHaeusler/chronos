package org.chronos.chronosphere.test.emf.estore.base;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.chronos.chronosphere.api.ChronoSphere;
import org.chronos.chronosphere.api.ChronoSphereTransaction;
import org.chronos.chronosphere.emf.impl.ChronoEFactory;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.testutils.factories.EMFEFactory;
import org.chronos.common.exceptions.UnknownEnumLiteralException;
import org.chronos.common.test.junit.categories.IntegrationTest;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EFactory;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public abstract class EStoreTest {

	// =====================================================================================================================
	// JUNIT PARAMETERS
	// =====================================================================================================================

	@Parameters(name = "Using {0}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				// option 1
				{ "EMF Reference", EmfAPI.REFERENCE_IMPLEMENTATION },
				// option 2
				{ "Chronos-Transient", EmfAPI.CHRONOS_TRANSIENT },
				// option 3
				{ "Chronos-Graph", EmfAPI.CHRONOS_GRAPH }
				// end options
		});
	}

	// =====================================================================================================================
	// JUNIT PARAMETER FIELDS
	// =====================================================================================================================

	@Parameter(0)
	public String name;

	@Parameter(1)
	public EmfAPI emfAPI;

	// =====================================================================================================================
	// INTERNAL FIELDS
	// =====================================================================================================================

	private ChronoSphere chronoSphereInstance = null;
	private ChronoSphereTransaction chronoSphereTransaction = null;
	private final Map<String, EPackage> registeredEPackages = Maps.newHashMap();

	// =====================================================================================================================
	// SETUP & TEAR DOWN
	// =====================================================================================================================

	@Before
	public void setup() {
		this.registeredEPackages.clear();
		if (this.emfAPI.requiresChronoSphere()) {
			this.chronoSphereInstance = ChronoSphere.FACTORY.create().inMemoryRepository().build();
		}
	}

	@After
	public void tearDown() {
		if (this.chronoSphereTransaction != null) {
			this.chronoSphereTransaction.close();
		}
		if (this.chronoSphereInstance != null) {
			this.chronoSphereInstance.close();
			this.chronoSphereInstance = null;
		}
		this.registeredEPackages.clear();
	}

	// =====================================================================================================================
	// METHODS FOR SUBCLASSES
	// =====================================================================================================================

	protected EFactory createNewEFactory() {
		switch (this.emfAPI) {
		case REFERENCE_IMPLEMENTATION:
			return new EMFEFactory();
		case CHRONOS_TRANSIENT:
			return new ChronoEFactory();
		case CHRONOS_GRAPH:
			return new ChronoEFactory();
		default:
			throw new UnknownEnumLiteralException(this.emfAPI);
		}
	}

	protected EPackage createNewEPackage(final String name, final String nsURI, final String nsPrefix) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkNotNull(nsURI, "Precondition violation - argument 'nsURI' must not be NULL!");
		checkNotNull(nsPrefix, "Precondition violation - argument 'nsPrefix' must not be NULL!");
		EPackage ePackage = EcoreFactory.eINSTANCE.createEPackage();
		ePackage.setEFactoryInstance(this.createNewEFactory());
		ePackage.setName(name);
		ePackage.setNsURI(nsURI);
		ePackage.setNsPrefix(nsPrefix);
		return ePackage;
	}

	protected void registerEPackages(final Collection<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		if (this.chronoSphereInstance != null) {
			this.chronoSphereInstance.getEPackageManager().registerOrUpdateEPackages(ePackages);
		}
		for (EPackage ePackage : EMFUtils.flattenEPackages(ePackages)) {
			assertNotNull(ePackage.getNsURI());
			assertNotNull(ePackage.getNsPrefix());
			this.registeredEPackages.put(ePackage.getNsURI(), ePackage);
		}
	}

	protected void registerEPackages(final EPackage... ePackages) {
		this.registerEPackages(Lists.newArrayList(ePackages));
	}

	protected EObject createEObject(final EClass eClass) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		if (this.registeredEPackages.isEmpty()) {
			throw new IllegalStateException("EPackages were not registered yet! "
					+ "Please call #registerEPackages(...) before creating EObjects!");
		}
		EObject eObject = null;
		switch (this.emfAPI) {
		case REFERENCE_IMPLEMENTATION:
			eObject = EcoreUtil.create(eClass);
			break;
		case CHRONOS_TRANSIENT:
			eObject = EcoreUtil.create(eClass);
			break;
		case CHRONOS_GRAPH:
			ChronoSphereTransaction tx = this.getTransaction();
			eObject = tx.createAndAttach(eClass);
			break;
		default:
			throw new UnknownEnumLiteralException(this.emfAPI);
		}
		assertNotNull(eObject);
		return eObject;
	}

	protected EPackage getEPackageByNsURI(final String nsUri) {
		if (this.registeredEPackages.isEmpty()) {
			throw new IllegalStateException("EPackages were not registered yet! "
					+ "Please call #registerEPackages(...) before requesting EPackages by NS URI!");
		}
		if (this.chronoSphereInstance != null) {
			// use the EPackage from the transaction
			return this.getTransaction().getEPackageByNsURI(nsUri);
		} else {
			return this.registeredEPackages.get(nsUri);
		}
	}

	private ChronoSphereTransaction getTransaction() {
		if (this.registeredEPackages.isEmpty()) {
			throw new IllegalStateException("EPackages were not registered yet! "
					+ "Please call #registerEPackages(...) before creating EObjects!");
		}
		if (this.chronoSphereTransaction != null) {
			if (this.chronoSphereTransaction.isClosed()) {
				// we had a transaction, but it is already closed; open a new one
				this.chronoSphereTransaction = this.chronoSphereInstance.tx();
			}
			// reuse existing transaction
			return this.chronoSphereTransaction;
		}
		if (this.chronoSphereInstance == null) {
			throw new IllegalArgumentException("The EMF API '" + this.emfAPI
					+ "' does not involve a ChronoSphere instance, cannot create a transaction!");
		}
		// we never used a transaction so far; create a new one
		this.chronoSphereTransaction = this.chronoSphereInstance.tx();
		return this.chronoSphereTransaction;
	}

}
