package org.chronos.chronosphere.testmodels.instance;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.chronos.chronosphere.emf.api.ChronoEObject;
import org.chronos.chronosphere.emf.impl.ChronoEObjectImpl;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public abstract class AbstractTestModel implements TestModel {

	private final Map<String, EObject> eObjectById = Maps.newHashMap();
	private final EPackage ePackage;

	protected AbstractTestModel(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		this.ePackage = ePackage;
		this.createModelData();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	public EPackage getEPackage() {
		return this.ePackage;
	}

	@Override
	public EObject getEObjectByID(final String id) {
		checkNotNull(id, "Precondition violation - argument 'id' must not be NULL!");
		return this.eObjectById.get(id);
	}

	@Override
	public Set<EObject> getAllEObjects() {
		return Collections.unmodifiableSet(Sets.newHashSet(this.eObjectById.values()));
	}

	@Override
	public Iterator<EObject> iterator() {
		return this.getAllEObjects().iterator();
	}

	// =====================================================================================================================
	// INTERNAL API
	// =====================================================================================================================

	protected void registerEObject(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		ChronoEObject chronoEObject = (ChronoEObject) eObject;
		this.eObjectById.put(chronoEObject.getId(), chronoEObject);
	}

	protected EObject createAndRegisterEObject(final String eObjectId, final EClass eClass) {
		checkNotNull(eObjectId, "Precondition violation - argument 'eObjectId' must not be NULL!");
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		EObject eObject = new ChronoEObjectImpl(eObjectId, eClass);
		this.registerEObject(eObject);
		return eObject;
	}

	// =====================================================================================================================
	// ABSTRACT METHOD DECLARATIONS
	// =====================================================================================================================

	protected abstract void createModelData();
}
