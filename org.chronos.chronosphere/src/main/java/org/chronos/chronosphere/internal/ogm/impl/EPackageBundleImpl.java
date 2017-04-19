package org.chronos.chronosphere.internal.ogm.impl;

import static com.google.common.base.Preconditions.*;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.chronos.chronosphere.internal.ogm.api.EPackageBundle;
import org.eclipse.emf.ecore.EPackage;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class EPackageBundleImpl implements EPackageBundle {

	private final List<EPackage> ePackages;

	public EPackageBundleImpl(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkArgument(ePackages.iterator().hasNext(),
				"Precondition violation - argument 'EPackages' must not be empty!");
		for (EPackage ePackage : ePackages) {
			checkNotNull(ePackage.getNsURI(),
					"Precondition violation - argument 'ePackages' contains an EPackage where 'ePackage.getNsURI()' == NULL!");
			checkNotNull(ePackage.getNsPrefix(),
					"Precondition violation - argument 'ePackages' contains an EPackage where 'ePackage.getNsPrefix()' == NULL!");
			checkNotNull(ePackage.getName(),
					"Precondition violation - argument 'ePackages' contains an EPackage where 'ePackage.getName()' ==  NULL!");
		}
		EMFUtils.assertEPackagesAreSelfContained(ePackages);
		// remove duplicates from the epackages
		Set<EPackage> ePackageSet = Sets.newLinkedHashSet(ePackages);
		this.ePackages = Collections.unmodifiableList(Lists.newArrayList(ePackageSet));
	}

	@Override
	public Iterator<EPackage> iterator() {
		return this.ePackages.iterator();
	}

	@Override
	public List<EPackage> getContents() {
		return this.ePackages;
	}

	@Override
	public EPackage getEPackageByNsURI(final String ePackageNsURI) {
		checkNotNull(ePackageNsURI, "Precondition violation - argument 'ePackageNsURI' must not be NULL!");
		for (EPackage ePackage : this) {
			if (ePackage.getNsURI().equals(ePackageNsURI)) {
				return ePackage;
			}
		}
		return null;
	}

}
