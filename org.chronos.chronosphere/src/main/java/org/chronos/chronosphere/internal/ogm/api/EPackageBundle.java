package org.chronos.chronosphere.internal.ogm.api;

import static com.google.common.base.Preconditions.*;

import java.util.List;

import org.chronos.chronosphere.internal.ogm.impl.EPackageBundleImpl;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcorePackage;

/**
 * An {@link EPackageBundle} is a collection of {@link EPackage}s that is self-contained, i.e. does not reference any
 * elements that are not contained in the bundle.
 *
 * <p>
 * The {@link EcorePackage} is an implicit member of any {@link EPackageBundle}. It will not be contained in
 * {@link #getContents()}, and will not be returned by the {@linkplain #iterator() iterator}.
 *
 *
 * @author martin.haeusler@uibk.ac.at -- Initial Contribution and API
 *
 */
public interface EPackageBundle extends Iterable<EPackage> {

	// =====================================================================================================================
	// FACTORY METHODS
	// =====================================================================================================================

	/**
	 * Creates and returns a new {@link EPackageBundle}, consisting of the given EPackages.
	 *
	 * @param ePackages
	 *            The EPackages to be contained in the new bundle. Must not be <code>null</code>, must not be empty.
	 * @return The newly created bundle. Never <code>null</code>.
	 */
	public static EPackageBundle of(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkArgument(ePackages.iterator().hasNext(),
				"Precondition violation - argument 'EPackages' must not be empty!");
		return new EPackageBundleImpl(ePackages);
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	/**
	 * Returns a list of {@link EPackage}s that are contained in this bundle.
	 *
	 * @return An unmodifiable view on the EPackages contained in this bundle.
	 */
	public List<EPackage> getContents();

	/**
	 * Checks if this bundle contains an {@link EPackage} with the given {@linkplain EPackage#getNsURI() namespace URI}.
	 *
	 * <p>
	 * This method will only check the root-level EPackages, sub-packages will <b>not</b> be checked.
	 *
	 * @param ePackageNsURI
	 *            The namespace URI to find. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if this bundle contains an EPackage with the given namespace URI, otherwise
	 *         <code>false</code>.
	 */
	public default boolean containsEPackageWithNsURI(final String ePackageNsURI) {
		checkNotNull(ePackageNsURI, "Precondition violation - argument 'ePackageNsURI' must not be NULL!");
		return this.getEPackageByNsURI(ePackageNsURI) != null;
	}

	/**
	 * Returns a contained {@link EPackage} by its {@linkplain EPackage#getNsURI() namespace URI}.
	 *
	 * <p>
	 * This method will only check the root-level EPackages, sub-packages will <b>not</b> be checked.
	 *
	 * @param ePackageNsURI
	 *            The namespace URI to get the EPackage for. Must not be <code>null</code>.
	 *
	 * @return The EPackage with the given namespace URI, or <code>null</code> if this bundle contains no such EPackage.
	 */
	public EPackage getEPackageByNsURI(String ePackageNsURI);

}
