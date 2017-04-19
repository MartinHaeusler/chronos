package org.chronos.chronosphere.emf.internal.util;

import static com.google.common.base.Preconditions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.chronos.chronosphere.api.exceptions.EPackagesAreNotSelfContainedException;
import org.chronos.chronosphere.api.exceptions.emf.NameResolutionException;
import org.chronos.chronosphere.api.exceptions.emf.XMIConversionFailedException;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.TreeIterator;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.resource.impl.ResourceSetImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;

public class EMFUtils {

	public static final String PACKAGE_PATH_SEPARATOR = "::";
	public static final String CLASS_FEATURE_SEPARATOR = "#";

	/**
	 * Factory method: Instantiates a new default {@link ResourceSet}.
	 *
	 * <p>
	 * The new resource set will always have the XMI factory associated with the "*.xmi" file ending.
	 *
	 * @return The new resource set. Never <code>null</code>.
	 */
	public static ResourceSet createResourceSet() {
		return createResourceSet(Collections.emptySet());
	}

	/**
	 * Factory method: Instantiates a new default {@link ResourceSet}.
	 *
	 * <p>
	 * The new resource set will always have the XMI factory associated with the "*.xmi" file ending.
	 *
	 * @param ePackages
	 *            The {@link EPackage}s to register at the resource set. Must not be <code>null</code>.
	 *
	 * @return The new resource set. Never <code>null</code>.
	 */
	public static ResourceSet createResourceSet(final Set<EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		ResourceSet resSet = new ResourceSetImpl();
		// make sure that XMI is available
		resSet.getResourceFactoryRegistry().getExtensionToFactoryMap().put("xmi", new XMIResourceFactoryImpl());
		for (EPackage ePackage : ePackages) {
			resSet.getPackageRegistry().put(ePackage.getNsURI(), ePackage);
		}
		return resSet;
	}

	/**
	 * Creates a temporary resource.<br>
	 * The resource will be contained in a (new) temporary resource set. It is important <b>not</b> to save this
	 * resource to disk!
	 *
	 * @param name
	 *            The name of the resource (implementation will be selected based on ending)
	 * @param contents
	 *            The contents to put into the new temporary resource
	 * @return The resource with the given contents and name
	 */
	public static Resource createTemporaryResource(final String name, final EObject... contents) {
		return createTemporaryResource(name, Collections.emptySet(), contents);
	}

	/**
	 * Creates a temporary resource.<br>
	 * The resource will be contained in a (new) temporary resource set. It is important <b>not</b> to save this
	 * resource to disk!
	 *
	 * @param name
	 *            The name of the resource (implementation will be selected based on ending)
	 * @param ePackages
	 *            The {@link EPackage}s to register at the temporary {@link ResourceSet}. Must not be <code>null</code>.
	 * @param contents
	 *            The contents to put into the new temporary resource
	 * @return The resource with the given contents and name
	 */
	public static Resource createTemporaryResource(final String name, final Set<EPackage> ePackages,
			final EObject... contents) {
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		ResourceSet resSet = createResourceSet(ePackages);
		return createTemporaryResource(resSet, name, contents);
	}

	/**
	 * Creates a temporary resource.<br>
	 * It is important <b>not</b> to save this resource to disk!
	 *
	 * @param set
	 *            The {@link ResourceSet} to put this resource into
	 * @param name
	 *            The name of the resource (implementation will be selected based on ending)
	 * @param contents
	 *            The contents to put into the new temporary resource
	 * @return The resource with the given contents and name
	 */
	public static Resource createTemporaryResource(final ResourceSet set, final String name,
			final EObject... contents) {
		Resource res = set.createResource(URI.createPlatformResourceURI("TEMP/" + name, false));
		for (EObject obj : contents) {
			res.getContents().add(obj);
		}
		return res;
	}

	/**
	 * Writes the given {@link EPackage} to XMI.
	 *
	 * @param ePackage
	 *            The EPackage to convert to XMI. Must not be <code>null</code>.
	 * @return The XMI representation of the EPackage. Never <code>null</code>.
	 */
	public static String writeEPackageToXMI(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		return writeEPackagesToXMI(Collections.singleton(ePackage));
	}

	/**
	 * Writes the given {@link EPackage}s to XMI.
	 *
	 * @param ePackages
	 *            The EPackages to convert to XMI. Must not be <code>null</code>.
	 * @return The XMI representation of the EPackages. Never <code>null</code>.
	 */
	public static String writeEPackagesToXMI(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		try {
			return writeEObjectsToXMI(ePackages);
		} catch (XMIConversionFailedException e) {
			throw new XMIConversionFailedException("Could not convert EPackages to XMI!", e);
		}
	}

	/**
	 * Reads the given XMI contents and converts them into an {@link EPackage}.
	 *
	 * <p>
	 * It is the responsibility of the caller to assert that there is exactly one {@link EObject} in the given XMI data,
	 * and that this object is an instance of {@link EPackage}.
	 *
	 * @param xmiContents
	 *            The XMI data that contains the EPackage to deserialize. Must not be <code>null</code>.
	 * @return The EPackage that was contained in the given XMI data. May be <code>null</code> if the XMI data was
	 *         empty.
	 */
	public static EPackage readEPackageFromXMI(final String xmiContents) {
		checkNotNull(xmiContents, "Precondition violation - argument 'xmiContents' must not be NULL!");
		try {
			List<EObject> eObjects = readEObjectsFromXMI(xmiContents);
			if (eObjects == null || eObjects.isEmpty()) {
				return null;
			}
			EObject singleObject = Iterables.getOnlyElement(eObjects);
			if (singleObject instanceof EPackage == false) {
				throw new IllegalStateException(
						"Attempted to read EPackage from XMI, but encountered EObject with EClass '"
								+ singleObject.eClass().getName() + "'!");
			}
			return (EPackage) singleObject;
		} catch (XMIConversionFailedException e) {
			throw new XMIConversionFailedException("Could not read EPackage from XMI data!", e);
		}
	}

	public static List<EPackage> readEPackagesFromXMI(final String xmiContents) {
		checkNotNull(xmiContents, "Precondition violation - argument 'xmiContents' must not be NULL!");
		try {
			List<EObject> eObjects = readEObjectsFromXMI(xmiContents);
			if (eObjects == null || eObjects.isEmpty()) {
				return null;
			}
			List<EPackage> ePackages = Lists.newArrayList();
			eObjects.forEach(eObject -> {
				if (eObject instanceof EPackage == false) {
					throw new IllegalStateException(
							"Attempted to read EPackage from XMI, but encountered EObject with EClass '"
									+ eObject.eClass().getName() + "'!");
				}
				ePackages.add((EPackage) eObject);
			});
			return ePackages;
		} catch (XMIConversionFailedException e) {
			throw new XMIConversionFailedException("Could not read EPackage from XMI data!", e);
		}
	}

	/**
	 * Writes the given {@link EObject} into its XMI representation.
	 *
	 * @param eObject
	 *            The EObject to convert to XMI. Must not be <code>null</code>.
	 * @return The XMI representation of the given EObject. Never <code>null</code>.
	 */
	public static String writeEObjectToXMI(final EObject eObject) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		return writeEObjectsToXMI(Collections.singleton(eObject));
	}

	/**
	 * Writse the given (@link EObject}s into their XMI representation.
	 *
	 * @param eObjects
	 *            The EObjects to convert to XMI. Must not be <code>null</code>.
	 * @return The XMI representation of the given EObjects. Never <code>null</code>.
	 */
	public static String writeEObjectsToXMI(final Iterable<? extends EObject> eObjects) {
		checkNotNull(eObjects, "Precondition violation - argument 'eObjects' must not be NULL!");
		EObject[] eObjectArray = Iterables.toArray(eObjects, EObject.class);
		Resource resource = createTemporaryResource("temp.xmi", eObjectArray);
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
			resource.save(baos, null);
			String xmi = baos.toString();
			return xmi;
		} catch (IOException ioe) {
			throw new XMIConversionFailedException("Could not convert EObject to XMI!", ioe);
		}
	}

	/**
	 * Reads the {@link EObject}s contained in the given XMI data.
	 *
	 * @param xmiContents
	 *            The XMI data to deserialize the EObjects from. Must not be <code>null</code>.
	 * @return The list of deserialized EObjects. May be empty, but never <code>null</code>.
	 */
	public static List<EObject> readEObjectsFromXMI(final String xmiContents) {
		checkNotNull(xmiContents, "Precondition violation - argument 'xmiContents' must not be NULL!");
		return readEObjectsFromXMI(xmiContents, Collections.emptySet());
	}

	/**
	 * Reads the {@link EObject}s contained in the given XMI data.
	 *
	 * @param xmiContents
	 *            The XMI data to deserialize the EObjects from. Must not be <code>null</code>.
	 * @param ePackages
	 *            The {@link EPackage}s to use for reading the XMI contents. Must not be <code>null</code>.
	 *
	 * @return The list of deserialized EObjects. May be empty, but never <code>null</code>.
	 */
	public static List<EObject> readEObjectsFromXMI(final String xmiContents, final Set<EPackage> ePackages) {
		checkNotNull(xmiContents, "Precondition violation - argument 'xmiContents' must not be NULL!");
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		Resource resource = createTemporaryResource("temp.xmi", ePackages);
		try (ByteArrayInputStream bais = new ByteArrayInputStream(xmiContents.getBytes())) {
			resource.load(bais, null);
			return resource.getContents();
		} catch (IOException ioe) {
			throw new XMIConversionFailedException("Could not read EObject(s) from XMI data!", ioe);
		}
	}

	/**
	 * Checks that the given {@link File} is an XMI file.
	 *
	 * @param file
	 *            The file to check. Must not be <code>null</code>.
	 *
	 * @return <code>true</code> if the given file is an XMI file, otherwise <code>false</code>.
	 *
	 * @see #assertIsXMIFile(File)
	 */
	public static boolean isXMIFile(final File file) {
		if (file == null) {
			return false;
		}
		if (file.exists() == false) {
			return false;
		}
		if (file.isFile() == false) {
			return false;
		}
		if (file.getName().endsWith(".xmi") == false) {
			return false;
		}
		return true;
	}

	/**
	 * Asserts that the given file is a valid, existing XMI file.
	 *
	 * @param file
	 *            The file to check. Must not be <code>null</code>.
	 *
	 * @throws IllegalArgumentException
	 *             Thrown if there are some irregularities with the given XMI file. The description of the exception
	 *             provides details.
	 */
	public static void assertIsXMIFile(final File file) {
		checkNotNull(file, "Precondition violation - argument 'file' must not be NULL!");
		checkArgument(file.exists(), "Precondition violation - the given XMI File does not exist!");
		checkArgument(file.isFile(), "Precondition violation - the given XMI File is a directory, not a file!");
		checkArgument(file.getName().endsWith(".xmi"),
				"Precondition violation - the given file does not end with '*.xmi'!");
	}

	/**
	 * A specialized version of {@link EObject#eGet(org.eclipse.emf.ecore.EStructuralFeature) eGet(...)} that works for
	 * multiplicity-many {@link EReference}s only.
	 *
	 * @param eObject
	 *            The eObject to run the operation on. Must not be <code>null</code>.
	 * @param eReference
	 *            The EReference to retrieve the targets for. Must not be <code>null</code>, must be multiplicity-many.
	 * @return The EObject-backed list of targets. May be empty, but never <code>null</code>.
	 */
	public static EList<EObject> eGetMany(final EObject eObject, final EReference eReference) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(eReference, "Precondition violation - argument 'eReference' must not be NULL!");
		checkArgument(eReference.isMany(), "Precondition violation - argument 'eReference' must be multiplicity-many!");
		@SuppressWarnings("unchecked")
		EList<EObject> list = (EList<EObject>) eObject.eGet(eReference);
		return list;
	}

	/**
	 * Given a collection of {@link EPackage}s, this method returns all {@link EReference}s with
	 * {@link EReference#getEReferenceType() target types} that are not contained in any of the given EPackages,
	 * E-Sub-Packages, or the Ecore EPackage.
	 *
	 * @param ePackages
	 *            The EPackages to check (recursively). Must not be <code>null</code>, may be empty.
	 * @return The set of {@link EReference}s that have target types outside the given EPackages (and also outside the
	 *         Ecore EPackage). Never <code>null</code>, may be empty.
	 *
	 * @see #areEPackagesSelfContained(Iterable)
	 * @see #assertEPackagesAreSelfContained(Iterable)
	 */
	public static Set<EReference> getEReferencesWithNonContainedEReferenceTypes(
			final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		// flatten the EPackage hierarchy
		Set<EPackage> allEPackages = flattenEPackages(ePackages);
		// all elements from the Ecore EPackage are also okay, even if they are not contained in the set above
		Set<EPackage> containedEPackages = Sets.newHashSet(allEPackages);
		containedEPackages.addAll(flattenEPackage(EcorePackage.eINSTANCE));
		// prepare the result set
		Set<EReference> resultSet = Sets.newHashSet();
		// for each ePackage, iterate over all EClasses, and find all EReferences with a target type that is NOT
		// contained in any known epackage
		for (EPackage ePackage : allEPackages) {
			for (EClassifier eClassifier : ePackage.getEClassifiers()) {
				if (eClassifier instanceof EClass == false) {
					// ignore non-eclasses
					continue;
				}
				EClass eClass = (EClass) eClassifier;
				for (EReference eReference : eClass.getEAllReferences()) {
					EClass eReferenceType = eReference.getEReferenceType();
					EPackage owningEPackage = eReferenceType.getEPackage();
					if (containedEPackages.contains(owningEPackage) == false) {
						resultSet.add(eReference);
					}
				}
			}
		}
		return resultSet;
	}

	/**
	 * Asserts that the given collection of {@link EPackage}s is self-contained, i.e. does not reference any
	 * {@link EClass}es that reside outside the given collection of EPackages (and also outside of the
	 * {@link EcorePackage} ).
	 *
	 * @param ePackages
	 *            The {@link EPackage}s to check. Must not be <code>null</code>. May be empty.
	 * @return <code>true</code> if the given collection of {@link EPackage}s is self-contained, otherwise
	 *         <code>false</code>.
	 *
	 * @see #getEReferencesWithNonContainedEReferenceTypes(Iterable)
	 * @see #assertEPackagesAreSelfContained(Iterable)
	 */
	public static boolean areEPackagesSelfContained(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		Set<EReference> nonContainedReferences = getEReferencesWithNonContainedEReferenceTypes(ePackages);
		if (nonContainedReferences.isEmpty() == false) {
			return false;
		} else {
			return true;
		}
	}

	/**
	 * Asserts that the given collection of {@link EPackage}s is self-contained, i.e. does not reference any
	 * {@link EClass}es that reside outside the given collection of EPackages (and also outside of the
	 * {@link EcorePackage} ).
	 *
	 * @param ePackages
	 *            The {@link EPackage}s to check. Must not be <code>null</code>. May be empty.
	 * @throws EPackagesAreNotSelfContainedException
	 *             Thrown if there is at least one {@link EReference} in the given EPackages that points to an EClass
	 *             that is not (recursively) contained in the given EPackages.
	 *
	 * @see #getEReferencesWithNonContainedEReferenceTypes(Iterable)
	 * @see #areEPackagesSelfContained(Iterable)
	 */
	public static void assertEPackagesAreSelfContained(final Iterable<? extends EPackage> ePackages)
			throws EPackagesAreNotSelfContainedException {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		Set<EReference> nonContainedReferences = getEReferencesWithNonContainedEReferenceTypes(ePackages);
		if (nonContainedReferences.isEmpty()) {
			return;
		}
		throw new EPackagesAreNotSelfContainedException(nonContainedReferences);
	}

	/**
	 * "Flattens" the given {@link EPackage} by iterating recursively over its sub-packages and throwing all encountered
	 * packages into a set.
	 *
	 * @param ePackage
	 *            The EPackage to flatten. Must not be <code>null</code>.
	 * @return A set containing the given EPackage, plus all of its sub-EPackages (recursively). Never <code>null</code>
	 *         , never empty.
	 */
	public static Set<EPackage> flattenEPackage(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		return flattenEPackages(Collections.singleton(ePackage));
	}

	/**
	 * "Flattens" the given collection of {@link EPackage}s by iterating recursively over their sub-packages and
	 * throwing all encountered packages into a set.
	 *
	 * @param ePackages
	 *            The EPackages to flatten. Must not be <code>null</code>. May be empty.
	 * @return A set containing the given EPackage, plus all of its sub-EPackages (recursively). Never <code>null</code>
	 *         , may be empty.
	 */
	public static Set<EPackage> flattenEPackages(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		Set<EPackage> flattenedEPackages = Sets.newHashSet();
		for (EPackage rootEPackage : ePackages) {
			flattenedEPackages.add(rootEPackage);
			TreeIterator<EObject> eAllContents = rootEPackage.eAllContents();
			eAllContents.forEachRemaining(eObject -> {
				if (eObject instanceof EPackage) {
					flattenedEPackages.add((EPackage) eObject);
				}
			});
		}
		return flattenedEPackages;
	}

	/**
	 * "Flattens" the given collection of {@link EPackage}s by iterating recursively over their sub-packages and
	 * throwing all encountered packages into a set.
	 *
	 * @param ePackages
	 *            The EPackages to flatten. Must not be <code>null</code>. May be empty.
	 * @return A set containing the given EPackage, plus all of its sub-EPackages (recursively). Never <code>null</code>
	 *         , may be empty.
	 */
	public static Set<EPackage> flattenEPackages(final Iterator<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		Set<EPackage> flattenedEPackages = Sets.newHashSet();
		while (ePackages.hasNext()) {
			EPackage rootEPackage = ePackages.next();
			flattenedEPackages.add(rootEPackage);
			TreeIterator<EObject> eAllContents = rootEPackage.eAllContents();
			eAllContents.forEachRemaining(eObject -> {
				if (eObject instanceof EPackage) {
					flattenedEPackages.add((EPackage) eObject);
				}
			});
		}
		return flattenedEPackages;
	}

	/**
	 * Finds and returns the {@link EReference} with the given name in the given {@link EClass}.
	 *
	 * @param eClass
	 *            The EClass to get the EReference from. Must not be <code>null</code>.
	 * @param name
	 *            The name of the EReference to look for. Must not be <code>null</code>.
	 *
	 * @return The EReference that is owned by the given EClass and has the given name, or <code>null</code> if none
	 *         exists.
	 */
	public static EReference getEReference(final EClass eClass, final String name) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		return eClass.getEAllReferences().stream().filter(eRef -> eRef.getName().equals(name)).findFirst().orElse(null);
	}

	/**
	 * Finds and returns the {@link EAttribute} with the given name in the given {@link EClass}.
	 *
	 * @param eClass
	 *            The EClass to get the EAttribute from. Must not be <code>null</code>.
	 * @param name
	 *            The name of the EAttribute to look for. Must not be <code>null</code>.
	 *
	 * @return The EAttribute that is owned by the given EClass and has the given name, or <code>null</code> if none
	 *         exists.
	 */
	public static EAttribute getEAttribute(final EClass eClass, final String name) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		checkNotNull(name, "Precondition violation - argument 'name' must not be NULL!");
		return eClass.getEAllAttributes().stream().filter(eAttr -> eAttr.getName().equals(name)).findFirst()
				.orElse(null);
	}

	/**
	 * Returns the fully qualified name for the given {@link EStructuralFeature feature}.
	 *
	 * @param feature
	 *            The feature to get the fully qualified name for. Must not be <code>null</code>.
	 * @return The fully qualified name for the given feature.
	 */
	public static String fullyQualifiedNameFor(final EStructuralFeature feature) {
		checkNotNull(feature, "Precondition violation - argument 'feature' must not be NULL!");
		String eClassName = fullyQualifiedNameFor(feature.getEContainingClass());
		return eClassName + CLASS_FEATURE_SEPARATOR + feature.getName();
	}

	/**
	 * Returns the fully qualified name for the given {@link EClass}.
	 *
	 * @param eClass
	 *            The EClass to get the fully qualified name for. Must not be <code>null</code>.
	 * @return The fully qualified name for the given EClass.
	 */
	public static String fullyQualifiedNameFor(final EClass eClass) {
		checkNotNull(eClass, "Precondition violation - argument 'eClass' must not be NULL!");
		String ePackageQualifiedName = fullyQualifiedNameFor(eClass.getEPackage());
		return ePackageQualifiedName + PACKAGE_PATH_SEPARATOR + eClass.getName();
	}

	/**
	 * Returns the fully qualified name for the given {@link EClassifier}.
	 *
	 * @param eClassifier
	 *            The EClassifier to get the fully qualified name for. Must not be <code>null</code>.
	 * @return The fully qualified name for the given EClassifier.
	 */
	public static String fullyQualifiedNameFor(final EClassifier eClassifier) {
		checkNotNull(eClassifier, "Precondition violation - argument 'eClassifier' must not be NULL!");
		String ePackageQualifiedName = fullyQualifiedNameFor(eClassifier.getEPackage());
		return ePackageQualifiedName + PACKAGE_PATH_SEPARATOR + eClassifier.getName();
	}

	/**
	 * Returns the fully qualified name for the given {@link EPackage}.
	 *
	 * @param ePackage
	 *            The EPackage to get the fully qualified name for. Must not be <code>null</code>.
	 * @return The fully qualified name for the given EPackage.
	 */
	public static String fullyQualifiedNameFor(final EPackage ePackage) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		List<String> qualifierChain = Lists.newArrayList();
		EPackage currentEPackage = ePackage;
		while (currentEPackage != null) {
			qualifierChain.add(currentEPackage.getName());
			currentEPackage = currentEPackage.getESuperPackage();
		}
		List<String> qualifiedNameParts = Lists.reverse(qualifierChain);
		StringBuilder builder = new StringBuilder();
		String separator = "";
		for (String namePart : qualifiedNameParts) {
			builder.append(separator);
			separator = PACKAGE_PATH_SEPARATOR;
			builder.append(namePart);
		}
		return builder.toString();
	}

	/**
	 * Returns all {@link EClass}es that are <i>directly</i> contained in the given {@link EPackage}.
	 *
	 * @param ePackage
	 *            The EPackage to get the directly contained classes for. Must not be <code>null</code>.
	 * @return An unmodifiable set of EClasses which are directly contained in the given EPackage. May be empty, but
	 *         never <code>null</code>.
	 *
	 * @see #allEClasses(EPackage)
	 */
	public static Set<EClass> eClasses(final EPackage ePackage) {
		EList<EClassifier> eClassifiers = ePackage.getEClassifiers();
		Set<EClass> eClasses = eClassifiers.stream().filter(classifier -> classifier instanceof EClass)
				.map(classifier -> (EClass) classifier).collect(Collectors.toSet());
		return Collections.unmodifiableSet(eClasses);
	}

	/**
	 * Returns an {@link Iterable} over all {@link EClass}es that are <i>directly or transitively</i> contained in the
	 * given {@link EPackage}.
	 *
	 * @param ePackage
	 *            The EPackage to get the directly and transitively contained EClasses for. Must not be
	 *            <code>null</code>.
	 * @return An unmodifiable set of EClasses which are directly or transitively contained in the given EPackage. May
	 *         be empty, but never <code>null</code>.
	 *
	 * @see #eClasses(EPackage)
	 */
	public static Set<EClass> allEClasses(final EPackage ePackage) {
		Set<EPackage> packages = flattenEPackage(ePackage);
		Set<EClass> eClasses = Sets.newHashSet();
		for (EPackage pack : packages) {
			eClasses.addAll(eClasses(pack));
		}
		return Collections.unmodifiableSet(eClasses);
	}

	public static Set<EClass> allEClasses(final Iterable<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		return allEClasses(ePackages.iterator());
	}

	public static Set<EClass> allEClasses(final EPackage[] ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		return allEClasses(Arrays.asList(ePackages));
	}

	public static Set<EClass> allEClasses(final Iterator<? extends EPackage> ePackages) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		Set<EClass> eClasses = Sets.newHashSet();
		while (ePackages.hasNext()) {
			EPackage ePackage = ePackages.next();
			eClasses.addAll(allEClasses(ePackage));
		}
		return Collections.unmodifiableSet(eClasses);
	}

	public static EPackage getEPackageByQualifiedName(final Iterable<EPackage> packages, final String qualifiedName) {
		checkNotNull(packages, "Precondition violation - argument 'packages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : packages) {
			EPackage resultPackage = getEPackageByQualifiedName(ePackage, qualifiedName);
			if (resultPackage != null) {
				return resultPackage;
			}
		}
		return null;
	}

	public static EPackage getEPackageByQualifiedName(final Iterator<EPackage> packages, final String qualifiedName) {
		checkNotNull(packages, "Precondition violation - argument 'packages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		while (packages.hasNext()) {
			EPackage ePackage = packages.next();
			EPackage resultPackage = getEPackageByQualifiedName(ePackage, qualifiedName);
			if (resultPackage != null) {
				return resultPackage;
			}
		}
		return null;
	}

	public static EPackage getEPackageByQualifiedName(final EPackage[] packages, final String qualifiedName) {
		checkNotNull(packages, "Precondition violation - argument 'packages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : packages) {
			EPackage resultPackage = getEPackageByQualifiedName(ePackage, qualifiedName);
			if (resultPackage != null) {
				return resultPackage;
			}
		}
		return null;
	}

	public static EPackage getEPackageByQualifiedName(final EPackage rootPackage, final String qualifiedName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		String[] parts = qualifiedName.split(PACKAGE_PATH_SEPARATOR);
		return getEPackageByQualifiedName(rootPackage, parts, 0);
	}

	private static EPackage getEPackageByQualifiedName(final EPackage ePackage, final String[] path,
			final int pathIndex) {
		String name = path[pathIndex];
		if (ePackage.getName().equals(name) == false) {
			return null;
		}
		if (pathIndex + 1 >= path.length) {
			// it's this package
			return ePackage;
		}
		// step deeper
		String subPackageName = path[pathIndex + 1];
		for (EPackage subPackage : ePackage.getESubpackages()) {
			if (subPackage.getName().equals(subPackageName)) {
				return getEPackageByQualifiedName(subPackage, path, pathIndex + 1);
			}
		}
		// not found
		return null;
	}

	public static EClassifier getEClassifierByQualifiedName(final EPackage rootPackage, final String qualifiedName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		String[] path = qualifiedName.split(PACKAGE_PATH_SEPARATOR);
		if (path.length <= 1) {
			// not a qualified name
			return null;
		}
		// split the path into the package path and the classifier name
		String classifierName = path[path.length - 1];
		String packagePath = qualifiedName.substring(0,
				qualifiedName.length() - classifierName.length() - PACKAGE_PATH_SEPARATOR.length());
		// get the EPackage
		EPackage ePackage = getEPackageByQualifiedName(rootPackage, packagePath);
		if (ePackage == null) {
			return null;
		}
		return ePackage.getEClassifier(classifierName);
	}

	public static EClassifier getEClassifierByQualifiedName(final Iterable<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EClassifier classifier = getEClassifierByQualifiedName(ePackage, qualifiedName);
			if (classifier != null) {
				return classifier;
			}
		}
		return null;
	}

	public static EClassifier getEClassifierByQualifiedName(final Iterator<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		while (rootPackages.hasNext()) {
			EPackage ePackage = rootPackages.next();
			EClassifier classifier = getEClassifierByQualifiedName(ePackage, qualifiedName);
			if (classifier != null) {
				return classifier;
			}
		}
		return null;
	}

	public static EClassifier getEClassifierByQualifiedName(final EPackage[] rootPackages, final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EClassifier classifier = getEClassifierByQualifiedName(ePackage, qualifiedName);
			if (classifier != null) {
				return classifier;
			}
		}
		return null;
	}

	public static EClass getEClassByQualifiedName(final EPackage rootPackage, final String qualifiedName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		EClassifier eClassifier = getEClassifierByQualifiedName(rootPackage, qualifiedName);
		return (EClass) eClassifier;
	}

	public static EClass getEClassByQualifiedName(final Iterable<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		EClassifier eClassifier = getEClassifierByQualifiedName(rootPackages, qualifiedName);
		return (EClass) eClassifier;
	}

	public static EClass getEClassByQualifiedName(final Iterator<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		EClassifier eClassifier = getEClassifierByQualifiedName(rootPackages, qualifiedName);
		return (EClass) eClassifier;
	}

	public static EClass getEClassByQualifiedName(final EPackage[] rootPackages, final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		EClassifier eClassifier = getEClassifierByQualifiedName(rootPackages, qualifiedName);
		return (EClass) eClassifier;
	}

	public static EStructuralFeature getFeatureByQualifiedName(final EPackage rootPackage, final String qualifiedName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return getFeatureByQualifiedName(rootPackage, qualifiedName, EStructuralFeature.class);
	}

	public static EStructuralFeature getFeatureByQualifiedName(final Iterable<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EStructuralFeature feature = getFeatureByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EStructuralFeature getFeatureByQualifiedName(final EPackage[] rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EStructuralFeature feature = getFeatureByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EStructuralFeature getFeatureByQualifiedName(final Iterator<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		while (rootPackages.hasNext()) {
			EPackage ePackage = rootPackages.next();
			EStructuralFeature feature = getFeatureByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static <T extends EStructuralFeature> T getFeatureByQualifiedName(final EPackage rootPackage,
			final String qualifiedName, final Class<T> featureClass) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		checkNotNull(featureClass, "Precondition violation - argument 'featureClass' must not be NULL!");
		int featureSeparatorIndex = qualifiedName.lastIndexOf(CLASS_FEATURE_SEPARATOR);
		if (featureSeparatorIndex < 0) {
			// not a qualified feature name
			return null;
		}
		String featureName = qualifiedName.substring(featureSeparatorIndex + 1, qualifiedName.length());
		String qualifiedClassName = qualifiedName.substring(0, featureSeparatorIndex);
		EClass eClass = getEClassByQualifiedName(rootPackage, qualifiedClassName);
		if (eClass == null) {
			return null;
		}
		if (featureClass.equals(EStructuralFeature.class)) {
			// we don't care about the type of feature
			return (T) eClass.getEStructuralFeature(featureName);
		} else if (featureClass.equals(EAttribute.class)) {
			return (T) getEAttribute(eClass, featureName);
		} else if (featureClass.equals(EReference.class)) {
			return (T) getEReference(eClass, featureName);
		} else {
			throw new RuntimeException("Unknown subtype of EStructuralFeature: " + featureClass.getCanonicalName());
		}
	}

	public static EAttribute getEAttributeByQualifiedName(final EPackage rootPackage, final String qualifiedName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return getFeatureByQualifiedName(rootPackage, qualifiedName, EAttribute.class);
	}

	public static EAttribute getEAttributeByQualifiedName(final Iterable<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EAttribute feature = getEAttributeByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EAttribute getEAttributeByQualifiedName(final EPackage[] rootPackages, final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EAttribute feature = getEAttributeByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EAttribute getEAttributeByQualifiedName(final Iterator<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		while (rootPackages.hasNext()) {
			EPackage ePackage = rootPackages.next();
			EAttribute feature = getEAttributeByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EReference getEReferenceByQualifiedName(final EPackage rootPackage, final String qualifiedName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		return getFeatureByQualifiedName(rootPackage, qualifiedName, EReference.class);
	}

	public static EReference getEReferenceByQualifiedName(final Iterable<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EReference feature = getEReferenceByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EReference getEReferenceByQualifiedName(final EPackage[] rootPackages, final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		for (EPackage ePackage : rootPackages) {
			EReference feature = getEReferenceByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EReference getEReferenceByQualifiedName(final Iterator<? extends EPackage> rootPackages,
			final String qualifiedName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(qualifiedName, "Precondition violation - argument 'qualifiedName' must not be NULL!");
		while (rootPackages.hasNext()) {
			EPackage ePackage = rootPackages.next();
			EReference feature = getEReferenceByQualifiedName(ePackage, qualifiedName);
			if (feature != null) {
				return feature;
			}
		}
		return null;
	}

	public static EPackage getEPackageBySimpleName(final EPackage rootPackage, final String simpleName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return getEPackageBySimpleName(Collections.singleton(rootPackage), simpleName);
	}

	public static EPackage getEPackageBySimpleName(final Iterable<? extends EPackage> rootPackages,
			final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return getEPackageBySimpleName(rootPackages.iterator(), simpleName);
	}

	public static EPackage getEPackageBySimpleName(final Iterator<? extends EPackage> rootPackages,
			final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		Set<EPackage> allPackages = flattenEPackages(rootPackages);
		EPackage resultPackage = null;
		for (EPackage ePackage : allPackages) {
			if (ePackage.getName().equals(simpleName)) {
				if (resultPackage == null) {
					resultPackage = ePackage;
				} else {
					throw new NameResolutionException("The simple EPackage name '" + simpleName
							+ "' is ambiguous! Candidates are '" + fullyQualifiedNameFor(resultPackage) + "' and '"
							+ fullyQualifiedNameFor(ePackage) + "' (potential others as well).");
				}
			}
		}
		return resultPackage;
	}

	public static EPackage getEPackageBySimpleName(final EPackage[] rootPackages, final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return getEPackageBySimpleName(Arrays.asList(rootPackages), simpleName);
	}

	public static EClassifier getEClassifierBySimpleName(final EPackage rootPackage, final String simpleName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return getEClassifierBySimpleName(Collections.singleton(rootPackage), simpleName);
	}

	public static EClassifier getEClassifierBySimpleName(final Iterable<? extends EPackage> rootPackages,
			final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return getEClassifierBySimpleName(rootPackages.iterator(), simpleName);
	}

	public static EClassifier getEClassifierBySimpleName(final EPackage[] rootPackages, final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return getEClassifierBySimpleName(Arrays.asList(rootPackages), simpleName);
	}

	public static EClassifier getEClassifierBySimpleName(final Iterator<? extends EPackage> rootPackages,
			final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		Set<EPackage> allPackages = flattenEPackages(rootPackages);
		EClassifier resultClassifier = null;
		for (EPackage ePackage : allPackages) {
			EClassifier classifier = ePackage.getEClassifier(simpleName);
			if (classifier != null) {
				if (resultClassifier == null) {
					resultClassifier = classifier;
				} else {
					throw new NameResolutionException("The simple EClassifier name '" + simpleName
							+ "' is ambiguous! Candidates are '" + fullyQualifiedNameFor(resultClassifier) + "' and '"
							+ fullyQualifiedNameFor(classifier) + "' (potential others as well).");
				}
			}
		}
		return resultClassifier;
	}

	public static EClass getEClassBySimpleName(final EPackage rootPackage, final String simpleName) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return (EClass) getEClassifierBySimpleName(rootPackage, simpleName);
	}

	public static EClass getEClassBySimpleName(final Iterable<? extends EPackage> rootPackages,
			final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackage' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return (EClass) getEClassifierBySimpleName(rootPackages, simpleName);
	}

	public static EClass getEClassBySimpleName(final EPackage[] rootPackages, final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return (EClass) getEClassifierBySimpleName(rootPackages, simpleName);
	}

	public static EClass getEClassBySimpleName(final Iterator<? extends EPackage> rootPackages,
			final String simpleName) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		checkNotNull(simpleName, "Precondition violation - argument 'simpleName' must not be NULL!");
		return (EClass) getEClassifierBySimpleName(rootPackages, simpleName);
	}

	@SuppressWarnings("unchecked")
	public static <T> T eGet(final EObject eObject, final String featureName) {
		checkNotNull(eObject, "Precondition violation - argument 'eObject' must not be NULL!");
		checkNotNull(featureName, "Precondition violation - argument 'featureName' must not be NULL!");
		EStructuralFeature feature = eObject.eClass().getEStructuralFeature(featureName);
		if (feature == null) {
			throw new IllegalArgumentException(
					"EClass '" + eObject.eClass().getName() + "' does not have a feature named '" + featureName + "'!");
		}
		return (T) eObject.eGet(feature);
	}

	public static Set<EReference> getEReferencesToEClass(final EPackage ePackage, final EClass targetEClass) {
		checkNotNull(ePackage, "Precondition violation - argument 'ePackage' must not be NULL!");
		checkNotNull(targetEClass, "Precondition violation - argument 'targetEClass' must not be NULL!");
		return getEReferencesToEClass(Collections.singleton(ePackage), targetEClass);
	}

	public static Set<EReference> getEReferencesToEClass(final Iterable<? extends EPackage> ePackages,
			final EClass targetEClass) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(targetEClass, "Precondition violation - argument 'targetEClass' must not be NULL!");
		return getEReferencesToEClass(ePackages.iterator(), targetEClass);
	}

	public static Set<EReference> getEReferencesToEClass(final EPackage[] ePackages, final EClass targetEClass) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(targetEClass, "Precondition violation - argument 'targetEClass' must not be NULL!");
		return getEReferencesToEClass(Arrays.asList(ePackages), targetEClass);
	}

	public static Set<EReference> getEReferencesToEClass(final Iterator<? extends EPackage> ePackages,
			final EClass targetEClass) {
		checkNotNull(ePackages, "Precondition violation - argument 'ePackages' must not be NULL!");
		checkNotNull(targetEClass, "Precondition violation - argument 'targetEClass' must not be NULL!");
		Set<EReference> resultSet = Sets.newHashSet();
		Set<EPackage> packages = flattenEPackages(ePackages);
		for (EPackage ePackage : packages) {
			for (EClass eClass : eClasses(ePackage)) {
				for (EReference eReference : eClass.getEAllReferences()) {
					if (eReference.getEReferenceType().isSuperTypeOf(targetEClass)) {
						resultSet.add(eReference);
					}
				}
			}
		}
		return resultSet;
	}

	public static SetMultimap<EClass, EReference> eClassToIncomingEReferences(
			final Iterable<? extends EPackage> rootPackages) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		SetMultimap<EClass, EReference> multimap = HashMultimap.create();
		for (EPackage ePackage : rootPackages) {
			for (EClass eClass : allEClasses(ePackage)) {
				multimap.putAll(eClass, getEReferencesToEClass(rootPackages, eClass));
			}
		}
		return multimap;
	}

	public static SetMultimap<EClass, EReference> eClassToIncomingEReferences(
			final Iterator<? extends EPackage> rootPackages) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		return eClassToIncomingEReferences(Lists.newArrayList(rootPackages));
	}

	public static SetMultimap<EClass, EReference> eClassToIncomingEReferences(final EPackage[] rootPackages) {
		checkNotNull(rootPackages, "Precondition violation - argument 'rootPackages' must not be NULL!");
		return eClassToIncomingEReferences(Arrays.asList(rootPackages));
	}

	public static SetMultimap<EClass, EReference> eClassToIncomingEReferences(final EPackage rootPackage) {
		checkNotNull(rootPackage, "Precondition violation - argument 'rootPackage' must not be NULL!");
		return eClassToIncomingEReferences(Collections.singleton(rootPackage));
	}
}
