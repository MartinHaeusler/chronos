package org.chronos.chronosphere.api.exceptions;

import static com.google.common.base.Preconditions.*;

import org.eclipse.emf.ecore.EReference;

public class EPackagesAreNotSelfContainedException extends ChronoSphereConfigurationException {

	private final String message;

	public EPackagesAreNotSelfContainedException(final Iterable<? extends EReference> violatingEReferences) {
		checkNotNull(violatingEReferences,
				"Precondition violation - argument 'violatingEReferences' must not be NULL!");
		this.message = this.generateMessage(violatingEReferences);
	}

	private String generateMessage(final Iterable<? extends EReference> eReferences) {
		StringBuilder msg = new StringBuilder();
		msg.append("The given EPackages are not self-contained. "
				+ "There are EReferences that point to non-contained EClasses. These are:");
		for (EReference eReference : eReferences) {
			msg.append("\n");
			msg.append(eReference.getEContainingClass().getEPackage().getName());
			msg.append("::");
			msg.append(eReference.getEContainingClass().getName());
			msg.append("#");
			msg.append(eReference.getName());
			msg.append(" -> ");
			msg.append(eReference.getEReferenceType().getName());
			msg.append(eReference.getEReferenceType().getEPackage().getName());
			msg.append(" [NOT CONTAINED]");
		}
		msg.append("\n");
		return msg.toString();
	}

	@Override
	public String getMessage() {
		return this.message;
	}

}
