package org.chronos.chronosphere.impl.query;

import static com.google.common.base.Preconditions.*;

import java.util.Comparator;

import org.chronos.chronosphere.api.query.Order;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EObject;

public class EObjectAttributeComparator implements Comparator<EObject> {

	private final EAttribute eAttribute;
	private final Order order;

	public EObjectAttributeComparator(final EAttribute eAttribute, final Order order) {
		checkNotNull(eAttribute, "Precondition violation - argument 'eAttribute' must not be NULL!");
		checkNotNull(order, "Precondition violation - argument 'order' must not be NULL!");
		this.eAttribute = eAttribute;
		this.order = order;
	}

	@Override
	public int compare(final EObject o1, final EObject o2) {
		int comparison = this.compareInternal(o1, o2);
		if (this.order.equals(Order.DESC)) {
			return comparison * -1;
		} else {
			return comparison;
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected int compareInternal(final EObject o1, final EObject o2) {
		if (o1 == null && o2 == null) {
			return 0;
		} else if (o1 == null && o2 != null) {
			return 1;
		} else if (o1 != null && o2 == null) {
			return -1;
		}
		Object val1 = o1.eGet(this.eAttribute);
		Object val2 = o2.eGet(this.eAttribute);
		if (val1 == null && val2 == null) {
			return 0;
		} else if (val1 != null && val2 == null) {
			return 1;
		} else if (val1 == null && val2 != null) {
			return -1;
		}
		if (val1 instanceof Comparable == false) {
			throw new IllegalStateException(
					"Cannot execute comparison! Value '" + val1 + "' (class: '" + val1.getClass().getCanonicalName()
							+ "') retrieved via EAttribute '" + this.eAttribute.getName() + "' is not Comparable!");
		}
		if (val2 instanceof Comparable == false) {
			throw new IllegalStateException(
					"Cannot execute comparison! Value '" + val2 + "' (class: '" + val2.getClass().getCanonicalName()
							+ "') retrieved via EAttribute '" + this.eAttribute.getName() + "' is not Comparable!");
		}
		Comparable compVal1 = (Comparable) val1;
		Comparable compVal2 = (Comparable) val2;
		return compVal1.compareTo(compVal2);
	}

}
