package org.chronos.chronodb.internal.impl.dump.entry;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.api.key.ChronoIdentifier;

public class ChronoDBDumpPlainEntry extends ChronoDBDumpEntry<Object> {

	private Object value;
	private String converterClassName;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected ChronoDBDumpPlainEntry() {
		// constructor for serialization purposes only
	}

	public ChronoDBDumpPlainEntry(final ChronoIdentifier identifier, final Object value,
			final ChronoConverter<?, ?> converter) {
		super(identifier);
		this.setValue(value);
		if (converter != null) {
			this.converterClassName = converter.getClass().getName();
		} else {
			// this happens if we are dealing with a well-known object that needs no converter.
			this.converterClassName = null;
		}
	}

	// =====================================================================================================================
	// GETTERS & SETTERS
	// =====================================================================================================================

	@Override
	public void setValue(final Object value) {
		this.value = value;
	}

	@Override
	public Object getValue() {
		return this.value;
	}

	public String getConverterClassName() {
		return this.converterClassName;
	}

	public void setConverterClassName(final String converterClassName) {
		this.converterClassName = converterClassName;
	}

}
