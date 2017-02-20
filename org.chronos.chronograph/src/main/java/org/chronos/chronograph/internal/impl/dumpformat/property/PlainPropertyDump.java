package org.chronos.chronograph.internal.impl.dumpformat.property;

import org.chronos.chronodb.api.dump.ChronoConverter;
import org.chronos.chronodb.api.exceptions.ChronoDBStorageBackendException;
import org.chronos.chronograph.internal.impl.structure.record.PropertyRecord;
import org.chronos.common.util.ReflectionUtils;

public class PlainPropertyDump extends AbstractPropertyDump {

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private Object value;
	private String converterClass;

	// =====================================================================================================================
	// CONSTRUCTORS
	// =====================================================================================================================

	protected PlainPropertyDump() {
		// serialization constructor
	}

	public PlainPropertyDump(final PropertyRecord record) {
		this(record, null);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public PlainPropertyDump(final PropertyRecord record, final ChronoConverter valueConverter) {
		super(record.getKey());
		if (valueConverter != null) {
			this.converterClass = valueConverter.getClass().getName();
			this.value = valueConverter.writeToOutput(record.getValue());
		} else {
			this.converterClass = null;
			this.value = record.getValue();
		}

	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Object getValue() {
		if (this.value == null) {
			return null;
		}
		if (this.converterClass == null) {
			// no converter given -> return the object directly
			return this.value;
		} else {
			// instantiate the converter and run it
			ChronoConverter converter = null;
			try {
				Class<?> converterClass = Class.forName(this.converterClass);
				converter = (ChronoConverter<?, ?>) ReflectionUtils.instantiate(converterClass);
			} catch (Exception e) {
				throw new ChronoDBStorageBackendException("Failed to instantiate value converter class '"
						+ this.converterClass + "', cannot deserialize value!", e);
			}
			try {
				return converter.readFromInput(this.value);
			} catch (Exception e) {
				throw new ChronoDBStorageBackendException("Failed to run converter of type '"
						+ converter.getClass().getName() + "' on value of type '" + this.value.getClass() + "'!", e);
			}
		}
	}

}
