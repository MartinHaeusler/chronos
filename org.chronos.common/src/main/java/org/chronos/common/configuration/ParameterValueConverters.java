package org.chronos.common.configuration;

import java.io.File;

public class ParameterValueConverters {

	public static class StringToFileConverter implements ParameterValueConverter {

		@Override
		public Object convert(final Object rawParameter) {
			String path = String.valueOf(rawParameter);
			File file = new File(path);
			return file;
		}

	}
}
