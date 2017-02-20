package org.chronos.chronograph.internal.impl.util;

import java.util.UUID;

public class ChronoId {

	public static String random() {
		return UUID.randomUUID().toString();
	}

}
