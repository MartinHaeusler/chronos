package org.chronos.chronodb.internal.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;

public class ChronosFileUtils {

	public static boolean isGZipped(final File file) {
		// code taken from: http://stackoverflow.com/a/30507742/3094906
		int magic = 0;
		try {
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			magic = raf.read() & 0xff | raf.read() << 8 & 0xff00;
			raf.close();
		} catch (Throwable e) {
			e.printStackTrace(System.err);
		}
		return magic == GZIPInputStream.GZIP_MAGIC;
	}

	public static boolean isExistingFile(final File file) {
		if (file == null) {
			return false;
		}
		if (file.exists() == false) {
			return false;
		}
		if (file.isFile() == false) {
			return false;
		}
		return true;
	}

}
