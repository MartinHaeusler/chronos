package org.chronos.chronodb.internal.util;

import static com.google.common.base.Preconditions.*;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ChronosFileUtils {

	private static final int UNZIP_BUFFER_SIZE_BYTES = 4096;

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

	/**
	 * Extracts the given zip file into the given target directory.
	 *
	 * @param zipFile
	 *            The *.zip file to extract. Must not be <code>null</code>, must refer to an existing file.
	 * @param targetDirectory
	 *            The target directory to extract the data to. Must not be <code>null</code>, must be an existing directory.
	 * @throws IOException
	 *             Thrown if an I/O error occurs during the process.
	 */
	public static void extractZipFile(final File zipFile, final File targetDirectory) throws IOException {
		checkNotNull(zipFile, "Precondition violation - argument 'zipFile' must not be NULL!");
		checkNotNull(targetDirectory, "Precondition violation - argument 'targetDirectory' must not be NULL!");
		checkArgument(zipFile.exists(), "Precondition violation - argument 'zipFile' must refer to an existing file!");
		checkArgument(targetDirectory.exists(), "Precondition violation - argument 'targetDirectory' must refer to an existing file!");
		checkArgument(zipFile.isFile(), "Precondition violation - argument 'zipFile' must point to a file (not a directory)!");
		checkArgument(targetDirectory.isDirectory(), "Precondition violation - argument 'targetDirectory' must point to a directory (not a file)!");
		try (ZipInputStream zin = new ZipInputStream(new FileInputStream(zipFile))) {
			ZipEntry entry;
			String name;
			String dir;
			while ((entry = zin.getNextEntry()) != null) {
				name = entry.getName();
				if (entry.isDirectory()) {
					mkdirs(targetDirectory, name);
					continue;
				}
				/*
				 * this part is necessary because file entry can come before directory entry where is file located i.e.: /foo/foo.txt /foo/
				 */
				dir = dirpart(name);
				if (dir != null) {
					mkdirs(targetDirectory, dir);
				}

				extractFile(zin, targetDirectory, name);
			}
		}
	}

	private static void extractFile(final ZipInputStream in, final File outdir, final String name) throws IOException {
		byte[] buffer = new byte[UNZIP_BUFFER_SIZE_BYTES];
		BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(new File(outdir, name)));
		int count = -1;
		while ((count = in.read(buffer)) != -1) {
			out.write(buffer, 0, count);
		}
		out.close();
	}

	private static void mkdirs(final File outdir, final String path) {
		File d = new File(outdir, path);
		if (!d.exists()) {
			d.mkdirs();
		}
	}

	private static String dirpart(final String name) {
		int s = name.lastIndexOf(File.separatorChar);
		return s == -1 ? null : name.substring(0, s);
	}
}
