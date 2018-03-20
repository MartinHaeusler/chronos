package org.chronos.common.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import com.google.common.base.Preconditions;

public class ClasspathUtils {

	public static void printClasspathJars() {
		ClassLoader cl = ClasspathUtils.class.getClassLoader();

		URL[] urls = ((URLClassLoader) cl).getURLs();

		for (URL url : urls) {
			System.out.println(url.getFile());
		}
	}

	public static URL getURIFromResourceByName(final String resourceName) {
		URL url = ClasspathUtils.class.getClassLoader().getResource(resourceName);
		if (url == null) {
			return null;
		}
		return url;
	}

	public static String getPathFromResourceByName(final String resourceName) {
		URL url = ClasspathUtils.class.getClassLoader().getResource(resourceName);
		if (url == null) {
			return null;
		}
		String path = url.toExternalForm();
		return path.substring(0, path.length() - resourceName.length());
	}

	/**
	 * Checks if there is a resource with the given name on the Java classpath.
	 *
	 * @param resourceName
	 *            The name of the resource to check. Must not be <code>null</code>. Must contain a non-whitespace character.
	 * @return <code>true</code> if the given name refers to an existing classpath resource, otherwise <code>false</code>.
	 */
	public static boolean isClasspathResource(final String resourceName) {
		Preconditions.checkNotNull(resourceName, "Cannot check classpath existence of resource with name NULL!");
		Preconditions.checkArgument(resourceName.trim().isEmpty() == false,
				"Cannot check classpath existence of resource with name EMPTY!");
		URL url = ClasspathUtils.class.getClassLoader().getResource(resourceName);
		return url != null;
	}

	public static File getResourceAsFile(final String resourceName) throws IOException {
		URL url = getURIFromResourceByName(resourceName);
		if (url == null) {
			return null;
		}
		// NOTE: url.getFile() is *dangerous* because space characters in the filepath will be encoded as
		// '%20', which will lead to failure later on (FileNotFoundException).
		// Therefore, instead of:
		// --- new File(url.getFile());
		// it is safer to use:
		// --- new File(url.toURI());
		try {
			return new File(url.toURI());
		} catch (URISyntaxException e) {
			throw new IOException("Unable to read file resource '" + resourceName + "'! See root cause for details.", e);
		} catch (IllegalArgumentException e) {
			// most like the file is zipped/war'ed/jar'ed, buildLRU temporary file from stream
			int extensionIndex = resourceName.lastIndexOf(".");
			String tmpExtension = resourceName.substring(extensionIndex);
			File tmpResource = null;
			try {
				// input stream to tmp file
				InputStream resourceInputStream = ClasspathUtils.class.getClassLoader()
						.getResourceAsStream(resourceName);
				tmpResource = File.createTempFile("tmp", tmpExtension);
				Files.copy(resourceInputStream, tmpResource.toPath(), StandardCopyOption.REPLACE_EXISTING);
				tmpResource.deleteOnExit();
				return tmpResource;
			} catch (IOException ioex) {
				String tmpFilePath = "<unknown>";
				if (tmpResource != null) {
					tmpFilePath = tmpResource.getAbsolutePath();
				}
				throw new IOException("Failed to read file resource '" + resourceName + "' from JAR, copied it to temp directory (" + tmpFilePath + "), but still failed to load it! See root cause for details.", ioex);
			}
		}
	}

}
