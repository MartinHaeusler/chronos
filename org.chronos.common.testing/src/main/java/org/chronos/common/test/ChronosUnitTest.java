package org.chronos.common.test;

import static com.google.common.base.Preconditions.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.chronos.common.util.ReflectionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ChronosUnitTest {

	private static final Logger log = LoggerFactory.getLogger(ChronosUnitTest.class);

	protected static File tempDir;

	// =================================================================================================================
	// CLASS SETUP & CLEANUP
	// =================================================================================================================

	static {
		// note: we use 'static' here instead of '@BeforeClass', because JUnit calls
		// methods annotated with '@Parameters' first. In case of ChronoDB tests, this
		// will result in db instances being produced BEFORE the temp dir is ready. By
		// declaring this code inside a static initializer, we ensure that it gets called
		// before the data for parameterized tests is constructed.
		try {
			Path tempDirPath = Files.createTempDirectory("ChronoDBTest");
			tempDir = tempDirPath.toFile();
			tempDir.mkdirs();
		} catch (IOException e) {
			log.info("Failed to provide temp directory: " + e.toString());
		}
	}

	@BeforeClass
	public static void beforeClass() {
		// note: we need to make sure again that the temp dir is created.
		// This is necessary in addition to the static initializer because when multipe
		// classes that inherit from this class are instantiated during a Test Suite,
		// then @AfterClass will delete it after the first test class has finished, so
		// all subsequent test classes won't have it, unless we recreate it here.
		tempDir.mkdirs();
		log.info("Temp directory is provided at: " + tempDir.getAbsolutePath());
	}

	@AfterClass
	public static void afterClass() {
		if (tempDir != null && tempDir.exists()) {
			log.info("Attempting to delete temp directory provided at: " + tempDir.getAbsolutePath());
			try {
				FileUtils.deleteDirectory(tempDir);
				log.info("Successfully deleted temp directory");
			} catch (IOException e) {
				log.warn("Failed to delete temp directory: " + e.toString());
			}
		}
	}

	// =================================================================================================================
	// FIELDS
	// =================================================================================================================

	private File testDirectory;

	@Rule
	public TestRule watcher = new ChronosTestWatcher();

	@Rule
	public TestName testName = new TestName();

	// =================================================================================================================
	// TEST METHOD SETUP & CLEANUP
	// =================================================================================================================

	@Before
	public void setup() {
		this.deleteTestDirectory();
		String testDirName = "TestDirectory" + UUID.randomUUID().toString().replaceAll("-", "");
		this.testDirectory = new File(tempDir, testDirName);
		this.testDirectory.mkdir();
	}

	@After
	public void tearDown() {
		this.deleteTestDirectory();
	}

	private void deleteTestDirectory() {
		if (this.testDirectory == null) {
			return;
		}
		if (this.testDirectory.exists() == false) {
			this.testDirectory = null;
			return;
		}
		try {
			FileUtils.deleteDirectory(this.testDirectory);
		} catch (IOException e) {
			log.warn("Failed to delete the test directory!");
		}
	}

	protected File getTestDirectory() {
		return this.testDirectory;
	}

	protected Method getCurrentTestMethod() {
		String methodName = this.testName.getMethodName();
		if (methodName == null) {
			return null;
		}
		// strip away the trailing "[case]" added by JUnit for parameterized tests (if any)
		methodName = methodName.replaceAll("\\[.*\\]", "");
		return ReflectionUtils.getDeclaredMethod(this.getClass(), methodName);
	}

	// =================================================================================================================
	// UTILITY METHODS
	// =================================================================================================================

	protected void sleep(final long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	protected File getSrcTestResourcesFile(final String fileName) {
		checkNotNull(fileName, "Precondition violation - argument 'fileName' must not be NULL!");
		URL url = this.getClass().getResource("/" + fileName);
		File file;
		try {
			file = new File(url.toURI());
			return file;
		} catch (URISyntaxException e) {
			fail(e.toString());
			return null;
		}
	}
}
