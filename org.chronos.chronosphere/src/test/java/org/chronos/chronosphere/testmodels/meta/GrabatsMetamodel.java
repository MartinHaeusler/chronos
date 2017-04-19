package org.chronos.chronosphere.testmodels.meta;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.chronos.chronosphere.emf.internal.util.EMFUtils;
import org.eclipse.emf.ecore.EPackage;

import com.google.common.collect.Lists;

public class GrabatsMetamodel {

	private static final String GRABATS_DIR = "testmetamodels/grabats/";

	public static List<EPackage> createCFGEPackages() {
		return loadGrabatsEPackages("CFG.ecore");
	}

	public static List<EPackage> createJDTASTEPackages() {
		return loadGrabatsEPackages("JDTAST.ecore");
	}

	public static List<EPackage> createPGEPackages() {
		return loadGrabatsEPackages("PDG.ecore");
	}

	public static List<EPackage> createQ1ViewEPackages() {
		return loadGrabatsEPackages("Q1View.ecore");
	}

	public static List<EPackage> createAllEPackages() {
		List<EPackage> resultList = Lists.newArrayList();
		resultList.addAll(createCFGEPackages());
		resultList.addAll(createJDTASTEPackages());
		resultList.addAll(createPGEPackages());
		resultList.addAll(createQ1ViewEPackages());
		return resultList;
	}

	private static List<EPackage> loadGrabatsEPackages(final String ecoreFileName) {
		try {
			InputStream stream = GrabatsMetamodel.class.getClassLoader()
					.getResourceAsStream(GRABATS_DIR + ecoreFileName);
			String xmiContents = IOUtils.toString(stream, Charset.forName("utf-8"));
			return EMFUtils.readEPackagesFromXMI(xmiContents);
		} catch (IOException ioe) {
			throw new RuntimeException("Failed to load Ecore test file!", ioe);
		}
	}
}
