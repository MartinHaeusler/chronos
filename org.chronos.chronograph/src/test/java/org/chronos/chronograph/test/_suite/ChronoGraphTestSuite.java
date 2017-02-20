package org.chronos.chronograph.test._suite;

import org.chronos.chronodb.test._suite.ChronoDBTestSuite;
import org.chronos.common.test.junit.PackageSuite;
import org.chronos.common.test.junit.SuiteIncludes;
import org.chronos.common.test.junit.SuitePackages;
import org.junit.runner.RunWith;

@RunWith(PackageSuite.class)
@SuitePackages("org.chronos.chronograph.test")
@SuiteIncludes(ChronoDBTestSuite.class)
public class ChronoGraphTestSuite {

}