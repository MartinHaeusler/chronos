package org.chronos.chronosphere.test._suite;

import org.chronos.chronograph.test._suite.ChronoGraphTestSuite;
import org.chronos.common.test.junit.ExcludeCategories;
import org.chronos.common.test.junit.PackageSuite;
import org.chronos.common.test.junit.SuiteIncludes;
import org.chronos.common.test.junit.SuitePackages;
import org.chronos.common.test.junit.categories.PerformanceTest;
import org.chronos.common.test.junit.categories.SlowTest;
import org.junit.runner.RunWith;

@ExcludeCategories({ PerformanceTest.class, SlowTest.class })
@RunWith(PackageSuite.class)
@SuitePackages("org.chronos.chronosphere.test")
@SuiteIncludes(ChronoGraphTestSuite.class)
public class ChronoSphereTestSuite {

}
