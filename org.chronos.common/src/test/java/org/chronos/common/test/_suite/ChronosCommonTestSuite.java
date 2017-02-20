package org.chronos.common.test._suite;

import org.chronos.common.test.collections.immutable.HashArrayMappedTreeTest;
import org.chronos.common.test.configuration.ChronosConfigurationUtilTest;
import org.chronos.common.test.util.BitFieldUtilTest.BitFieldUtilTest;
import org.chronos.common.test.version.ChronosVersionTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
		//
		ChronosConfigurationUtilTest.class,
		//
		ChronosVersionTest.class,
		//
		BitFieldUtilTest.class,
		//
		HashArrayMappedTreeTest.class
		//
})
public class ChronosCommonTestSuite {

}
