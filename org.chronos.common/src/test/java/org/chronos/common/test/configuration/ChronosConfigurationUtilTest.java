package org.chronos.common.test.configuration;

import static org.junit.Assert.*;

import java.time.DayOfWeek;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.chronos.common.configuration.ChronosConfigurationUtil;
import org.chronos.common.exceptions.ChronosConfigurationException;
import org.chronos.common.test.configuration.MyConfiguration.MyEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class ChronosConfigurationUtilTest {

	private TestAppender logSpy;

	@Before
	public void installTestAppender() {
		this.logSpy = new TestAppender();
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		this.logSpy.setContext(lc);
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.addAppender(this.logSpy);
	}

	@After
	public void uninstallTestAppender() {
		Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
		root.detachAppender(this.logSpy);
		this.logSpy.clearEvents();
		this.logSpy = null;
	}

	@Test
	public void correctConfiguration1Works() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_correct1.properties");
		MyConfiguration config = ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
		assertEquals("Martin", config.getName());
		assertEquals(47, config.getIntValue());
		assertEquals(DayOfWeek.SUNDAY, config.getDayOfWeek());
	}

	@Test
	public void correctConfiguration2Works() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_correct2.properties");
		MyConfiguration config = ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
		assertEquals("Martin", config.getName());
		assertEquals(47, config.getIntValue());
		assertEquals(DayOfWeek.MONDAY, config.getDayOfWeek());
		assertEquals(true, config.isHangover());
		assertEquals(100.0, config.getMotivation(), 0.1d);
	}

	@Test
	public void valueAliasingWorks() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_aliasing.properties");
		MyConfiguration config = ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
		assertEquals("Martin", config.getName());
		assertEquals(47, config.getIntValue());
		assertEquals(DayOfWeek.MONDAY, config.getDayOfWeek());
		assertEquals(true, config.isHangover());
		assertEquals(100.0, config.getMotivation(), 0.1d);
	}

	@Test
	public void customValueParsingWorks() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_valueparser.properties");
		MyConfiguration config = ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
		assertEquals("Martin", config.getName());
		assertEquals(47, config.getIntValue());
		assertEquals(DayOfWeek.MONDAY, config.getDayOfWeek());
		assertEquals(true, config.isHangover());
		assertEquals(100.0, config.getMotivation(), 0.1d);
		assertEquals("123", config.getCoordinate().getX());
		assertEquals("456", config.getCoordinate().getY());
	}

	@Test
	public void configWithSuperfluousParametersWorks() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_superfluous1.properties");
		MyConfiguration config = ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
		assertEquals("Martin", config.getName());
		assertEquals(47, config.getIntValue());
		assertEquals(DayOfWeek.MONDAY, config.getDayOfWeek());
		assertEquals(true, config.isHangover());
		assertEquals(100.0, config.getMotivation(), 0.1d);
		// assert that we have printed a warning
		assertEquals(1, this.logSpy.getWarnings().size());
	}

	@Test(expected = ChronosConfigurationException.class)
	public void missingValuesAreDetectedProperly() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_error1.properties");
		ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
	}

	@Test(expected = ChronosConfigurationException.class)
	public void wrongEnumValuesAreDetectedProperly() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_error2.properties");
		ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
	}

	@Test
	public void enumFactoryMethodAnnotationWorks() throws ConfigurationException {
		Configuration apacheConfig = new PropertiesConfiguration("myconfiguration_enumFactoryMethod.properties");
		MyConfiguration config = ChronosConfigurationUtil.build(apacheConfig, MyConfiguration.class);
		assertEquals(MyEnum.THREE, config.getMyEnum());

	}

	@Test
	public void canDependOnAValueOfTypeBoolean_case1() throws ConfigurationException {
		// case 1: boolean is true, and dependent value is present
		Configuration apacheConfig = new PropertiesConfiguration("booleanDependentConfiguration_correct.properties");
		BooleanDependentConfiguration config = ChronosConfigurationUtil.build(apacheConfig,
				BooleanDependentConfiguration.class);
		assertEquals(true, config.getBool());
		assertEquals("yes", config.getString());
	}

	@Test
	public void canDependOnAValueOfTypeBoolean_case2() throws ConfigurationException {
		// case 2: boolean is false
		Configuration apacheConfig = new PropertiesConfiguration("booleanDependentConfiguration_correct2.properties");
		BooleanDependentConfiguration config = ChronosConfigurationUtil.build(apacheConfig,
				BooleanDependentConfiguration.class);
		assertEquals(false, config.getBool());
	}

	@Test
	public void canDependOnAValueOfTypeBoolean_case3() throws ConfigurationException {
		// case 3: boolean is true, but dependent value is missing
		try {
			Configuration apacheConfig = new PropertiesConfiguration("booleanDependentConfiguration_wrong.properties");
			ChronosConfigurationUtil.build(apacheConfig, BooleanDependentConfiguration.class);
			fail();
		} catch (ChronosConfigurationException expected) {
			// pass
		}
	}

	private static class TestAppender extends AppenderBase<ILoggingEvent> {

		private List<ILoggingEvent> events = Lists.newArrayList();

		@Override
		public String getName() {
			return "LogSpy";
		}

		@Override
		public void doAppend(final ILoggingEvent eventObject) {
			this.events.add(eventObject);
		}

		public void clearEvents() {
			this.events.clear();
		}

		public List<ILoggingEvent> getEvents() {
			return Collections.unmodifiableList(this.events);
		}

		public List<ILoggingEvent> getWarnings() {
			return this.getEvents().stream().filter(e -> e.getLevel().equals(Level.WARN)).collect(Collectors.toList());
		}

		@SuppressWarnings("unused")
		public List<ILoggingEvent> getErrors() {
			return this.getEvents().stream().filter(e -> e.getLevel().equals(Level.ERROR)).collect(Collectors.toList());
		}

		@Override
		protected void append(final ILoggingEvent eventObject) {
			// unused; see 'doAppend'
		}

	}
}
