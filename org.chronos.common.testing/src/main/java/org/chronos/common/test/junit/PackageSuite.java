package org.chronos.common.test.junit;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.experimental.categories.Categories.ExcludeCategory;
import org.junit.experimental.categories.Category;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.ClassPath;

public class PackageSuite extends Suite {

	public PackageSuite(final Class<?> suiteClass, final RunnerBuilder builder) throws InitializationError {
		super(builder, suiteClass, getSuiteClasses(suiteClass));
	}

	public static Class<?>[] getSuiteClasses(final Class<?> suiteClass) throws InitializationError {
		SuitePackages suitePackages = suiteClass.getAnnotation(SuitePackages.class);
		if (suitePackages == null) {
			throw new InitializationError(
					"PackageSuite class '" + suiteClass.getName() + "' must declare @SuitePackages!");
		}
		String[] packages = suitePackages.value();
		if (packages == null || packages.length == 0) {
			throw new InitializationError("PackageSuite class '" + suiteClass.getName()
					+ "' must declare at least one package in its @SuitePackages annotation!");
		}
		Set<Class<?>> allTestClasses = getClassesInPackages(packages);
		allTestClasses.addAll(getExplicitSuiteClasses(suiteClass));
		allTestClasses.addAll(getClassesFromIncludedSuiteClasses(suiteClass));
		applySuiteClassesExcludeFilter(suiteClass, allTestClasses);
		List<Class<?>> testClasses = Lists.newArrayList(allTestClasses);
		Collections.sort(testClasses, (classA, classB) -> classA.getName().compareTo(classB.getName()));
		return testClasses.toArray(new Class<?>[testClasses.size()]);
	}

	public static Set<Class<?>> getClassesInPackages(final String[] packageNames) {
		Set<Class<?>> resultSet = Sets.newHashSet();
		for (String packageName : packageNames) {
			try {
				ClassPath classPath = ClassPath.from(Thread.currentThread().getContextClassLoader());
				Set<Class<?>> topLevelClasses = classPath.getTopLevelClassesRecursive(packageName).stream()
						.map(ci -> ci.load()).collect(Collectors.toSet());
				resultSet.addAll(topLevelClasses);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return resultSet;
	}

	private static Set<Class<?>> getExplicitSuiteClasses(final Class<?> suiteClass) {
		Set<Class<?>> resultSet = Sets.newHashSet();
		SuiteClasses suiteClasses = suiteClass.getAnnotation(SuiteClasses.class);
		if (suiteClasses == null) {
			// annotation not given, abort
			return resultSet;
		}
		Class<?>[] explicitSuiteClasses = suiteClasses.value();
		if (explicitSuiteClasses == null || explicitSuiteClasses.length < 1) {
			// annotation contains no values, abort
			return resultSet;
		}
		for (Class<?> explicitSuiteClass : explicitSuiteClasses) {
			resultSet.add(explicitSuiteClass);
		}
		return resultSet;
	}

	private static Set<Class<?>> getClassesFromIncludedSuiteClasses(final Class<?> suiteClass)
			throws InitializationError {
		Set<Class<?>> resultSet = Sets.newHashSet();
		SuiteIncludes suiteIncludes = suiteClass.getAnnotation(SuiteIncludes.class);
		if (suiteIncludes == null) {
			// annotation not present, abort
			return resultSet;
		}
		Class<?>[] includedSuiteClasses = suiteIncludes.value();
		if (includedSuiteClasses == null || includedSuiteClasses.length < 1) {
			// no value present, abort
			return resultSet;
		}
		for (Class<?> includedSuiteClass : includedSuiteClasses) {
			Class<?>[] suiteClasses = getSuiteClasses(includedSuiteClass);
			Set<Class<?>> suiteClassesSet = Sets.newHashSet(suiteClasses);
			resultSet.addAll(suiteClassesSet);
		}
		return resultSet;
	}

	private static void applySuiteClassesExcludeFilter(final Class<?> suiteClass, final Set<Class<?>> testClasses) {
		// remove all abstract classes and interfaces
		testClasses.removeIf(clazz -> clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers()));
		// remove all classes that have no test methods
		testClasses.removeIf(clazz -> hasTestMethod(clazz) == false);

		// apply the categories filter
		Set<Class<?>> excludedCategories = getExcludedCategories(suiteClass);
		if (excludedCategories.isEmpty()) {
			// no classes excluded
			return;
		}
		Map<Class<?>, Set<Class<?>>> testClassesByCategory = getTestClassesByCategory(testClasses);
		for (Class<?> excludedCategory : excludedCategories) {
			Set<Class<?>> categoryTestClasses = testClassesByCategory.get(excludedCategory);
			if (categoryTestClasses != null && categoryTestClasses.isEmpty() == false) {
				testClasses.removeAll(categoryTestClasses);
			}
		}
	}

	private static Set<Class<?>> getExcludedCategories(final Class<?> suiteClass) {
		Set<Class<?>> resultSet = Sets.newHashSet();
		Set<Class<?>> excludedCategory = getClassesInExcludeCategoryAnnotation(suiteClass);
		if (excludedCategory != null) {
			resultSet.addAll(excludedCategory);
		}
		resultSet.addAll(getClassesInExcludeCategoriesAnnotation(suiteClass));
		return resultSet;
	}

	private static Set<Class<?>> getClassesInExcludeCategoryAnnotation(final Class<?> suiteClass) {
		ExcludeCategory excludeCategory = suiteClass.getAnnotation(ExcludeCategory.class);
		if (excludeCategory == null) {
			// annotation not present, abort
			return null;
		}
		Class<?>[] excludedClasses = excludeCategory.value();
		Set<Class<?>> classes = Sets.newHashSet(excludedClasses);
		return classes;
	}

	private static Set<Class<?>> getClassesInExcludeCategoriesAnnotation(final Class<?> suiteClass) {
		Set<Class<?>> resultSet = Sets.newHashSet();
		ExcludeCategories excludeCategories = suiteClass.getAnnotation(ExcludeCategories.class);
		if (excludeCategories == null) {
			// annotation not present, abort
			return resultSet;
		}
		Class<?>[] excludedCategories = excludeCategories.value();
		if (excludedCategories.length < 1) {
			// no annotation value given, abort
			return resultSet;
		}
		resultSet.addAll(Sets.newHashSet(excludedCategories));
		return resultSet;
	}

	private static Map<Class<?>, Set<Class<?>>> getTestClassesByCategory(final Set<Class<?>> testClasses) {
		Map<Class<?>, Set<Class<?>>> testClassesByCategory = Maps.newHashMap();
		for (Class<?> testClass : testClasses) {
			Category categoryAnnotation = testClass.getAnnotation(Category.class);
			if (categoryAnnotation == null) {
				continue;
			}
			Class<?>[] testCategoryClasses = categoryAnnotation.value();
			for (Class<?> categoryClass : testCategoryClasses) {
				Set<Class<?>> categoryMembers = testClassesByCategory.get(categoryClass);
				if (categoryMembers == null) {
					categoryMembers = Sets.newHashSet();
					testClassesByCategory.put(categoryClass, categoryMembers);
				}
				categoryMembers.add(testClass);
			}
		}
		return testClassesByCategory;
	}

	private static boolean hasTestMethod(final Class<?> maybeTestClass) {
		for (Method method : maybeTestClass.getMethods()) {
			if (method.isAnnotationPresent(Test.class)) {
				return true;
			}
		}
		return false;
	}
}
