package org.chronos.chronograph.api.builder.query;

import org.apache.tinkerpop.gremlin.structure.Element;

public interface GraphWhereBuilder<E extends Element> {

	public GraphFinalizableQueryBuilder<E> contains(String text);

	public GraphFinalizableQueryBuilder<E> containsIgnoreCase(String text);

	public GraphFinalizableQueryBuilder<E> notContains(String text);

	public GraphFinalizableQueryBuilder<E> notContainsIgnoreCase(String text);

	public GraphFinalizableQueryBuilder<E> startsWith(String text);

	public GraphFinalizableQueryBuilder<E> startsWithIgnoreCase(String text);

	public GraphFinalizableQueryBuilder<E> notStartsWith(String text);

	public GraphFinalizableQueryBuilder<E> notStartsWithIgnoreCase(String text);

	public GraphFinalizableQueryBuilder<E> endsWith(String text);

	public GraphFinalizableQueryBuilder<E> endsWithIgnoreCase(String text);

	public GraphFinalizableQueryBuilder<E> notEndsWith(String text);

	public GraphFinalizableQueryBuilder<E> notEndsWithIgnoreCase(String text);

	public GraphFinalizableQueryBuilder<E> matchesRegex(String regex);

	public GraphFinalizableQueryBuilder<E> notMatchesRegex(String regex);

	public GraphFinalizableQueryBuilder<E> isEqualTo(String value);

	public GraphFinalizableQueryBuilder<E> isEqualToIgnoreCase(String value);

	public GraphFinalizableQueryBuilder<E> isNotEqualTo(String value);

	public GraphFinalizableQueryBuilder<E> isNotEqualToIgnoreCase(String value);
}
