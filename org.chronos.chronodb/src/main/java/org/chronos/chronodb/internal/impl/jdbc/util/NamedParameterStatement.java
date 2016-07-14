package org.chronos.chronodb.internal.impl.jdbc.util;

import static com.google.common.base.Preconditions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Maps;

public class NamedParameterStatement implements AutoCloseable {

	// =====================================================================================================================
	// CONSTANTS
	// =====================================================================================================================

	private static final String NAMED_PARAMETER_REGEX = "\\$\\{[a-zA-Z0-9_\\.]+\\}";
	private static final Pattern NAMED_PARAMETER_PATTERN = Pattern.compile(NAMED_PARAMETER_REGEX);

	// =====================================================================================================================
	// FIELDS
	// =====================================================================================================================

	private final Connection connection;
	private final String query;
	private final Map<String, Object> parameterNameToParameterValue;

	private PreparedStatement currentPreparedStatement;

	// =====================================================================================================================
	// CONSTRUCTOR
	// =====================================================================================================================

	public NamedParameterStatement(final Connection connection, final String query) {
		checkNotNull(connection, "Precondition violation - argument 'connection' must not be NULL!");
		checkNotNull(query, "Precondition violation - argument 'query' must not be NULL!");
		this.connection = connection;
		this.query = query;
		this.parameterNameToParameterValue = Maps.newHashMap();
	}

	// =====================================================================================================================
	// PUBLIC API
	// =====================================================================================================================

	public void setParameter(final String parameterName, final Object parameterValue) {
		checkNotNull(parameterName, "Precondition violation - argument 'parameterName' must not be NULL!");
		this.parameterNameToParameterValue.put(parameterName, parameterValue);
	}

	public Object getParameter(final String parameterName) {
		checkNotNull(parameterName, "Precondition violation - argument 'parameterName' must not be NULL!");
		return this.parameterNameToParameterValue.get(parameterName);
	}

	public Map<String, Object> getParameters() {
		return Collections.unmodifiableMap(this.parameterNameToParameterValue);
	}

	public ResultSet executeQuery() throws SQLException {
		Map<Integer, String> parameterIndexToParameterName = this.getVariableOccurrences();
		String preparedStatementSQL = this.getPreparedStatementQuery();
		this.currentPreparedStatement = this.connection.prepareStatement(preparedStatementSQL);
		this.setParameters(parameterIndexToParameterName, this.currentPreparedStatement);
		return this.currentPreparedStatement.executeQuery();
	}

	public int executeUpdate() throws SQLException {
		Map<Integer, String> parameterIndexToParameterName = this.getVariableOccurrences();
		String preparedStatementSQL = this.getPreparedStatementQuery();
		this.currentPreparedStatement = this.connection.prepareStatement(preparedStatementSQL);
		this.setParameters(parameterIndexToParameterName, this.currentPreparedStatement);
		return this.currentPreparedStatement.executeUpdate();
	}

	@Override
	public void close() throws SQLException {
		if (this.currentPreparedStatement != null) {
			this.currentPreparedStatement.close();
			this.currentPreparedStatement = null;
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("NamedParameterStatement[");
		builder.append(this.query.replaceAll("(\\r)?\\n", " "));
		builder.append("]");
		return builder.toString();
	}

	public String toStringWithResolvedParameters() {
		StringBuilder builder = new StringBuilder();
		builder.append("NamedParameterStatement[");
		Map<Integer, String> variableOccurrences = this.getVariableOccurrences();
		String query = this.query;
		for (int i = 0; i < variableOccurrences.size(); i++) {
			String variableName = variableOccurrences.get(i);
			Object variableValue = this.parameterNameToParameterValue.get(variableName);
			String variableValueString = String.valueOf(variableValue);
			if (variableValue == null) {
				variableValueString = "NULL";
			}
			if (variableValue instanceof byte[]) {
				byte[] byteArray = (byte[]) variableValue;
				variableValueString = "BLOB{" + byteArray.length + "}";
			} else if (variableValue instanceof String) {
				variableValueString = "'" + variableValue + "'";
			}
			query = query.replaceFirst(NAMED_PARAMETER_REGEX, variableValueString);
		}
		builder.append(query);
		builder.append("]");
		return builder.toString();
	}

	// =====================================================================================================================
	// INTERNAL HELPER METHODS
	// =====================================================================================================================

	private Map<Integer, String> getVariableOccurrences() {
		Map<Integer, String> parameterIndexToParameterName = Maps.newHashMap();
		Matcher matcher = NAMED_PARAMETER_PATTERN.matcher(this.query);
		int matchIndex = 0;
		while (matcher.find()) {
			String variable = matcher.group();
			// a variable has the syntax '${NAME}', we want only NAME
			variable = variable.substring(2, variable.length() - 1);
			parameterIndexToParameterName.put(matchIndex, variable);
			matchIndex++;
		}
		return parameterIndexToParameterName;
	}

	private String getPreparedStatementQuery() {
		return this.query.replaceAll(NAMED_PARAMETER_REGEX, "?");
	}

	private void setParameters(final Map<Integer, String> indexToName, final PreparedStatement pStmt)
			throws SQLException {
		for (int i = 0; i < indexToName.size(); i++) {
			String parameterName = indexToName.get(i);
			if (this.parameterNameToParameterValue.containsKey(parameterName) == false) {
				throw new IllegalStateException("No value was assigned to parameter '" + parameterName + "'!");
			}
			Object parameterValue = this.parameterNameToParameterValue.get(parameterName);
			pStmt.setObject(i + 1, parameterValue);
		}
	}

}
