package org.tarantool.jdbc;

import org.tarantool.util.JdbcConstants;
import org.tarantool.util.SQLStates;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tarantool {@link Statement} implementation.
 * <p>
 * Supports {@link ResultSet#TYPE_FORWARD_ONLY} and {@link ResultSet#TYPE_SCROLL_INSENSITIVE}
 * types of cursors.
 * Supports only {@link ResultSet#HOLD_CURSORS_OVER_COMMIT} holdability type.
 */
public class SQLStatement implements TarantoolStatement {

    protected final SQLConnection connection;

    /**
     * Current result set / update count associated to this statement.
     */
    protected SQLResultSet resultSet;
    protected int updateCount;

    private boolean isCloseOnCompletion;

    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    private int maxRows;

    /**
     * Query timeout in millis.
     */
    private long timeout;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    protected SQLStatement(SQLConnection sqlConnection) throws SQLException {
        this.connection = sqlConnection;
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.resultSetHoldability = sqlConnection.getHoldability();
    }

    protected SQLStatement(SQLConnection sqlConnection,
                           int resultSetType,
                           int resultSetConcurrency,
                           int resultSetHoldability) throws SQLException {
        this.connection = sqlConnection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkNotClosed();
        if (!executeInternal(sql)) {
            throw new SQLException("No results were returned", SQLStates.NO_DATA.getSqlState());
        }
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkNotClosed();
        if (executeInternal(sql)) {
            throw new SQLException(
                "Result was returned but nothing was expected",
                SQLStates.TOO_MANY_RESULTS.getSqlState()
            );
        }
        return updateCount;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        JdbcConstants.checkGeneratedKeysConstant(autoGeneratedKeys);
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException();
        }
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void close() throws SQLException {
        if (isClosed.compareAndSet(false, true)) {
            cancel();
            discardLastResults();
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkNotClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int maxRows) throws SQLException {
        checkNotClosed();
        if (maxRows < 0) {
            throw new SQLNonTransientException("Max rows parameter can't be a negative value");
        }
        this.maxRows = maxRows;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return (int) TimeUnit.MILLISECONDS.toSeconds(timeout);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLNonTransientException(
                "Query timeout must be positive or zero",
                SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
            );
        }
        timeout = TimeUnit.SECONDS.toMillis(seconds);
    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkNotClosed();
        return executeInternal(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        JdbcConstants.checkGeneratedKeysConstant(autoGeneratedKeys);
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException();
        }
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkNotClosed();
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkNotClosed();
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkNotClosed();
        JdbcConstants.checkCurrentResultConstant(current);
        if (resultSet != null &&
            (current == KEEP_CURRENT_RESULT || current == CLOSE_ALL_RESULTS)) {
            throw new SQLFeatureNotSupportedException();
        }

        // the driver doesn't support multiple results
        // close current result and return no-more-results flag
        discardLastResults();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkNotClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkNotClosed();
        // no-op
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkNotClosed();
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkNotClosed();
        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkNotClosed();
        return new SQLResultSet(SQLResultHolder.ofEmptyQuery(), this);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkNotClosed();
        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed.get() || connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * {@inheritDoc}
     * <p>
     * <strong>Impl Note:</strong> this method doesn't affect
     * execution methods which close the last result set implicitly.
     * It is applied only when {@link ResultSet#close()} is invoked
     * explicitly by the app.
     *
     * @throws SQLException if this method is called on a closed
     * {@code Statement}
     */
    @Override
    public void closeOnCompletion() throws SQLException {
        checkNotClosed();
        isCloseOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkNotClosed();
        return isCloseOnCompletion;
    }

    @Override
    public void checkCompletion() throws SQLException {
        if (isCloseOnCompletion &&
            resultSet != null &&
            resultSet.isClosed()) {
            close();
        }
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("SQLStatement does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    /**
     * Clears the results of the most recent execution.
     */
    protected void discardLastResults() throws SQLException {
        final SQLResultSet lastResultSet = resultSet;

        clearWarnings();
        updateCount = -1;
        resultSet = null;

        if (lastResultSet != null) {
            try {
                lastResultSet.close();
            } catch (Exception ignored) {
                // No-op.
            }
        }
    }

    /**
     * Performs query execution.
     *
     * @param sql    query
     * @param params optional params
     *
     * @return {@code true}, if the result is a ResultSet object;
     */
    protected boolean executeInternal(String sql, Object... params) throws SQLException {
        discardLastResults();
        SQLResultHolder holder;
        try {
            holder = connection.execute(timeout, sql, params);
        } catch (StatementTimeoutException e) {
            cancel();
            throw new SQLTimeoutException();
        }

        if (holder.isQueryResult()) {
            resultSet = new SQLResultSet(holder, this);
        }
        updateCount = holder.getUpdateCount();
        return holder.isQueryResult();
    }

    @Override
    public ResultSet executeMetadata(SQLResultHolder data) throws SQLException {
        checkNotClosed();
        return createResultSet(data);
    }

    protected SQLResultSet createResultSet(SQLResultHolder holder) throws SQLException {
        return new SQLResultSet(holder, this);
    }

    protected void checkNotClosed() throws SQLException {
        if (isClosed()) {
            throw new SQLNonTransientException("Statement is closed.");
        }
    }

}
