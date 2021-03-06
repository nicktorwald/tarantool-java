package org.tarantool.jdbc;

import org.tarantool.SqlProtoUtils;
import org.tarantool.util.SQLStates;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientException;
import java.sql.Types;
import java.util.List;

public class SQLResultSetMetaData implements ResultSetMetaData {

    private final List<SqlProtoUtils.SQLMetaData> sqlMetadata;

    public SQLResultSetMetaData(List<SqlProtoUtils.SQLMetaData> sqlMetaData) {
        this.sqlMetadata = sqlMetaData;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return sqlMetadata.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getName();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        return sqlMetadata.get(column - 1).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getScale(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return Types.OTHER;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return "scalar";
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        if (isWrapperFor(type)) {
            return type.cast(this);
        }
        throw new SQLNonTransientException("ResultSetMetadata does not wrap " + type.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return type.isAssignableFrom(this.getClass());
    }

    void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > getColumnCount()) {
            throw new SQLNonTransientException(
                    String.format("Column index %d is out of range. Max index is %d", columnIndex, getColumnCount()),
                    SQLStates.INVALID_PARAMETER_VALUE.getSqlState()
            );
        }
    }

    @Override
    public String toString() {
        return "SQLResultSetMetaData{" +
                "sqlMetadata=" + sqlMetadata +
                '}';
    }
}
