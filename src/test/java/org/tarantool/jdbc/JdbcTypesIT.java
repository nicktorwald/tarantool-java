package org.tarantool.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.tarantool.TestAssumptions.assumeMinimalServerVersion;
import static org.tarantool.TestUtils.fromHex;

import org.tarantool.ServerVersion;
import org.tarantool.TarantoolTestHelper;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

class JdbcTypesIT {

    public static Integer[] INT_VALS = new Integer[] { Integer.MIN_VALUE, 0, Integer.MAX_VALUE, 1, 100, -50 };
    public static Double[] DOUBLE_VALS = new Double[] { 0.0d, Double.MIN_VALUE, Double.MAX_VALUE, 1.00001d, 100.5d };
    public static Float[] FLOAT_VALS = new Float[] { 0.0f, Float.MIN_VALUE, Float.MAX_VALUE, 1.00001f, 100.5f };
    public static String[] STRING_VALS = new String[] { "", "1", "A", "test test" };
    public static Byte[] BYTE_VALS = new Byte[] { Byte.MIN_VALUE, Byte.MAX_VALUE, (byte)0, (byte)100 };
    public static Short[] SHORT_VALS = new Short[] { Short.MIN_VALUE, Short.MAX_VALUE, (short)0, (short)1000 };
    public static Long[] LONG_VALS = new Long[] { Long.MIN_VALUE, Long.MAX_VALUE, 0L, 100000000L};
    public static BigDecimal[] BIGDEC_VALS = new BigDecimal[] {
        BigDecimal.valueOf(Double.MIN_VALUE), BigDecimal.valueOf(Double.MAX_VALUE), BigDecimal.ZERO, BigDecimal.ONE
    };
    public static byte[][] BINARY_VALS = new byte[][] {
        fromHex(""), fromHex("00"), fromHex("FFFF"), fromHex("0102030405060708")
    };
    public static Date[] DATE_VALS = new Date[] { Date.valueOf("1983-03-14"), new Date(129479994) };

    private static final String[] INIT_SQL = new String[] {
        getCreateTableSQL()
    };

    private static final String[] CLEAN_SQL = new String[] {
        "DROP TABLE IF EXISTS test_types"
    };

    private static TarantoolTestHelper testHelper;
    private static Connection connection;

    @BeforeAll
    static void setupEnv() throws SQLException {
        testHelper = new TarantoolTestHelper("jdbc-types-it");
        testHelper.createInstance();
        testHelper.startInstance();

        connection = DriverManager.getConnection(SqlTestUtils.makeDefaultJdbcUrl());
    }

    @AfterAll
    static void teardownEnv() throws SQLException {
        if (connection != null) {
            connection.close();
        }
        testHelper.stopInstance();
    }

    @BeforeEach
    void setUpTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(INIT_SQL);
    }

    @AfterEach
    void tearDownTest() throws SQLException {
        assumeMinimalServerVersion(testHelper.getInstanceVersion(), ServerVersion.V_2_1);
        testHelper.executeSql(CLEAN_SQL);
    }

    @Test
    void testSetByte() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Byte.class)
            .setColumns(TarantoolSqlType.INT, TarantoolSqlType.INTEGER)
            .setValues(BYTE_VALS)
            .testTypes();
    }

    @Test
    void testSetShort() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Short.class)
            .setColumns(TarantoolSqlType.INT, TarantoolSqlType.INTEGER)
            .setValues(SHORT_VALS)
            .testTypes();
    }

    @Test
    void testSetInt() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Integer.class)
            .setColumns(TarantoolSqlType.INT, TarantoolSqlType.INTEGER)
            .setValues(INT_VALS)
            .testTypes();
    }

    @Test
    void testSetLong() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Long.class)
            .setColumns(TarantoolSqlType.INT, TarantoolSqlType.INTEGER)
            .setValues(LONG_VALS)
            .testTypes();
    }

    @Test
    void testSetString() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", String.class)
            .setColumns(TarantoolSqlType.VARCHAR, TarantoolSqlType.TEXT)
            .setValues(STRING_VALS)
            .testTypes();
    }

    @Test
    void testSetFloat() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Float.class)
            .setColumns(TarantoolSqlType.REAL)
            .setValues(FLOAT_VALS)
            .testTypes();
    }

    @Test
    void testSetDouble() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Double.class)
            .setColumns(TarantoolSqlType.FLOAT, TarantoolSqlType.DOUBLE)
            .setValues(DOUBLE_VALS)
            .testTypes();
    }

    @Test
    void testSetBigDecimal() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", BigDecimal.class)
            .setColumns(TarantoolSqlType.REAL, TarantoolSqlType.FLOAT, TarantoolSqlType.DOUBLE)
            .setValues(BIGDEC_VALS)
            .testTypes();
    }

    @Test
    void testSetByteArray() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", byte[].class)
            .setColumns(TarantoolSqlType.SCALAR)
            .setValues(BINARY_VALS)
            .testTypes();
    }

    @Test
    void testSetDate() throws SQLException {
        new TarantoolTestTypeHelper<>(connection, "test_types", Date.class)
            .setColumns(TarantoolSqlType.INT, TarantoolSqlType.INTEGER)
            .setValues(DATE_VALS)
            .testTypes();
    }

    private static String getCreateTableSQL() {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append("test_types");
        sb.append("(KEY INT PRIMARY KEY");
        for (TarantoolSqlType tntType : TarantoolSqlType.values()) {
            sb.append(", F");
            sb.append(tntType.ordinal());
            sb.append(" ");
            sb.append(tntType.sqlType);
        }
        sb.append(")");
        return sb.toString();
    }

    private static class TarantoolTestTypeHelper<T> {

        private Class<T> cls;
        private T[] vals;
        private TarantoolSqlType[] colTypes;

        private Connection connection;

        TarantoolTestTypeHelper(Connection connection, String tableName, Class<T> cls) {
            this.connection = connection;
            this.cls = cls;
        }

        TarantoolTestTypeHelper<T> setValues(T... vals) {
            this.vals = vals;
            return this;
        }

        TarantoolTestTypeHelper<T> setColumns(TarantoolSqlType... colTypes) {
            this.colTypes = colTypes;
            return this;
        }

        void testTypes() throws SQLException {
            checkSetParameter();
            checkResultSetGet();
        }

        private void checkSetParameter() throws SQLException {
            String sql = getParameterizedInsertSQL();
            try (PreparedStatement prep = connection.prepareStatement(sql)) {
                assertNotNull(prep);
                int count = 0;
                for (T val : vals) {
                    prep.setInt(1, count++);
                    for (int col = 0; col < colTypes.length; col++) {
                        apply(prep, 2 + col, val);
                    }
                    assertEquals(1, prep.executeUpdate());
                }
            } catch (Throwable e) {
                throw new SQLException(e);
            }
        }

        private String getParameterizedInsertSQL() {
            StringBuilder sb = new StringBuilder("INSERT INTO ");
            sb.append("test_types");
            sb.append("(KEY");
            for (TarantoolSqlType tntType : colTypes) {
                sb.append(", F");
                sb.append(tntType.ordinal());
            }
            sb.append(") VALUES (");
            sb.append("?"); // KEY value
            for (TarantoolSqlType ignored : colTypes) {
                sb.append(", ?");
            }
            sb.append(")");
            return sb.toString();
        }

        private void checkResultSetGet() throws SQLException {
            try (Statement stmt = connection.createStatement()) {
                String sql = getSelectSQL();
                ResultSet rs = stmt.executeQuery(sql);
                assertNotNull(rs);
                try {
                    for (int row = 0; row < vals.length; row++) {
                        assertTrue(rs.next());
                        int valIdx = rs.getInt(1);
                        T val = vals[valIdx];
                        for (int col = 0; col < colTypes.length; col++) {
                            TarantoolSqlType tntSqlType = colTypes[col];
                            check(rs, 2 + col, "F" + tntSqlType.ordinal(), val);
                        }
                    }
                } finally {
                    rs.close();
                }
            }
        }

        private String getSelectSQL() {
            StringBuilder sb = new StringBuilder("SELECT KEY");
            for (TarantoolSqlType tntType : colTypes) {
                sb.append(", F");
                sb.append(tntType.ordinal());
            }
            sb.append(" FROM ");
            sb.append("test_types");
            return sb.toString();
        }

        protected void apply(PreparedStatement ps, int col, T val) throws SQLException {
            if (cls == Byte.class) {
                ps.setByte(col, (Byte)val);
            } else if (cls == Short.class) {
                ps.setShort(col, (Short)val);
            } else if (cls == Integer.class) {
                ps.setInt(col, (Integer)val);
            } else if (cls == Long.class) {
                ps.setLong(col, (Long)val);
            } else if (cls == String.class) {
                ps.setString(col, (String)val);
            } else if (cls == Float.class) {
                ps.setFloat(col, (Float)val);
            } else if (cls == Double.class) {
                ps.setDouble(col, (Double)val);
            } else if (cls == Boolean.class) {
                ps.setBoolean(col, (Boolean)val);
            } else if (cls == BigDecimal.class) {
                ps.setBigDecimal(col, (BigDecimal) val);
            } else if (cls == byte[].class) {
                ps.setBytes(col, (byte[])val);
            } else if (cls == Date.class) {
                ps.setDate(col, (Date)val);
            } else if (cls == Time.class) {
                ps.setTime(col, (Time)val);
            } else if (cls == Timestamp.class) {
                ps.setTimestamp(col, (Timestamp)val);
            } else {
                throw new IllegalArgumentException("val is of unexpected type " + cls.getName());
            }
        }

        protected void check(ResultSet rs, int col, String name, T val) throws SQLException {
            if (cls == Byte.class) {
                assertEquals(val, rs.getByte(col));
                assertEquals(val, rs.getByte(name));
            } else if (cls == Short.class) {
                assertEquals(val, rs.getShort(col));
                assertEquals(val, rs.getShort(name));
            } else if (cls == Integer.class) {
                assertEquals(val, rs.getInt(col));
                assertEquals(val, rs.getInt(name));
            } else if (cls == Long.class) {
                assertEquals(val, rs.getLong(col));
                assertEquals(val, rs.getLong(name));
            } else if (cls == String.class) {
                assertEquals(val, rs.getString(col));
                assertEquals(val, rs.getString(name));
            } else if (cls == Float.class) {
                assertEquals((Float)val, rs.getFloat(col), Math.ulp(1.0f));
                assertEquals((Float)val, rs.getFloat(name), Math.ulp(1.0f));
            } else if (cls == Double.class) {
                assertEquals((Double)val, rs.getDouble(col), Math.ulp(1.0d));
                assertEquals((Double)val, rs.getDouble(name), Math.ulp(1.0d));
            } else if (cls == Boolean.class) {
                assertEquals(val, rs.getBoolean(col));
                assertEquals(val, rs.getBoolean(name));
            } else if (cls == BigDecimal.class) {
                assertEquals(0, ((BigDecimal)val).compareTo(rs.getBigDecimal(col)));
                assertEquals(0, ((BigDecimal)val).compareTo(rs.getBigDecimal(name)));
            } else if (cls == byte[].class) {
                assertArrayEquals((byte[])val, rs.getBytes(col));
                assertArrayEquals((byte[])val, rs.getBytes(name));
            } else if (cls == Date.class) {
                assertEquals(val, rs.getDate(col));
                assertEquals(val, rs.getDate(name));
            } else if (cls == Time.class) {
                assertEquals(val, rs.getTime(col));
                assertEquals(val, rs.getTime(name));
            } else if (cls == Timestamp.class) {
                assertEquals(val, rs.getTimestamp(col));
                assertEquals(val, rs.getTimestamp(name));
            } else {
                throw new IllegalArgumentException("val is of unexpected type " + cls.getName());
            }
        }
    }

}
