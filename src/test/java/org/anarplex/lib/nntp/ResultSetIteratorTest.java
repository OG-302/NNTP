package org.anarplex.lib.nntp;

import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

class ResultSetIteratorTest {

    @Test
    void hasNext_and_iteration_over_multiple_rows_then_statement_closed() throws Exception {
        // Arrange: in-memory HSQLDB with two rows
        try (Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:iterdb1;shutdown=true", "sa", "");
             Statement st = conn.createStatement()) {
            st.execute("create table t(id int primary key, name varchar(50))");
            st.execute("insert into t(id, name) values (1, 'alpha')");
            st.execute("insert into t(id, name) values (2, 'beta')");

            ResultSet rs = st.executeQuery("select id, name from t order by id");

            ResultSetIterator<String> it = new ResultSetIterator<>(rs, r -> {
                try {
                    return r.getInt("id") + ":" + r.getString("name");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            // Act + Assert: iterate two elements, then no more
            assertTrue(it.hasNext());
            assertEquals("1:alpha", it.next());

            assertTrue(it.hasNext());
            assertEquals("2:beta", it.next());

            // Exhausted now
            assertFalse(it.hasNext());

            // The underlying Statement should be closed by the iterator when exhausted
            assertTrue(st.isClosed(), "Statement should be closed after iterator exhaustion");
        }
    }

    @Test
    void empty_result_set_reports_no_elements_and_closes_statement_on_first_hasNext() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:hsqldb:mem:iterdb2;shutdown=true", "sa", "");
             Statement st = conn.createStatement()) {
            st.execute("create table t(id int primary key, name varchar(50))");

            ResultSet rs = st.executeQuery("select id, name from t order by id");

            ResultSetIterator<String> it = new ResultSetIterator<>(rs, r -> "should-not-be-called");

            assertFalse(it.hasNext());
            assertNull(it.next());

            // Should already be closed when hasNext() returned false
            assertTrue(st.isClosed(), "Statement should be closed when no rows are present");
        }
    }

}