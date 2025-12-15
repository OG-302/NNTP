package org.anarplex.lib.nntp.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * An Iterator wrapper for a JDBC ResultSet, includes a mapper function to convert a ResultSet row
 * and is an AutoCloseable resource.
 * This implementation correctly handles hasNext() without using isAfterLast() or isLast() (which
 * Apache dbUtils does).  Specifically:
 * 1. Never uses isAfterLast() — correct per JDBC spec.
 * 2. Safe even if ResultSet is forward-only (most common) by not relying on isLast(), etc.
 * 3. Follows Iterator contract strictly.
 * When this class is closed, the underlying ResultSet AND its parent SQL Statement object is closed as well.
 *
 * @param <T> the type of objects produced by mapping the ResultSet rows
 */
public class ResultSetIterator<T> implements Iterator<T>, AutoCloseable {

    private final ResultSet resultSet;
    private final ResultSetMapper<T> mapper;
    private boolean hasNext;
    private T nextElement;

    /**
     * Constructs an iterator from a ResultSet and a mapper function.
     * @param resultSet the JDBC ResultSet (should not be null)
     * @param mapper maps a ResultSet row to an object of type T
     * @throws SQLException if a database access error occurs
     */
    public ResultSetIterator(ResultSet resultSet, ResultSetMapper<T> mapper) throws SQLException {
        this.resultSet = resultSet;
        this.mapper = mapper;
        fetchNext();    // Prime the first element
    }

    @Override
    public boolean hasNext() {
        if (!hasNext) {
            close();
        }
        return hasNext;
    }

    /**
     * Returns the next element in the iteration.
     * If there are no more elements null is returned.
     * @return the next element in the iteration or null if there are no more elements
     */
    @Override
    public T next() {
        if (!hasNext) {
            return null;
        }
        T result = nextElement;
        fetchNext(); // Prepare the next element
        return result;
    }

    @Override
    public void close() {
        try {
            // resultSet.close();
            if (resultSet.getStatement() != null) {
                resultSet.getStatement().close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close ResultSet", e);
        }
    }

    /**
     * Mapper interface to convert a ResultSet row into an object.
     * @param <T> the type of the result
     */
    @FunctionalInterface
    public interface ResultSetMapper<T> {
        T map(ResultSet rs);
    }

    /**
     * Attempts to fetch the next element and stores it.
     * Updates hasNext and nextElement accordingly.
     */
    private void fetchNext()  {
        try {
            if (resultSet.next()) {
                nextElement = mapper.map(resultSet);
                hasNext = true;
                return;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        nextElement = null;
        hasNext = false;
    }
}
