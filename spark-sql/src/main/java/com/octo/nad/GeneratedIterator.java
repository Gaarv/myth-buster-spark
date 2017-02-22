package com.octo.nad;

import com.octo.mythbuster.spark.sql.*;
import com.octo.mythbuster.spark.sql.catalyst.parser.TableColumn;
import scala.collection.Iterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


/**
 * Created by marc on 14/02/2017.
 */
public abstract class GeneratedIterator {
    protected LinkedList<Map<String, Object>> currentRows = new LinkedList<>();

    protected Iterator<Map<String, Object>> input;

    public boolean hasNext() throws IOException {
        if (currentRows.isEmpty()) {
            processNext();
        }
        return !currentRows.isEmpty();
    }

    public Map<String, Object> next() {
        return currentRows.remove();
    }

    /**
     * Initializes from array of iterators of InternalRow.
     */
    public void init(Iterator<Map<String, Object>> iter) {
        this.input = iter;
    }

    /**
     * Append a row to currentRows.
     */
    protected void append(Map<String, Object> row) {
        currentRows.add(row);
    }

    /**
     * Returns whether `processNext()` should stop processing next row from `input` or not.
     *
     * If it returns true, the caller should exit the loop (return from processNext()).
     */
    protected boolean shouldStop() {
        return !currentRows.isEmpty();
    }

    /**
     * Processes the input until have a row as output (currentRow).
     *
     * After it's called, if currentRow is still null, it means no more rows left.
     */
    protected abstract void processNext() throws IOException;
}
