package octo.sql.codegen;

import com.google.common.collect.Lists;
import scala.collection.Iterator;

import java.util.LinkedList;

public abstract class AbstractCodeGeneratedIterator<O> implements Iterator<O> {

    protected LinkedList<O> currentRows = Lists.newLinkedList();

    protected Iterator<PhysicalRow> childRows;

    public boolean hasNext() {
        if (currentRows.isEmpty()) {
            continueProcessing();
        }
        return !currentRows.isEmpty();
    }

    public O next() {
        return currentRows.remove();
    }

    protected void appendToCurrentRows(O row) {
        currentRows.add(row);
    }

    protected boolean shouldProcessingContinue() {
        return !currentRows.isEmpty();
    }

    protected abstract void continueProcessing();

}
