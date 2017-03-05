package octo.sql.plan.physical.codegen;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.LinkedList;

public abstract class InternalRowCodeGeneratedIterator implements Iterator<InternalRow> {

    protected LinkedList<InternalRow> currentRows = Lists.newLinkedList();

    private Iterator<InternalRow> childRows;

    public InternalRowCodeGeneratedIterator(Iterator<InternalRow> childRows) {
        super();

        this.childRows = childRows;
    }

    public boolean hasNextChildRow() {
        return childRows.hasNext();
    }

    public Iterator<InternalRow> getChildRows() {
        return childRows;
    }

    public boolean addNextChildRowToCurrentRows() {
        boolean hasNext = hasNextChildRow();
        if (hasNext) {
            addToCurrentRow(nextChildRow());
        }
        return hasNext;
    }

    public InternalRow nextChildRow() {
        return childRows.next();
    }

    public boolean hasNext() {
        if (currentRows.isEmpty()) {
            continueProcessing();
        }
        return !currentRows.isEmpty();
    }

    public InternalRow next() {
        return currentRows.remove();
    }

    protected void addToCurrentRow(InternalRow row) {
        currentRows.add(row);
    }

    protected boolean shouldProcessingContinue() {
        return !currentRows.isEmpty();
    }

    protected abstract void continueProcessing();

}
