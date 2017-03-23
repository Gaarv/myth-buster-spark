package com.octo.mythbuster.spark.sql.plan.physical.codegen.wrapper;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.LinkedList;

public abstract class CodeGeneratedInternalRowIterator implements Iterator<InternalRow> {

    protected LinkedList<InternalRow> currentRows = Lists.newLinkedList();

    private Iterator<InternalRow> childRows;

    public CodeGeneratedInternalRowIterator(Iterator<InternalRow> childRows) {
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

    protected void addToCurrentRow(InternalRow row) {
        currentRows.add(row);
    }

    protected boolean shouldContinue() {
        return !currentRows.isEmpty();
    }

    protected abstract void doContinue();

    public LinkedList<InternalRow> getCurrentRows() {
        return currentRows;
    }

    public boolean hasNext() {
        if (currentRows.isEmpty()) {
            doContinue();
        }
        return !currentRows.isEmpty();
    }

    public InternalRow next() {
        return currentRows.remove();
    }


}
