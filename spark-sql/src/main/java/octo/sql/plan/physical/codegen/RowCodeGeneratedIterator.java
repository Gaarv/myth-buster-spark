package octo.sql.plan.physical.codegen;

import java.util.Iterator;

public abstract class RowCodeGeneratedIterator extends CodeGeneratedIterator<Row> {

    public RowCodeGeneratedIterator(Iterator<InternalRow> childRows) {
        super(childRows);
    }

}
