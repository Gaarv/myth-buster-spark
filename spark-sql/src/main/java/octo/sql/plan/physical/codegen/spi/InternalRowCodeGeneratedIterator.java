package octo.sql.plan.physical.codegen.spi;

import java.util.Iterator;

public abstract class InternalRowCodeGeneratedIterator extends CodeGeneratedIterator<InternalRow> {

    public InternalRowCodeGeneratedIterator(Iterator<InternalRow> childRows) {
        super(childRows);
    }

}
