
 public Object generate(Object[] references) {
   return new GeneratedIterator(references);
 }

 final class GeneratedIterator extends org.apache.spark.sql.execution.BufferedRowIterator {
   private Object[] references;
   private scala.collection.Iterator[] inputs;
   private scala.collection.Iterator inputadapter_input;
   private org.apache.spark.broadcast.TorrentBroadcast bhj_broadcast;
   private org.apache.spark.sql.execution.joins.LongHashedRelation bhj_relation;
   private org.apache.spark.sql.execution.metric.SQLMetric bhj_numOutputRows;
   private UnsafeRow bhj_result;
   private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder bhj_holder;
   private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter bhj_rowWriter;

   public GeneratedIterator(Object[] references) {
     this.references = references;
   }

   public void init(int index, scala.collection.Iterator[] inputs) {
     partitionIndex = index;
     this.inputs = inputs;
     inputadapter_input = inputs[0];
     this.bhj_broadcast = (org.apache.spark.broadcast.TorrentBroadcast) references[0];

     bhj_relation = ((org.apache.spark.sql.execution.joins.LongHashedRelation) bhj_broadcast.value()).asReadOnlyCopy();
     incPeakExecutionMemory(bhj_relation.estimatedSize());

     this.bhj_numOutputRows = (org.apache.spark.sql.execution.metric.SQLMetric) references[1];
     bhj_result = new UnsafeRow(6);
     this.bhj_holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(bhj_result, 64);
     this.bhj_rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(bhj_holder, 6);

   }

   protected void processNext() throws java.io.IOException {
     while (inputadapter_input.hasNext()) {
       InternalRow inputadapter_row = (InternalRow) inputadapter_input.next();
       int inputadapter_value3 = inputadapter_row.getInt(3);

       // generate join key for stream side

       boolean bhj_isNull = false;
       long bhj_value = -1L;
       if (!false) {
         bhj_value = (long) inputadapter_value3;
       }
       // find matches from HashedRelation
       UnsafeRow bhj_matched = bhj_isNull ? null: (UnsafeRow)bhj_relation.getValue(bhj_value);
       if (bhj_matched == null) continue;

       bhj_numOutputRows.add(1);

       int inputadapter_value = inputadapter_row.getInt(0);
       boolean inputadapter_isNull1 = inputadapter_row.isNullAt(1);
       UTF8String inputadapter_value1 = inputadapter_isNull1 ? null : (inputadapter_row.getUTF8String(1));
       double inputadapter_value2 = inputadapter_row.getDouble(2);
       int bhj_value2 = bhj_matched.getInt(0);
       boolean bhj_isNull3 = bhj_matched.isNullAt(1);
       UTF8String bhj_value3 = bhj_isNull3 ? null : (bhj_matched.getUTF8String(1));
       bhj_holder.reset();

       bhj_rowWriter.zeroOutNullBytes();

       bhj_rowWriter.write(0, inputadapter_value);

       if (inputadapter_isNull1) {
         bhj_rowWriter.setNullAt(1);
       } else {
         bhj_rowWriter.write(1, inputadapter_value1);
       }

       bhj_rowWriter.write(2, inputadapter_value2);

       bhj_rowWriter.write(3, inputadapter_value3);

       bhj_rowWriter.write(4, bhj_value2);

       if (bhj_isNull3) {
         bhj_rowWriter.setNullAt(5);
       } else {
         bhj_rowWriter.write(5, bhj_value3);
       }
       bhj_result.setTotalSize(bhj_holder.totalSize());
       append(bhj_result);
       if (shouldStop()) return;
     }
   }
 }