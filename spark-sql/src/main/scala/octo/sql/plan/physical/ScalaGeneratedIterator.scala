package octo.sql.plan.physical

import octo.GeneratedIterator

import scala.collection.JavaConverters._

/**
class ScalaGeneratedIterator(generatedIterator: GeneratedIterator) extends Iterator[Row] {
  override def hasNext: Boolean = generatedIterator.hasNext

  override def next(): Row = generatedIterator.next().asScala.map{ case (tableColumn :String, value : AnyRef) =>
    (tableColumn.split('.').head, tableColumn.split('.').last) -> value
  }.toMap[(String, String), AnyRef]
}
  */
