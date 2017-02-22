package com.octo.mythbuster.spark.sql.catalyst.plans.physical

import com.octo.mythbuster.spark.sql.Row
import com.octo.nad.GeneratedIterator

import scala.collection.JavaConverters._

/**
  * Created by marc on 20/02/2017.
  */
class ScalaGeneratedIterator(generatedIterator: GeneratedIterator) extends Iterator[Row] {
  override def hasNext: Boolean = generatedIterator.hasNext

  override def next(): Row = generatedIterator.next().asScala.map{ case (tableColumn :String, value : AnyRef) =>
    (tableColumn.split('.').head, tableColumn.split('.').last) -> value
  }.toMap[(String, String), AnyRef]
}
