package com.octo.mythbuster.spark.sql.plan.physical.codegen

import scala.collection.JavaConverters._
import com.octo.mythbuster.spark.sql.plan.physical.{ Row => ScalaRow }
import scala.collection.{ Iterator => ScalaIterator }
import java.util.{ Iterator => JavaIterator }
import wrapper.{ InternalRow => JavaInternalRow }

import com.octo.mythbuster.spark.Implicits._

object Implicits {

  implicit class ScalaRowImplicits(scalaRow: ScalaRow) {

    def wrapForJava(): JavaInternalRow = {
      JavaInternalRow.wrap(scalaRow.map({ case (columnName, value) =>
        (columnName, value.asInstanceOf[AnyRef])
      }).asJava)
    }

  }

  implicit class JavaInternalRowImplicits(internalRow: JavaInternalRow) {

    def unwrapForScala(): ScalaRow = {
      internalRow.unwrap().asScala.toMap
    }
  }

  implicit class ScalaRowIteratorImplicits(iterator: ScalaIterator[ScalaRow]) {

    def wrapForJava(): JavaIterator[JavaInternalRow] = iterator.map(_.wrapForJava()).asJava

  }

  implicit class JavaInternalRowIteratorImplicits(iterator: JavaIterator[JavaInternalRow]) {

    def unwrapForScala(): ScalaIterator[ScalaRow] = iterator.asScala.map(_.unwrapForScala())

  }


}
