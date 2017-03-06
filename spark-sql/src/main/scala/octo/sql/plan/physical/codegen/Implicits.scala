package octo.sql.plan.physical.codegen

import scala.collection.JavaConverters._

import octo.sql.{ Row => ScalaRow }
import octo.sql.plan.physical.{ InternalRow => ScalaInternalRow }

import octo.sql.plan.physical.codegen.{ InternalRow => JavaInternalRow, Row => JavaRow }

import scala.collection.{Iterator => ScalaIterator}
import _root_.java.util.{ Iterator => JavaIterator}

object Implicits {

  implicit class ScalaInternalRowImplicits(internalRow: ScalaInternalRow) {

    def wrapForJava: JavaInternalRow = {
      JavaInternalRow.wrap(internalRow.map({ case ((tableName, columnName),value) =>
        (TableNameAndColumnName.of(tableName, columnName), value.asInstanceOf[AnyRef])
      }).asJava)
    }

  }

  implicit class JavaInternalRowImplicits(internalRow: InternalRow) {

    def unwrapForScala: ScalaInternalRow = {
      internalRow.unwrap().asScala.toMap.map({ case (tableNameAndColumnName, value) =>
        ((tableNameAndColumnName.getTableName, tableNameAndColumnName.getColumnName), value)
      })
    }
  }

  implicit class JavaRowImplicits(row: JavaRow) {

    def unwrapForScala: ScalaRow = {
      row.unwrap().asScala.toMap
    }
  }

  implicit class ScalaRowImplicits(scalaRow: ScalaRow) {

    def wrapForJava: JavaRow = {
      JavaRow.wrap(scalaRow.map({ case (columnName, value) =>
        (columnName, value.asInstanceOf[AnyRef])
      }).asJava)
    }

  }

  implicit class ScalaInternalRowIteratorImplicits(iterator: ScalaIterator[ScalaInternalRow]) {

    def wrapForJava: JavaIterator[JavaInternalRow] = iterator.map(_.wrapForJava).asJava

  }

  implicit class JavaInternalRowIteratorImplicits(iterator: JavaIterator[JavaInternalRow]) {

    def unwrapForScala: ScalaIterator[ScalaInternalRow] = iterator.asScala.map(_.unwrapForScala)

  }

  implicit class ScalaRowIteratorImplicits(scalaIterator: ScalaIterator[ScalaRow]) {

    def wrapForJava: JavaIterator[JavaRow] = scalaIterator.map(_.wrapForJava).asJava

  }

  implicit class JavaRowIteratorImplicits(javaIterator: JavaIterator[JavaRow]) {

    def unwrapForScala: ScalaIterator[ScalaRow] = javaIterator.asScala.map(_.unwrapForScala)

  }

}
