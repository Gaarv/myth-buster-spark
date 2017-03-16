package octo.sql.plan.physical.codegen

import octo.Implicits._
import scala.collection.JavaConverters._

import scala.collection.{ Iterator => ScalaIterator }
import java.util.{ Iterator => JavaIterator }

import octo.sql.plan.physical.{ InternalRow => ScalaInternalRow }
import octo.sql.plan.physical.codegen.spi.{TableNameAndColumnName, InternalRow => JavaInternalRow}

object Implicits {

  implicit class ScalaInternalRowImplicits(internalRow: ScalaInternalRow) {

    def wrapForJava: JavaInternalRow = {
      JavaInternalRow.wrap(internalRow.map({ case ((tableName, columnName),value) =>
        (TableNameAndColumnName.of(tableName.asJava(), columnName), value.asInstanceOf[AnyRef])
      }).asJava)
    }

  }

  implicit class JavaInternalRowImplicits(internalRow: JavaInternalRow) {

    def unwrapForScala: ScalaInternalRow = {
      internalRow.unwrap().asScala.toMap.map({ case (tableNameAndColumnName, value) =>
        ((tableNameAndColumnName.getTableName.asScala, tableNameAndColumnName.getColumnName), value)
      })
    }
  }

  implicit class ScalaInternalRowIteratorImplicits(iterator: ScalaIterator[ScalaInternalRow]) {

    def wrapForJava: JavaIterator[JavaInternalRow] = iterator.map(_.wrapForJava).asJava

  }

  implicit class JavaInternalRowIteratorImplicits(iterator: JavaIterator[JavaInternalRow]) {

    def unwrapForScala: ScalaIterator[ScalaInternalRow] = iterator.asScala.map(_.unwrapForScala)

  }


}
