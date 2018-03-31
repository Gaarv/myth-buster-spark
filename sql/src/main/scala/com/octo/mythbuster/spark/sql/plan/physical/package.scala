package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.sql.{ ColumnName, Name, Row }

package object physical {

  type RelationName = Name

  type Row = Map[ColumnName, Any]

}
