package com.octo.mythbuster.spark

package object sql {

  type Name = String

  type ExpressionName = Name

  type RelationName = Name

  type QualifierName = Name

  type ColumnName = Name

  type Row = Map[ColumnName, Any]

}
