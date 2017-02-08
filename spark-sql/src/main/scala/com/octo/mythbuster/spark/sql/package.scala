package com.octo.mythbuster.spark

package object sql {

  type Name = String

  type ColumnName = Name

  type TableName = Name

  type Row = Map[(TableName, ColumnName), Any]

}
