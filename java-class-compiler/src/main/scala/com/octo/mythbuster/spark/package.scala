package com.octo.mythbuster

package object spark {

  type Name = String

  type ColumnName = Name

  type TableName = Name

  type Row = Map[(TableName, ColumnName), Any]

}
