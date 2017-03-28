package com.octo.mythbuster.spark.sql.plan

import com.google.common.io.Resources
import com.octo.mythbuster.spark.sql._
import com.octo.mythbuster.spark.sql.{expression => e}
import com.octo.mythbuster.spark.sql.plan.{logical => l, physical => p}

import scala.util.{Failure, Success, Try}

// The query planner transforms a logical plan into a physical plan
object QueryPlanner {

  /*
    That's were we choose the actual source of the data
      l.Projection -> p.Projection
      l.CartesianProduct -> p.CartesianProduct
      l.Filter -> p.Filter
      l.TableScan -> Either CSVFileFullScan or IterableFullScan
   */

  protected def doPlanQuery(logicalPlan: l.LogicalPlan)(implicit rowIterableRegistry: RowIterableRegistry): p.PhysicalPlan = logicalPlan match {
    case l.Projection(child, expressions) => p.Projection(doPlanQuery(child), expressions)
    case l.CartesianProduct(leftChild, rightChild) => p.CartesianProduct(doPlanQuery(leftChild), doPlanQuery(rightChild))

    case l.Filter(child, expression: e.Expression) => p.Filter(doPlanQuery(child), expression)

    case l.TableScan(tableName: l.TableName, aliasName: Option[RelationName]) => {
      val qualifierName = aliasName.getOrElse(tableName)
      rowIterableRegistry.getRowIterableByName(tableName) match {
        case Success(iterable) => p.IterableFullScan(qualifierName, iterable)
        case Failure(e) => {
          Option(Resources.getResource(s"${tableName}.csv")) match {
            case Some(csvFileURL) => p.CSVFileFullScan(qualifierName, csvFileURL)
            case None => throw new IllegalArgumentException(s"Unable to find any table matching ${tableName}")
          }
        }
      }
    }
    case _ => throw new IllegalArgumentException(s"Unable to plan query because ${logicalPlan} needs to be a projection")
  }

  def planQuery(logicalPlan: l.LogicalPlan)(implicit rowIterableRegistry: RowIterableRegistry): Try[p.PhysicalPlan] = Try {
    doPlanQuery(logicalPlan)
  }


}
