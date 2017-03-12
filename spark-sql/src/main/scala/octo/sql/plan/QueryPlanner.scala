package octo.sql.plan

import octo.Implicits._
import octo.sql.{expression => e}
import octo.sql.{Row, TableName, TableRegistry, _}
import octo.sql.expression.TableColumn
import octo.sql.plan.physical.PhysicalPlan
import octo.sql.plan.{logical => l, physical => p}

import scala.util.{Failure, Success, Try}

object QueryPlanner {

  def planExec()

  def planQuery(logicalPlan: l.LogicalPlan)(implicit tableRegistry: TableRegistry): PhysicalPlan = logicalPlan match {
    case l.Projection(child, expressions) => p.Projection(planQuery(child), expressions)
    case l.CartesianProduct(leftChild, rightChild) => p.CartesianProduct(planQuery(leftChild), planQuery(rightChild))

    case l.Filter(child, expression: e.Expression) => p.Filter(planQuery(child), expression)

    case l.Scan(tableName: TableName) => tableRegistry.getTableByName(tableName) match {
      case Success(table) => p.TableScan(tableName, table)
      case Failure(e) => throw e
    }
    case _ => throw new IllegalArgumentException(s"Unable to plan query because ${logicalPlan} needs to be a projection")
  }//.map(optimize)

  /*protected def optimize(physicalPlan: PhysicalPlan): PhysicalPlan = physicalPlan match {


  }*/


}
