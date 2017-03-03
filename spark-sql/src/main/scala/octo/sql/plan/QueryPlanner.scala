package octo.sql.plan

import octo.Implicits._
import octo.sql.{expression => e}
import octo.sql.{Row, TableName, TableRegistry, _}
import octo.sql.expression.TableColumn
import octo.sql.plan.physical.PhysicalPlan
import octo.sql.plan.{logical => l, physical => p}

import scala.util.{Failure, Success, Try}

object QueryPlanner {

  protected def planStage(logicalPlan: l.LogicalPlan)(implicit tableRegistry: TableRegistry): p.Stage = logicalPlan match {
    case l.CartesianProduct(leftChild, rightChild) => p.CartesianProduct(planStage(leftChild), planStage(rightChild))

    case l.Filter(child, expression: e.Expression) => p.Filter(planStage(child), expression)

    case l.Scan(tableName: TableName) => tableRegistry.getTableByName(tableName) match {
      case Success(table) => p.TableScan(tableName, table)
      case Failure(e) => throw e
    }

  }

  def planQuery(logicalPlan: l.LogicalPlan)(implicit tableRegistry: TableRegistry): Try[PhysicalPlan] = (logicalPlan match {
    case l.Projection(child, expressions) => Success(p.PhysicalPlan(p.Projection(planStage(child), expressions)))
    case _ => Failure(new IllegalArgumentException(s"Unable to plan query because ${logicalPlan} needs to be a projection"))
  }).map(optimize)

  protected def optimize(physicalPlan: PhysicalPlan): PhysicalPlan = physicalPlan match {


  }


}
