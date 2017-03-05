package octo.sql.plan.physical.codegen

trait CodeGenerator {

  type Code = String

  def generateCode(parentCode: Code = ""): Code

}
