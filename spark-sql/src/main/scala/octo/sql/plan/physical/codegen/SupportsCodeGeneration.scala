package octo.sql.plan.physical.codegen

trait SupportsCodeGeneration {

  type Code = String

  def generateCode(parentCode: Code = ""): Code

}
