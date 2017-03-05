package octo.sql.plan.physical.codegen

trait SupportsCodeGen {

  type Code = String

  def generateCode(parentCode: Code = ""): Code

}
