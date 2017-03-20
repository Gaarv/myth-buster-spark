package octo.sql.plan

trait Plan[P <: Plan[P]] extends t.TreeNode[P] {
  self: P =>
}
