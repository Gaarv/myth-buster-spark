package octo.sql.plan

import octo.{ tree => t }

trait Plan[P <: Plan[P]] extends t.TreeNode[P] {
  self: P =>
}
