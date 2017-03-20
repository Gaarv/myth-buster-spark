package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.{ tree => t }

trait Plan[P <: Plan[P]] extends t.TreeNode[P] {
  self: P =>
}
