package com.octo.mythbuster.spark.sql.plan

import com.octo.mythbuster.spark.{ tree => t }

// A plan can be either logical or physical, and inherits all the properties of a TreeNode (in order to simplify the optimization)
trait Plan[P <: Plan[P]] extends t.TreeNode[P] {
  self: P =>
}
