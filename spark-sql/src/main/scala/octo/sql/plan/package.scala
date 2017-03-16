package octo.sql

package object plan {

  type Rule[P <: Plan[P]] = P => P

}
