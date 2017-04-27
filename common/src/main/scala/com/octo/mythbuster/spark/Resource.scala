package com.octo.mythbuster.spark

import java.net.URL

import com.google.common.io.Resources

import scala.util.Try

object Resource {

  def apply(resourceName: String): Option[URL] = Try(Resources.getResource(resourceName)).toOption

}
