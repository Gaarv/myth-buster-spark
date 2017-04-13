package com.octo.mythbuster.spark

import java.net.URL

import com.google.common.io.Resources

object Resource {

  def apply(resourceName: String): Option[URL] = Option(Resources.getResource(resourceName))

}
