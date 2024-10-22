package org.apache.spark

import scala.reflect.ClassTag

object Utils {

  def classforName(className: String, initialize: Boolean = true): Class[_] = {
    val utilsClass = classOf[Utils]
    
    // Check if Spark 3.0 or higher by attempting to resolve the method
    try {
      val method = utilsClass.getMethod("classForName", classOf[String], classOf[Boolean], classOf[ClassLoader])
      method.invoke(null, className, java.lang.Boolean.box(initialize), Thread.currentThread().getContextClassLoader).asInstanceOf[Class[_]]
    } catch {
      // If NoSuchMethodException (Spark 2.4 or lower), fall back to the older signature
      case _: NoSuchMethodException =>
        val method = utilsClass.getMethod("classForName", classOf[String], classOf[Boolean])
        method.invoke(null, className, java.lang.Boolean.box(initialize)).asInstanceOf[Class[_]]
    }
  }
}
