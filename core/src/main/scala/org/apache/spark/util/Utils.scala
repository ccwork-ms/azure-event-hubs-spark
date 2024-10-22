package org.apache.spark

object Utils {

  def classforName(className: String, initialize: Boolean = true): Class[_] = {
    val utilsClass = classOf[Utils.type]

    // Method to invoke the classForName method reflectively
    def invokeClassForName(methodName: String, params: Any*): Class[_] = {
      val method = utilsClass.getMethod(methodName, params.map(_.getClass): _*)
      method.invoke(null, params: _*).asInstanceOf[Class[_]]
    }

    try {
      // Attempt to use the Spark 3.0+ signature
      invokeClassForName("classForName", className, initialize, Thread.currentThread().getContextClassLoader)
    } catch {
      case _: NoSuchMethodException =>
        // Fallback for Spark 2.4 or lower
        invokeClassForName("classForName", className, initialize)
      case e: ClassNotFoundException =>
        throw new ClassNotFoundException(s"Class not found: $className", e)
      case e: Exception =>
        throw new RuntimeException("Error while loading class", e)
    }
  }
}
