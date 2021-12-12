package scalalearn.singletonobject

import Logger.info

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/13 16:34
 */
object SingletonObject {

  /*class Project(name: String, daysToComplete: Int)

  class Test {
    val project1 = new Project("TPS Reports", 1)
    val project2 = new Project("Website redesign", 5)
    // Prints "INFO: Created projects"
    info("Created projects")

  }*/
  def main(args: Array[String]): Unit = {
    val scalaCenterEmail = Email.fromString("scala.center@epfl.ch")
    scalaCenterEmail match {
      case Some(email) => println(
        s"""Registered an email
           |Username: ${email.username}
           |Domain name: ${email.domainName}
     """)
      case None => println("Error: could not parse email")
    }
  }


}
