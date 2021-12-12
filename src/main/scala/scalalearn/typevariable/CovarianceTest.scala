package scalalearn.typevariable

import scalalearn.typevariable.bean.{Animal, AnimalPrinter, Cat, CatPrinter, Dog, Printer}

/**
 * @Author: 21081020 lpl
 * @Date: 2021/9/14 10:08
 */
object CovarianceTest extends App {
  /*def printAnimalNames(animals: List[Animal]): Unit = {
    animals.foreach {
      animal => println(animal.name)
    }
  }

  val cats: List[Cat] = List(Cat("qwe"), Cat("quq"))
  val dogs: List[Dog] = List(Dog("汪"), Dog("汪汪"))
  printAnimalNames(cats)
  printAnimalNames(dogs)*/


  val myCat: Cat = Cat("Boots")
  //针对Printer[Cat]写一个方法
  def printMyCat(printer: Printer[Cat]): Unit = {
    printer.print(myCat)
  }
  val catPrinter: Printer[Cat] = new CatPrinter
  val animalPrinter: Printer[Animal] = new AnimalPrinter
  printMyCat(catPrinter)
  //Printer[Animal]也可以使用printMyCat，可见Printer[Animal]是Printer[Cat]的子类
  printMyCat(animalPrinter)
}
