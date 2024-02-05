package part1recap

import scala.annotation.tailrec

object MyObject extends App {

  print(reverse(List(1,2,3,4,5)))




  def reverse(list: List[Int]): List[Int] = {
    @tailrec
    def rec(list: List[Int], acc: List[Int] = List()): List[Int] = {
      list match {
        case Nil => acc
        case head :: tail => rec(tail, head::acc)
      }
    }

    rec(list)
  }
}

trait MyObject