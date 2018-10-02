import scala.collection.Seq

package object Functions {

    def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {

        Some(runningCount.getOrElse(0) + newValues.sum)
    }
}
