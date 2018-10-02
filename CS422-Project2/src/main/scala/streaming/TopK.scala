package streaming

abstract class TopK extends Product with Serializable

case class PreciseTopK(topK: Int) extends TopK

case class ApproxTopK(monitoredPair: String) extends TopK {
    var mask: Array[(Int, Int)] = Array.empty
}