package cubeoperator

object Functions {

    def combinations(xs: List[Any]): List[List[Any]] = xs match {
        case Nil => List(Nil)
        case h :: t => for (y <- List(h, "ALL"); ys <- combinations(t)) yield y :: ys
    }
}
