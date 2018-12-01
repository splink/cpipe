package example
object Output {
  var lastLength = 0
  def apply(s: String) = {
    val max = lastLength
    lastLength = s.length
    val sliced = s.slice(0, max)
    val diff = max - sliced.length
    val output = sliced + (0 to diff).map(_ => " ").mkString
    Console.err.print(s"$output\r")
  }
}
