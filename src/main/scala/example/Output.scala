package example

object Output {
  def apply(s: String) = {
    val max = 120
    val sliced = s.slice(0, max)
    val diff = max - sliced.length
    val output = sliced + (0 to diff).map(_ => " ").mkString
    Console.err.print(s"$output\r")
  }
}
