package org.splink.cpipe

class JsonFrame {

  def push(a: Array[Char]): Option[String] = {
    a.foreach(balance)

    buffer = buffer ++ a

    if (isComplete) {
      val result = Some(buffer.mkString)
      reset()
      result

    } else None
  }

  def balance(char: Char) = {
    if (char == Quote && !wasBackslash) {
      isQuoted = !isQuoted
    }

    if (!isQuoted) {

      if (char == OpenSquare && balanceCurly == 0) {
        balanceSquare = balanceSquare + 1
        isArray = true
      } else if (isArray) {
        if (char == OpenSquare) balanceSquare = balanceSquare + 1
        else if (char == CloseSquare) balanceSquare = balanceSquare - 1
      } else {
        if (char == OpenCurly) balanceCurly = balanceCurly + 1
        else if (char == CloseCurly) balanceCurly = balanceCurly - 1
      }

    }

    wasBackslash = char == Backslash
  }

  def isComplete =
    balanceCurly == 0 && !isArray || balanceSquare == 0 && isArray

  def reset() = {
    buffer = Array.empty[Char]
    balanceCurly = 0
    balanceSquare = 0
    isArray = false
  }

  val OpenCurly = '{'
  val CloseCurly = '}'
  val OpenSquare = '['
  val CloseSquare = ']'
  val Quote = '"'
  val Backslash = '\\'
  var balanceCurly = 0
  var balanceSquare = 0
  var isQuoted = false
  var buffer = Array.empty[Char]
  var isArray = false
  var wasBackslash = false

}