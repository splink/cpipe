package example

class Rps {
  private var timestamp = System.currentTimeMillis()
  private var rps = 0
  private var lastRowstamp = 0
  private var rowstamps = List.empty[Int]

  override def toString = rps.toString

  def compute(index: Int) = {
    val nextTimestamp = System.currentTimeMillis()
    if(nextTimestamp - timestamp > 1000) {
      val currentRps = index - lastRowstamp
      rowstamps = (currentRps :: rowstamps).slice(0, 20).sorted
      rps = if(rowstamps.size > 10) rowstamps(10) else rowstamps.last
      lastRowstamp = index
      timestamp = nextTimestamp
    }
  }
}
