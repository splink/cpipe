package example.processors

import com.datastax.driver.core.Session
import example.Config

trait Processor {
  def process(session: Session, config: Config): Int
}
