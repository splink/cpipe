package org.splink.cpipe.processors

import com.datastax.driver.core.Session
import org.splink.cpipe.config.Config

trait Processor {
  def process(session: Session, config: Config): Int
}
