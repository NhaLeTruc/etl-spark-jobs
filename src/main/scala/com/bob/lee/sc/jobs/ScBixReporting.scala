package com.bob.lee.sc.jobs

import com.bob.lee.sc.config.OtherConfig
import com.bob.lee.sc.operationns.pega.{ConvertToCsv, EnrichScBix}
import com.bob.lee.odyssey.calypso.CalypsoApplication
import com.bob.lee.odyssey.calypso.config.CalypsoConfigLoader
import com.bob.lee.odyssey.calypso.operations.impl.{NormalizedConform, PersistDataFrame}
import pureconfig.generic.auto._

object ScBixReporting extends CalypsoApplication {
  lazy val otherConfig = CalypsoConfigLoader("other-config", calypsoCommandLineOptions.configFile.get)
    .loadOrThrow[OtherConfig]

  val jobPipeline =
    EnrichScBix("output" -> "fcs", dataRepositoryConfig) >>
    PersistDataFrame("filtered" -> "sc", "filteredScBix") >>
    NormalizedConform("output" -> "fcs", "output" -> "fcs", otherConfig.conformanceMap, withNulls = true) >>
    PersistDataFrame("conformed" -> "sc", "output" -> "fcs")

  jobPipeline.execute
}
