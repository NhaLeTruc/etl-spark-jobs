package com.bob.lee.sc.jobs

import com.bob.lee.sc.config.OtherConfig
import com.bob.lee.sc.operationns.pega.{ConvertToCsv, EnrichScBix}
import com.bob.lee.etl.sparkbase.sparkbaseApplication
import com.bob.lee.etl.sparkbase.config.sparkbaseConfigLoader
import com.bob.lee.etl.sparkbase.operations.impl.{NormalizedConform, PersistDataFrame}
import pureconfig.generic.vnto._

object ScBixReporting extends sparkbaseApplication {
  lazy val otherConfig = sparkbaseConfigLoader("other-config", sparkbaseCommandLineOptions.configFile.get)
    .loadOrThrow[OtherConfig]

  val jobPipeline =
    EnrichScBix("output" -> "fcs", dataRepositoryConfig) >>
    PersistDataFrame("filtered" -> "sc", "filteredScBix") >>
    NormalizedConform("output" -> "fcs", "output" -> "fcs", otherConfig.conformanceMap, withNulls = true) >>
    PersistDataFrame("conformed" -> "sc", "output" -> "fcs")

  jobPipeline.execute
}
