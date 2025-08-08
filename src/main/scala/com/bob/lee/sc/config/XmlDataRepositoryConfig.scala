package com.bob.lee.sc.config

import com.bob.lee.etl.sparkbase.datarepository.DataUID

case class XmlDataRepositoryConfig(
                                  dataStoreConfig: List[XmlDataRepositoryConfig]
                                  )

case class XmlDataStoreConfig(
                             dataUID: DataUID,
                             path: String,
                             rowTag: String,
                             selectColumns: Option[List[String]],
                             renameColumns: Option[Map[String, String]],
                             partitioning: Option[Boolean] = Some(true)
                             )
