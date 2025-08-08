package com.bob.lee.sc.sqls.common

case object GlobalSettings {
  val SqlStatement =
    """
      |SELECT
      |to_date(snapshot_date) AS snapshot_date,
      |snapshot_year
      |FROM
      | kyc_snapshot_config
      |WHERE table_name = 'kyc_snapshot'
      |""".stripMargin
}
