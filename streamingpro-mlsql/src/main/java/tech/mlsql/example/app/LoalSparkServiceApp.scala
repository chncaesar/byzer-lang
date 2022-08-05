package tech.mlsql.example.app

import streaming.core.StreamingApp
import tech.mlsql.runtime.AppRuntimeStore

/**
 * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
 */
object LocalSparkServiceApp {

  def main(args: Array[String]): Unit = {
    StreamingApp.main(Array(
      "-streaming.master", "local[*]",
      "-streaming.name", "Mlsql-desktop",
      "-streaming.rest", "true",
      "-streaming.thrift", "false",
      "-streaming.platform", "spark",
      "-streaming.spark.service", "true",
      "-streaming.job.cancel", "true",
      "-streaming.datalake.path", "/Users/jiachuan.zhu/juicefs-data/byzer-lang-1/delta",
      "-streaming.driver.port", "9003",
      "-streaming.enableHiveSupport", "true",
      "-streaming.plugin.builtin.load.before.config", "tech.mlsql.plugin.load.MultiS3BucketSourcePlugin",
      "-streaming.plugin.builtin.save.before.config", "tech.mlsql.plugin.load.MultiS3BucketSinkPlugin",
      "-streaming.plugin.builtin.fs.before.config", "tech.mlsql.plugin.load.MultiS3BucketFSPlugin",
      "-streaming.zen.service", "http://prime-backend-service.zen:9003"
    ) ++ args )
  }
}
