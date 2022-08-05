package tech.mlsql.plugin.load


import org.apache.http.client.fluent.Request
import org.apache.http.util.EntityUtils

import streaming.core.datasource.{DataSinkConfig, DataSourceConfig, FSConfig, RewritableFSConfig, RewritableSinkConfig, RewritableSourceConfig, SourceInfo}
import streaming.core.strategy.platform.PlatformManager
import streaming.dsl.MLSQLExecuteContext
import tech.mlsql.common.PathFun
import tech.mlsql.common.utils.log.Logging
import tech.mlsql.plugin.load.MultiS3BucketSourcePlugin.{PATH_PREFIX, TENANT_ID}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.charset.Charset


class MultiS3BucketSourcePlugin extends RewritableSourceConfig with Logging {

  override def rewrite_conf(config: DataSourceConfig, format: String, context: MLSQLExecuteContext): DataSourceConfig = {
    val awsInfo = MultiS3Bucket.getAWSInfo(context)
    if( awsInfo.isEmpty ) {
      logWarning("Failed to get AWS bucket and role")
      return config
    }
    // Save FS config to SparkSession
    val fsConf = MultiS3Bucket.S3AccessConf + ("fs.s3a.assumed.role.arn" -> awsInfo.get.role)
    MLSQLMultiBucket.configFS(fsConf, context.execListener.sparkSession )

    val prefix = "s3a://" + awsInfo.get.bucket + "/" + context.execListener.env()(PATH_PREFIX)
    // Rewrite the path
    config.copy( PathFun.joinPath( prefix, config.path) )
  }

  override def rewrite_source(sourceInfo: SourceInfo, format: String, context: MLSQLExecuteContext): SourceInfo = {
    sourceInfo
  }
}

object MultiS3BucketSourcePlugin {
  val PATH_PREFIX = "path_prefix"
  val TENANT_ID = "tenant_id"
}

class MultiS3BucketSinkPlugin extends RewritableSinkConfig with Logging {
  override def rewrite(config: DataSinkConfig, format: String, context: MLSQLExecuteContext): DataSinkConfig = {
    val awsInfo = MultiS3Bucket.getAWSInfo(context)
    if( awsInfo.isEmpty ) {
      logWarning("Failed to get AWS bucket and role")
      return config
    }

    // Save FS config to SparkSession
    val fsConf = MultiS3Bucket.S3AccessConf + ("fs.s3a.assumed.role.arn" -> awsInfo.get.role)
    MLSQLMultiBucket.configFS(fsConf, context.execListener.sparkSession )
    val prefix = "s3a://" + awsInfo.get.bucket + "/" + context.execListener.env()(PATH_PREFIX)
    // Rewrite the path
    config.copy( PathFun.joinPath( prefix, config.path) )

  }
}

class MultiS3BucketFSPlugin extends RewritableFSConfig with Logging {
  override def rewrite(config: FSConfig, context: MLSQLExecuteContext): FSConfig = {

    val awsInfo = MultiS3Bucket.getAWSInfo(context)
    if( awsInfo.isEmpty ) {
      logWarning("Failed to get AWS bucket and role")
      return config
    }
    // Save FS config to SparkSession
    val fsConf = MultiS3Bucket.S3AccessConf + ("fs.s3a.assumed.role.arn" -> awsInfo.get.role)
    MLSQLMultiBucket.configFS(fsConf, context.execListener.sparkSession )

    val prefix = "s3a://" + awsInfo.get.bucket + "/" + context.execListener.env()(PATH_PREFIX)
    // Rewrite the path
    config.copy( config.conf, PathFun.joinPath( prefix, config.path) )
  }
}

object MultiS3Bucket extends Logging {

  def getAWSInfo(context: MLSQLExecuteContext): Option[AWSInfo] = {
    if ( ! context.execListener.env().contains( TENANT_ID ) || ! context.execListener.env().contains(PATH_PREFIX) ) {
      logWarning(s"Either ${TENANT_ID} or ${PATH_PREFIX} is not defined")
      return Option.empty
    }
    val tenantId = context.execListener.env()(TENANT_ID)

    val params = PlatformManager.getOrCreate.config.get()
    val _zenServiceUrl = params.getParam("streaming.zen.service", "http://http://prime-backend-service.zen:9002")
    val url = s"http://${_zenServiceUrl}/api/v1/roles?tenant_id=${tenantId}"
    val resp = Request.Get(url)
      .connectTimeout(10 * 1000)      // Timeout in milliseconds
      .execute()
      .returnResponse()
    if( resp.getStatusLine.getStatusCode != 200 ) {
      throw new RuntimeException(s"Failed to get user ${context.owner} S3 access information")
    }
    val content = if (resp.getEntity != null) EntityUtils.toByteArray(resp.getEntity) else Array[Byte]()
    val contentStr = new String(content, Charset.forName(UTF_8.name()))

    import org.json4s._
    import org.json4s.jackson.Serialization.read
    implicit val formats = DefaultFormats
    val zenResult = read[ZenResult](contentStr)
    zenResult.data

    val bucket = "s3a://zjc-2"
    val role = "arn:aws:iam::013043072193:role/zjc_test_role"
    Some(AWSInfo(bucket, role))
  }

  val S3AccessConf = Map("fs.AbstractFileSystem.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3A" ,
    "fs.s3a.impl" -> "org.apache.hadoop.fs.s3a.S3AFileSystem" ,
    "fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider")

}

case class AWSInfo(bucket: String, role: String)

case class ZenS3Data(name: String, region: String, role: String, tenant_id: String)
case class ZenResult(code: String, message: String, data: Seq[ZenS3Data])
