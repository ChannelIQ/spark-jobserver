/**
 * Copyright 2015 (C) Channel IQ.  All Rights Reserved.
 */
package spark.jobserver.io

import java.io.{FileOutputStream, BufferedOutputStream, File}
import java.nio.ByteBuffer

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.typesafe.config.Config
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

/**
 * Stores job, jar and config data in Cassandra tables.
 */
class JobCassandraDAO(config: Config) extends JobDAO {
  import spark.jobserver.io.JobCassandraDAO._
  import spark.jobserver.io.JobDAO._

  private val logger = LoggerFactory.getLogger(getClass)

  private val rootDir = getOrElse(config.getString("spark.jobserver.sqldao.rootdir"),
    "/tmp/spark-jobserver/cassandradao/data")
  private val rootDirFile = new File(rootDir)

  private val keyspace = getOrElse(config.getString(ConfigPrefix + "keyspace"), "jobserver")
  private val datacenter = getOrElse(config.getString(ConfigPrefix + "datacenter"), "Spark")
  private val hosts = getOrElse(config.getString(ConfigPrefix + "hosts"), "127.0.0.1")

  private val cluster = Cluster.builder
    .withLoadBalancingPolicy(new DCAwareRoundRobinPolicy(datacenter))
    .addContactPoints(hosts.split(",") : _*)
    .build()

  private val session = cluster.connect(keyspace)

  init()

  private val insertJarStatement = session.prepare(INSERT_JAR)
  private val insertAppStatement = session.prepare(INSERT_APP)
  private val insertJobStatement = session.prepare(INSERT_JOB_INFO)
  private val insertConfigStatement = session.prepare(INSERT_CONFIG)
  private val selectJarStatement = session.prepare(SELECT_JAR)

  private def init() {
    // Create the data directory if it doesn't exist
    if (!rootDirFile.exists()) {
      if (!rootDirFile.mkdirs()) {
        throw new RuntimeException("Could not create directory " + rootDir)
      }
    }

    def schemaInitialized: Boolean = {
      val ks = cluster.getMetadata.getKeyspace(keyspace)
      def tableExists(name: String) = ks.getTable(name) != null
      (ks != null) && tableExists("jars") && tableExists("jobs") && tableExists("apps") && tableExists("configs")
    }

    if (! schemaInitialized) {
      logger.info("Found uninitialized keyspace, creating tables in keyspace \"{}\"", keyspace)
      session.execute(CREATE_TABLE_JARS)
      session.execute(CREATE_TABLE_APPS)
      session.execute(CREATE_TABLE_JOBS)
      session.execute(CREATE_TABLE_CONFIGS)
    }
  }

  implicit def toDateTime(date: java.util.Date): DateTime = new DateTime(date)

  override def saveJar(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]): Unit = {
    cacheJar(jarFile(appName, uploadTime), jarBytes)

    session.execute(insertJarStatement.bind(appName, uploadTime.toDate, ByteBuffer.wrap(jarBytes)))
    session.execute(insertAppStatement.bind(appName, uploadTime.toDate))
  }

  override def getApps: Map[String, DateTime] = {
    session.execute(SELECT_ALL_APPS).all.toList.map(row =>
      row.getString("name") -> toDateTime(row.getDate("last_upload_time"))).toMap
  }

  override def saveJobConfig(jobId: String, jobConfig: Config): Unit = {
    session.execute(insertConfigStatement.bind(jobId, serializeJobConfigToJson(jobConfig)))
  }

  override def getJobInfos: Map[String, JobInfo] = {
    session.execute(SELECT_ALL_JOBS).all.toList.map(row => {
      val jobId = row.getString("job_id")
      jobId -> new JobInfo(
        jobId,
        row.getString("context_name"),
        new JarInfo(row.getString("jar_name"), row.getDate("jar_upload_time")),
        row.getString("classpath"),
        row.getDate("start_time"),
        Option(row.getDate("end_time")),
        Option(row.getString("error")).map(new Throwable(_))
      )
    }).toMap
  }

  override def saveJobInfo(jobInfo: JobInfo): Unit = {
    session.execute(insertJobStatement.bind(
      jobInfo.jobId,
      jobInfo.contextName,
      jobInfo.jarInfo.appName,
      jobInfo.jarInfo.uploadTime.toDate,
      jobInfo.classPath,
      jobInfo.startTime.toDate,
      jobInfo.endTime.map(_.toDate).orNull,
      jobInfo.error.map(t => t.getClass.getName + ":" + t.getMessage).orNull))
  }

  override def getJobConfigs: Map[String, Config] = {
    session.execute(SELECT_ALL_CONFIGS).all.toList.map(row =>
      row.getString("job_id") -> readJobConfigAsJson(row.getString("config"))).toMap
  }

  def retrieveJarFile(appName: String, uploadTime: DateTime): String = {
    def loadJar = session.execute(selectJarStatement.bind(appName, uploadTime.toDate)).one
      .getBytes("jar").array()

    val file = jarFile(appName, uploadTime)
    if (!file.exists()) {
      cacheJar(file, loadJar)
    }
    file.getAbsolutePath

  }

  def cacheJar(outFile: File, jarBytes: Array[Byte]): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", jarBytes.size, outFile.getPath)
      bos.write(jarBytes)
      bos.flush()
    } finally {
      bos.close()
    }
  }

  private def jarFile(appName: String, uploadTime: DateTime) =
    new File(rootDir, appName + "-" + uploadTime.toString().replace(':', '_') + ".jar")
}

object JobCassandraDAO {
  val ConfigPrefix = "spark.jobserver.cassandradao."

  val CREATE_TABLE_JARS =
    """CREATE TABLE IF NOT EXISTS jars (
      name text,
      upload_time timestamp,
      jar blob,
      PRIMARY KEY ((name, upload_time)));"""

  val CREATE_TABLE_APPS =
    """CREATE TABLE IF NOT EXISTS apps (
      name text PRIMARY KEY,
      last_upload_time timestamp);"""

  val CREATE_TABLE_JOBS =
    """CREATE TABLE IF NOT EXISTS jobs (
        job_id text PRIMARY KEY,
        context_name text,
        jar_name text,
        jar_upload_time timestamp,
        classpath text,
        start_time timestamp,
        end_time timestamp,
        error text);"""

  val CREATE_TABLE_CONFIGS =
    """CREATE TABLE IF NOT EXISTS configs (
      job_id text PRIMARY KEY,
      config text);"""

  val INSERT_JAR =
    """INSERT INTO jars (name, upload_time, jar) VALUES (?,?,?)"""

  val INSERT_APP =
    """INSERT INTO apps (name, last_upload_time) VALUES (?,?)"""

  val INSERT_CONFIG =
    """INSERT INTO configs (job_id, config) VALUES (?,?)"""

  val INSERT_JOB_INFO =
  """INSERT INTO jobs (job_id, context_name, jar_name, jar_upload_time, classpath, start_time, end_time, error)
     VALUES (?,?,?,?,?,?,?,?)"""

  val SELECT_JAR =
  """SELECT name, upload_time, jar FROM jars WHERE name=? AND upload_time=?"""

  val SELECT_ALL_APPS =
    """SELECT name, last_upload_time FROM apps"""

  val SELECT_ALL_CONFIGS =
    """SELECT job_id, config FROM configs"""

  val SELECT_ALL_JOBS =
    """SELECT job_id, context_name, jar_name, jar_upload_time, classpath, start_time, end_time, error FROM jobs"""
}
