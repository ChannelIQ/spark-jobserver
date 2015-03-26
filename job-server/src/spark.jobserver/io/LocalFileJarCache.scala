/**
 * Copyright 2015 (C) Channel IQ.  All Rights Reserved.
 */
package spark.jobserver.io

import java.io.{FileOutputStream, BufferedOutputStream, File}

import org.joda.time.DateTime
import org.slf4j.LoggerFactory

class LocalFileJarCache(rootDir: String) {

  private val logger = LoggerFactory.getLogger(getClass)

  private val rootDirFile = new File(rootDir)
  if (!rootDirFile.exists()) {
    if (!rootDirFile.mkdirs()) {
      throw new RuntimeException("Could not create directory " + rootDir)
    }
  }

  def store(appName: String, uploadTime: DateTime, jarBytes: Array[Byte]): Unit = {
    store(jarFile(appName, uploadTime), jarBytes)
  }

  def store(outFile: File, jarBytes: Array[Byte]): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(outFile))
    try {
      logger.debug("Writing {} bytes to file {}", jarBytes.size, outFile.getPath)
      bos.write(jarBytes)
      bos.flush()
    } finally {
      bos.close()
    }
  }

  def retrieve(appName: String, uploadTime: DateTime, loadJar: => Array[Byte]): String = {
    val file = jarFile(appName, uploadTime)
    if (!file.exists()) {
      store(file, loadJar)
    }
    file.getAbsolutePath
  }

  def jarFile(appName: String, uploadTime: DateTime) =
    new File(rootDir, appName + "-" + uploadTime.toString().replace(':', '_') + ".jar")
}
