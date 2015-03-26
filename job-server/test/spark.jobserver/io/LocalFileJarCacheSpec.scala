package spark.jobserver.io

import java.io.File
import java.util.UUID

import com.google.common.io.Files
import org.joda.time.{DateTime, DateTimeZone}
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfter, FunSpec}

class LocalFileJarCacheSpec extends FunSpec with ShouldMatchers with BeforeAndAfter {

  var rootDir: File = _
  var cache: LocalFileJarCache = _

  before {
    rootDir = new File(new File(System.getProperty("java.io.tmpdir")),
      "test-" + UUID.randomUUID())
  }

  describe("A LocalFileJarCache") {

    it("should create temp directories on construction") {
      rootDir.exists() should be (false)
      cache = new LocalFileJarCache(rootDir.getAbsolutePath)
      rootDir.exists() should be (true)
    }

    it("should name jar files with app name and timestamp") {
      val ts = new DateTime(2014, 12, 31, 6, 11, 59, DateTimeZone.UTC)
      assert(cache.jarFile("testApp", ts).getName === "testApp-2014-12-31T06_11_59.000Z.jar")
    }

    it("should store jar files in the configured root path") {
      val ts = new DateTime(2010, 1, 1, 0, 0, 0, DateTimeZone.UTC)
      val jar = Array[Byte](0x0, 0x1, 0x2, 0x3)

      cache = new LocalFileJarCache(rootDir.getAbsolutePath)
      cache.store("fooApp", ts, jar)

      Files.toByteArray(new File(rootDir, "fooApp-2010-01-01T00_00_00.000Z.jar")) should be (jar)
    }

    it("should retrieve from file cache when present") {
      val ts = new DateTime(2010, 1, 1, 0, 0, 0, DateTimeZone.UTC)
      val jar = Array[Byte](0x0, 0x1, 0x2, 0x3)

      cache = new LocalFileJarCache(rootDir.getAbsolutePath)

      val f = new File(rootDir, "fooApp-2010-01-01T00_00_00.000Z.jar")
      f.createNewFile()
      Files.write(jar, f)

      def jarLoader = { throw new Exception("test fail: jar should be read from file system cache") }
      Files.toByteArray(new File(cache.retrieve("fooApp", ts, jarLoader))) should be(jar)
    }

    it("should retrieve from provided loader when not cached on file system") {
      val ts = new DateTime(2010, 1, 1, 0, 0, 0, DateTimeZone.UTC)
      val jar = Array[Byte](0xA, 0xB, 0xC, 0xD)

      cache = new LocalFileJarCache(rootDir.getAbsolutePath)

      Files.toByteArray(new File(cache.retrieve("fooApp", ts, { jar }))) should be(jar)
    }
  }
}
