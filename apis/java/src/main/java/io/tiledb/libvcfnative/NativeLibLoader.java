/*--------------------------------------------------------------------------
 *  Copyright 2011 Taro L. Saito
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *--------------------------------------------------------------------------
 *
 * Originally from https://github.com/xerial/snappy-java
 * Modifications made by TileDB, Inc. 2018
 *
 * */

package io.tiledb.libvcfnative;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;

/** Helper class that finds native libraries embedded as resources and loads them dynamically. */
public class NativeLibLoader {
  private static final String UNKNOWN = "unknown";
  private static final Logger logger = Logger.getLogger(NativeLibLoader.class.getName());

  /** Path (relative to jar) where native libraries are located. */
  private static final String LIB_RESOURCE_DIR = "/lib";

  /** Finds and loads native TileDB. */
  static void loadNativeTileDB() {
    String versionedLibName;
    try {
      versionedLibName = getVersionedName();
      if (versionedLibName != null) {
        logger.info("Loaded libtiledb library: " + versionedLibName);
        loadNativeLib(versionedLibName, false);
      } else {
        logger.info(
            "libtiledb library not in JAR, assuming system version exists or linker errors will occur");
      }
    } catch (java.lang.UnsatisfiedLinkError e) {
      // If a native library fails to link, we fall back to depending on the system
      // dynamic linker to satisfy the requirement. Therefore, we do nothing here
      // (if the library is not available via the system linker, a runtime error
      // will occur later).
      logger.warning(e.getMessage());
    } catch (IOException ioe) {
      logger.warning(ioe.getMessage());
    }
  }

  /** Finds and loads native TileDB-VCF. */
  static void loadNativeTileDBVCF() {
    try {
      loadNativeLib("tiledbvcf", true);
    } catch (java.lang.UnsatisfiedLinkError e) {
      // If a native library fails to link, we fall back to depending on the system
      // dynamic linker to satisfy the requirement. Therefore, we do nothing here
      // (if the library is not available via the system linker, a runtime error
      // will occur later).
      logger.warning(e.getMessage());
    }
  }

  /** Finds and loads native TileDB-VCF JNI. */
  static void loadNativeTileDBVCFJNI() {
    try {
      loadNativeLib("tiledbvcfjni", true);
    } catch (java.lang.UnsatisfiedLinkError e) {
      // If a native library fails to link, we fall back to depending on the system
      // dynamic linker to satisfy the requirement. Therefore, we do nothing here
      // (if the library is not available via the system linker, a runtime error
      // will occur later).
      logger.warning(e.getMessage());
    }
  }

  /** Finds and loads native HTSlib. */
  static void loadNativeHTSLib() {
    String os = getOSClassifier();
    String versionedLibName = os.startsWith("osx") ? "libhts.1.15.1.dylib" : "libhts.so.1.15.1";
    try {
      // Don't use name mapping to get the versioned htslib
      loadNativeLib(versionedLibName, false);
    } catch (java.lang.UnsatisfiedLinkError e) {
      // If a native library fails to link, we fall back to depending on the system
      // dynamic linker to satisfy the requirement. Therefore, we do nothing here
      // (if the library is not available via the system linker, a runtime error
      // will occur later).
      logger.warning(e.getMessage());
    }
  }

  private static boolean contentsEquals(InputStream in1, InputStream in2) throws IOException {
    if (!(in1 instanceof BufferedInputStream)) {
      in1 = new BufferedInputStream(in1);
    }
    if (!(in2 instanceof BufferedInputStream)) {
      in2 = new BufferedInputStream(in2);
    }

    int ch = in1.read();
    while (ch != -1) {
      int ch2 = in2.read();
      if (ch != ch2) {
        return false;
      }
      ch = in1.read();
    }
    int ch2 = in2.read();
    return ch2 == -1;
  }

  private static String normalizeOs(String value) {
    value = normalize(value);
    if (value.startsWith("aix")) {
      return "aix";
    }
    if (value.startsWith("hpux")) {
      return "hpux";
    }
    if (value.startsWith("os400")) {
      // Avoid the names such as os4000
      if (value.length() <= 5 || !Character.isDigit(value.charAt(5))) {
        return "os400";
      }
    }
    if (value.startsWith("linux")) {
      return "linux";
    }
    if (value.startsWith("macosx") || value.startsWith("osx")) {
      return "osx";
    }
    if (value.startsWith("freebsd")) {
      return "freebsd";
    }
    if (value.startsWith("openbsd")) {
      return "openbsd";
    }
    if (value.startsWith("netbsd")) {
      return "netbsd";
    }
    if (value.startsWith("solaris") || value.startsWith("sunos")) {
      return "sunos";
    }
    if (value.startsWith("windows")) {
      return "windows";
    }

    return UNKNOWN;
  }

  private static String normalizeArch(String value) {
    value = normalize(value);
    if (value.matches("^(x8664|amd64|ia32e|em64t|x64)$")) {
      return "x86_64";
    }
    if (value.matches("^(x8632|x86|i[3-6]86|ia32|x32)$")) {
      return "x86_32";
    }
    if (value.matches("^(ia64w?|itanium64)$")) {
      return "itanium_64";
    }
    if ("ia64n".equals(value)) {
      return "itanium_32";
    }
    if (value.matches("^(sparc|sparc32)$")) {
      return "sparc_32";
    }
    if (value.matches("^(sparcv9|sparc64)$")) {
      return "sparc_64";
    }
    if (value.matches("^(arm|arm32)$")) {
      return "arm_32";
    }
    if ("aarch64".equals(value)) {
      return "aarch_64";
    }
    if (value.matches("^(mips|mips32)$")) {
      return "mips_32";
    }
    if (value.matches("^(mipsel|mips32el)$")) {
      return "mipsel_32";
    }
    if ("mips64".equals(value)) {
      return "mips_64";
    }
    if ("mips64el".equals(value)) {
      return "mipsel_64";
    }
    if (value.matches("^(ppc|ppc32)$")) {
      return "ppc_32";
    }
    if (value.matches("^(ppcle|ppc32le)$")) {
      return "ppcle_32";
    }
    if ("ppc64".equals(value)) {
      return "ppc_64";
    }
    if ("ppc64le".equals(value)) {
      return "ppcle_64";
    }
    if ("s390".equals(value)) {
      return "s390_32";
    }
    if ("s390x".equals(value)) {
      return "s390_64";
    }

    return UNKNOWN;
  }

  private static String normalize(String value) {
    if (value == null) {
      return "";
    }
    return value.toLowerCase(Locale.US).replaceAll("[^a-z0-9]+", "");
  }

  private static String getOSClassifier() {

    final Properties allProps = new Properties(System.getProperties());
    final String osName = allProps.getProperty("os.name");
    final String osArch = allProps.getProperty("os.arch");

    final String detectedName = normalizeOs(osName);
    final String detectedArch = normalizeArch(osArch);

    return detectedName + "-" + detectedArch;
  }

  /**
   * Extract the specified library file from resources to the target directory.
   *
   * @param libraryDir Path of directory containing native library
   * @param libraryName Name of native library
   * @param targetDir Path of target directory to extract library to
   * @param mapLibraryName If true, transform libraryName with System.mapLibraryName
   * @return File pointing to the extracted library
   */
  private static File extractLibraryFile(
      String libraryDir, String libraryName, String targetDir, boolean mapLibraryName) {
    String libraryFileName = mapLibraryName ? System.mapLibraryName(libraryName) : libraryName;
    String nativeLibraryFilePath = new File(libraryDir, libraryFileName).getAbsolutePath();

    // Attach UUID to the native library file to ensure multiple class loaders can read the
    // libsnappy-java multiple times.
    String uuid = UUID.randomUUID().toString();
    String extractedLibFileName = String.format("%s-%s-%s", libraryName, uuid, libraryFileName);
    File extractedLibFile = new File(targetDir, extractedLibFileName);

    try {
      // Extract a native library file into the target directory
      InputStream reader = null;
      FileOutputStream writer = null;
      try {
        reader = NativeLibLoader.class.getResourceAsStream(nativeLibraryFilePath);
        try {
          writer = new FileOutputStream(extractedLibFile);

          byte[] buffer = new byte[8192];
          int bytesRead = 0;
          while ((bytesRead = reader.read(buffer)) != -1) {
            writer.write(buffer, 0, bytesRead);
          }
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
      } finally {
        if (reader != null) {
          reader.close();
        }

        // Delete the extracted lib file on JVM exit.
        extractedLibFile.deleteOnExit();
      }

      // Set executable (x) flag to enable Java to load the native library
      boolean success =
          extractedLibFile.setReadable(true)
              && extractedLibFile.setWritable(true, true)
              && extractedLibFile.setExecutable(true);
      if (!success) {
        // Setting file flag may fail, but in this case another error will be thrown in later phase
      }

      // Check whether the contents are properly copied from the resource folder
      {
        InputStream nativeIn = null;
        InputStream extractedLibIn = null;
        try {
          nativeIn = NativeLibLoader.class.getResourceAsStream(nativeLibraryFilePath);
          extractedLibIn = new FileInputStream(extractedLibFile);

          if (!contentsEquals(nativeIn, extractedLibIn)) {
            throw new IOException(
                String.format("Failed to write a native library file at %s", extractedLibFile));
          }
        } finally {
          if (nativeIn != null) {
            nativeIn.close();
          }
          if (extractedLibIn != null) {
            extractedLibIn.close();
          }
        }
      }

      return new File(targetDir, extractedLibFileName);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      return null;
    }
  }

  /**
   * Finds and extracts a native library from resources to a temporary directory on the filesystem.
   *
   * @param libraryName Name of native library
   * @param mapLibraryName If true, transform libraryName with System.mapLibraryName
   * @return File pointing to the extracted library
   */
  private static File findNativeLibrary(String libraryName, boolean mapLibraryName) {
    String mappedLibraryName = mapLibraryName ? System.mapLibraryName(libraryName) : libraryName;
    String libDir = new File(LIB_RESOURCE_DIR).getAbsolutePath();
    File libPath = new File(libDir, mappedLibraryName);

    boolean hasNativeLib = hasResource(libPath.getAbsolutePath());
    if (!hasNativeLib) {
      return null;
    }

    // Temporary folder for the extracted native lib.
    File tempFolder = new File(System.getProperty("java.io.tmpdir"));
    if (!tempFolder.exists()) {
      boolean created = tempFolder.mkdirs();
      if (!created) {
        // if created == false, it will fail eventually in the later part
      }
    }

    // Extract and load a native library inside the jar file
    return extractLibraryFile(libDir, libraryName, tempFolder.getAbsolutePath(), mapLibraryName);
  }

  /**
   * Finds and loads a native library of the given name.
   *
   * @param libraryName Name of native library
   * @param mapLibraryName If true, transform libraryName with System.mapLibraryName
   */
  private static void loadNativeLib(String libraryName, boolean mapLibraryName) {
    File nativeLibFile = findNativeLibrary(libraryName, mapLibraryName);
    if (nativeLibFile != null) {
      // Load extracted or specified native library.
      System.load(nativeLibFile.getAbsolutePath());
    } else {
      // Try loading preinstalled library (in the path -Djava.library.path)
      System.loadLibrary(mapLibraryName ? System.mapLibraryName(libraryName) : libraryName);
    }
  }
  /**
   * Extracts the versioned libtiledb library name
   *
   * @return The library name
   */
  private static String getVersionedName() throws IOException {
    URL jarLocation = NativeLibLoader.class.getProtectionDomain().getCodeSource().getLocation();

    String libName = getOSClassifier().startsWith("osx") ? "libtiledb.dylib" : "libtiledb.so";

    // Not running from a .jar file
    if (!jarLocation.toString().endsWith(".jar")) {
      String currentPath = Paths.get(".").toAbsolutePath().normalize().toString();
      String libPath = currentPath + "/build/resources/main/lib";
      Collection<File> files = FileUtils.listFiles(new File(libPath), null, false);

      for (File f : files) {
        if (f.toString().contains(libName)) return f.getName();
      }
    }
    // Jar file
    else {
      ZipInputStream zipInputStream = new ZipInputStream(jarLocation.openStream());
      ZipEntry e;

      while (true) {
        e = zipInputStream.getNextEntry();

        if (e == null) break;

        if (e.toString().contains(libName)) {
          String[] splitName = e.toString().split("\\/");
          return splitName[splitName.length - 1];
        }
      }
    }
    return null;
  }

  private static boolean hasResource(String path) {
    return NativeLibLoader.class.getResource(path) != null;
  }
}
