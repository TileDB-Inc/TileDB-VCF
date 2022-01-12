package io.tiledb.vcf;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * An immutable string-to-string map in which keys are case-insensitive. This is used to represent
 * data source options.
 *
 * <p>Each data source implementation can define its own options and teach its users how to set
 * them. Spark doesn't have any restrictions about what options a data source should or should not
 * have. Instead Spark defines some standard options that data sources can optionally adopt. It's
 * possible that some options are very common and many data sources use them. However different data
 * sources may define the common options(key and meaning) differently, which is quite confusing to
 * end users.
 */
public class DataSourceOptions {
  private final Map<String, String> keyLowerCasedMap;

  private String toLowerCase(String key) {
    return key.toLowerCase(Locale.ROOT);
  }

  public static DataSourceOptions empty() {
    return new DataSourceOptions(new HashMap<>());
  }

  public DataSourceOptions(Map<String, String> originalMap) {
    keyLowerCasedMap = new HashMap<>(originalMap.size());
    for (Map.Entry<String, String> entry : originalMap.entrySet()) {
      keyLowerCasedMap.put(toLowerCase(entry.getKey()), entry.getValue());
    }
  }

  public Map<String, String> asMap() {
    return new HashMap<>(keyLowerCasedMap);
  }

  /** Returns the option value to which the specified key is mapped, case-insensitively. */
  public Optional<String> get(String key) {
    return Optional.ofNullable(keyLowerCasedMap.get(toLowerCase(key)));
  }

  /**
   * Returns the boolean value to which the specified key is mapped, or defaultValue if there is no
   * mapping for the key. The key match is case-insensitive
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey)
        ? Boolean.parseBoolean(keyLowerCasedMap.get(lcaseKey))
        : defaultValue;
  }

  /**
   * Returns the integer value to which the specified key is mapped, or defaultValue if there is no
   * mapping for the key. The key match is case-insensitive
   */
  public int getInt(String key, int defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey)
        ? Integer.parseInt(keyLowerCasedMap.get(lcaseKey))
        : defaultValue;
  }

  /**
   * Returns the long value to which the specified key is mapped, or defaultValue if there is no
   * mapping for the key. The key match is case-insensitive
   */
  public long getLong(String key, long defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey)
        ? Long.parseLong(keyLowerCasedMap.get(lcaseKey))
        : defaultValue;
  }

  /**
   * Returns the double value to which the specified key is mapped, or defaultValue if there is no
   * mapping for the key. The key match is case-insensitive
   */
  public double getDouble(String key, double defaultValue) {
    String lcaseKey = toLowerCase(key);
    return keyLowerCasedMap.containsKey(lcaseKey)
        ? Double.parseDouble(keyLowerCasedMap.get(lcaseKey))
        : defaultValue;
  }

  /** The option key for singular path. */
  public static final String PATH_KEY = "path";

  /** The option key for multiple paths. */
  public static final String PATHS_KEY = "paths";

  /** The option key for table name. */
  public static final String TABLE_KEY = "table";

  /** The option key for database name. */
  public static final String DATABASE_KEY = "database";

  /**
   * Returns all the paths specified by both the singular path option and the multiple paths option.
   */
  public String[] paths() {
    String[] singularPath = get(PATH_KEY).map(s -> new String[] {s}).orElseGet(() -> new String[0]);
    Optional<String> pathsStr = get(PATHS_KEY);
    if (pathsStr.isPresent()) {
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        String[] paths = objectMapper.readValue(pathsStr.get(), String[].class);
        return Stream.of(singularPath, paths).flatMap(Stream::of).toArray(String[]::new);
      } catch (IOException e) {
        return singularPath;
      }
    } else {
      return singularPath;
    }
  }

  /** Returns the value of the table name option. */
  public Optional<String> tableName() {
    return get(TABLE_KEY);
  }

  /** Returns the value of the database name option. */
  public Optional<String> databaseName() {
    return get(DATABASE_KEY);
  }
}
