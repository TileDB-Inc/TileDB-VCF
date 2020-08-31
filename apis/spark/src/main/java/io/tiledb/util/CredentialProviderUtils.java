package io.tiledb.util;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;

/** Utility for handling AWS session tokens. */
public final class CredentialProviderUtils {

  private static final Logger log = Logger.getLogger(CredentialProviderUtils.class);

  private static final String TILEDB_ACCESS_KEY_PROP = "tiledb.vfs.s3.aws_access_key_id";
  private static final String TILEDB_SECRET_KEY_PROP = "tiledb.vfs.s3.aws_secret_access_key";
  private static final String TILEDB_SESSION_TOKEN_PROP = "tiledb.vfs.s3.aws_session_token";

  /**
   * Builds a credentials provider using Java's reflection API.
   *
   * @param className class of credential provider
   * @param roleArn IAM role arn to use
   * @return credential provider
   */
  public static Optional<AWSSessionCredentialsProvider> get(
      final String className, final String roleArn) {

    try {
      Class<? extends AWSSessionCredentialsProvider> clazz =
          Class.forName(className).asSubclass(AWSSessionCredentialsProvider.class);
      Constructor<? extends AWSSessionCredentialsProvider> constructor =
          clazz.getConstructor(String.class);
      AWSSessionCredentialsProvider provider = constructor.newInstance(roleArn);
      return Optional.of(provider);

    } catch (final Exception e) {
      log.error("Unable to form credentials provider", e);
      return Optional.empty();
    }
  }

  /**
   * Builds a key-value configuration map of tile-db AWS credentials.
   *
   * @param provider aws credential provider
   * @return map of configuration
   */
  public static Map<String, String> buildConfigMap(final AWSSessionCredentialsProvider provider) {
    final AWSSessionCredentials credentials = provider.getCredentials();
    return ImmutableMap.of(
        TILEDB_ACCESS_KEY_PROP, credentials.getAWSAccessKeyId(),
        TILEDB_SECRET_KEY_PROP, credentials.getAWSSecretKey(),
        TILEDB_SESSION_TOKEN_PROP, credentials.getSessionToken());
  }
}
