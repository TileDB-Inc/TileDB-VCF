package io.tiledb.util;

import com.amazonaws.auth.AWSSessionCredentials;
import com.amazonaws.auth.AWSSessionCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import java.util.Optional;
import org.junit.Assert;
import org.junit.Test;

public class CredentialProviderUtilsTest {

  public static class NoOpCredentialProvider implements AWSSessionCredentialsProvider {

    public NoOpCredentialProvider(String roleArn) {
      // no-op
    }

    @Override
    public AWSSessionCredentials getCredentials() {
      return new BasicSessionCredentials("foo", "bar", "baz");
    }

    @Override
    public void refresh() {
      // no-op
    }
  }

  @Test
  public void testGet() {
    Optional<AWSSessionCredentialsProvider> provider =
        CredentialProviderUtils.get(
            "io.tiledb.util.CredentialProviderUtilsTest$NoOpCredentialProvider", "test");
    AWSSessionCredentials credentials = provider.get().getCredentials();
    Assert.assertEquals(credentials.getSessionToken(), "baz");
  }
}
