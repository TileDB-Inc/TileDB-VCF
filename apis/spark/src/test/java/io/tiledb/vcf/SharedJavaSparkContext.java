package io.tiledb.vcf;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.Utils;
import org.junit.*;

public class SharedJavaSparkContext {

  private static transient SparkContext sparkContext;
  private static transient JavaSparkContext javaSparkContext;
  protected boolean inititalized = false;

  public String appID() {
    Long randomId = ((Double) Math.floor(Math.random() * 10E4)).longValue();
    return this.getClass().getName() + randomId.toString();
  }

  public SparkConf conf() {
    return new SparkConf()
        .setMaster("local[*]")
        .setAppName("test")
        .set("spark.ui.enabled", "false")
        .set("spark.app.id", appID())
        .set("spark.driver.host", "localhost");
  }

  public SparkContext sc() {
    return sparkContext;
  }

  public JavaSparkContext jsc() {
    return javaSparkContext;
  }

  public SparkContext setup(SparkContext sc) {
    sc.setCheckpointDir(
        Utils.createTempDir(System.getProperty("java.io.tmpdir"), "spark").toPath().toString());
    return sc;
  }

  public void runBeforeHook() {}

  @Before
  public void runBefore() {
    inititalized = (sparkContext != null);
    if (!inititalized) {
      sparkContext = setup(new SparkContext(conf()));
      javaSparkContext = new JavaSparkContext(sparkContext);
      runBeforeHook();
    }
  }

  @AfterClass
  public static void runAfterClass() {
    if (javaSparkContext != null) {
      javaSparkContext.stop();
    }
    sparkContext = null;
    javaSparkContext = null;
  }
}
