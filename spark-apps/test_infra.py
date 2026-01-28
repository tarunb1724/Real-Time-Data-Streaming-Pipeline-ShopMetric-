from pyspark.sql import SparkSession

def test_infrastructure():
    print("--- STARTING INFRASTRUCTURE TEST ---")
    
    # NOTE: The path is now /opt/spark/user-jars/
    spark = SparkSession.builder \
        .appName("InfrastructureTest") \
        .config("spark.jars", "/opt/spark/user-jars/hadoop-aws-3.3.4.jar,/opt/spark/user-jars/aws-java-sdk-bundle-1.12.262.jar") \
        .getOrCreate()

    print("1. Spark Session Created!")
    
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
    df = spark.createDataFrame(data, ["Language", "Users"])
    df.show()
    
    print("--- INFRASTRUCTURE TEST PASSED ---")
    spark.stop()

if __name__ == "__main__":
    test_infrastructure()
    