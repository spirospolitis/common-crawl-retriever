import logging

def get_spark_logger(spark_session, name):
    log4j = spark_session.sparkContext._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(name)

    return logger