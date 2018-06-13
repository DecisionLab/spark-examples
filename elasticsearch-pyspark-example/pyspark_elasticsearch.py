from pyspark import SparkContext
from pyspark.sql import SparkSession

# mostly stolen from https://qbox.io/blog/elasticsearch-in-apache-spark-python
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .getOrCreate()

    sc = spark.sparkContext

    # https://www.elastic.co/guide/en/elasticsearch/hadoop/5.5/configuration.html
    titanic_es_conf = {
        "es.nodes": "localhost",
        "es.port": "9200",
        "es.resource.read": "titanic/passenger",  # "aws-logins", # <index>/<type>
        "es.resource.write": "titanic/value_counts",
        "es.net.http.auth.user": "elastic",
        "es.net.http.auth.pass": "changeme"
    }

    assert isinstance(sc, SparkContext)
    passengers_rdd = sc.newAPIHadoopRDD(
        inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=titanic_es_conf
    )

    print(passengers_rdd.first())

    # for every field (?) count the unique values, and write those with a count greater than 1 to new index in ES.
    doc = passengers_rdd.first()[1]
    for field in doc:
        value_counts = passengers_rdd.map(lambda item: item[1][field])
        value_counts = value_counts.map(lambda word: (word, 1))
        value_counts = value_counts.reduceByKey(lambda a, b: a + b)
        value_counts = value_counts.filter(lambda item: item[1] > 1)
        value_counts = value_counts.map(lambda item: ('key', {
            'field': field,
            'val': item[0],
            'count': item[1]
        }))
        value_counts.saveAsNewAPIHadoopFile(
            path='-',
            outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf=titanic_es_conf)
