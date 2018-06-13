
Most of this was shamelessly stolen from (https://qbox.io/blog/elasticsearch-in-apache-spark-python)

Need to download the elasticsearch-hadoop adapter from (https://www.elastic.co/downloads/hadoop)
- Download the package (zip file) that matches your ElasticSearch cluster.  Then use the jar that matches your Spark installation (spark-20_2.11).
- What's the difference between the `elasticsearch-hadoop` and the `elasticsearch-spark` jars?  Spark seems to return results more quickly when using the hadoop jars.
The [docs](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/install.html) suggest the `-spark` jar is simply more minimal, so that seems odd.

Launch pyspark shell:
```
/usr/hdp/current/spark2-client/bin/pyspark --jars  elasticsearch-hadoop-5.5.1/dist/elasticsearch-spark-20_2.11-5.5.1.jar
```

Or use spark-submit:
```
/usr/hdp/current/spark2-client/bin/spark-submit --master yarn --jars elasticsearch-hadoop-5.5.1/dist/elasticsearch-spark-20_2.11-5.5.1.jar pyspark_elasticsearch.py
```


Use curl to verify that new elasticsearch index was created:
```
curl -XPOST -u elastic:changeme 'localhost:9200/titanic/value_counts/_search?pretty' -d '
{
    "query": {
        "match_all": {}
    }
}'

```