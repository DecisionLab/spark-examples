import bisect
import os.path
from sys import argv

from ipaddr import IPv4Network, IPAddress
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

ipv4CountryFile = "GeoLite2-Country-Blocks-IPv4.csv"
countryNamesFile = "GeoLite2-Country-Locations-en.csv"
keys = []
ranges = []

def networkToIp (network):
    start = IPv4Network(network)[0]
    return int(start)
    #return network.split('/')[0]

def networkToEndIp (network):
    end = IPv4Network(network)[-1]
    return int(end)

def lookup (ip, r, k):
    i_ip = int(IPAddress(ip))
    index = bisect.bisect(k, i_ip)
    result = None
    if index > 0:
        index -= 1
        found_range = r[index]
        if found_range['ip_end'] > i_ip:
            result = found_range['geoname_id']
    return result

# geolocate IP addresses using 'bisect' and the MaxMind GeoLite2 database
if __name__ == "__main__":

    if (len(argv) < 3):
        quit()

    spark = SparkSession \
        .builder \
        .getOrCreate()

    # need path for all of the MaxMind Geo files
    geolitePath = argv[1]

    # Sample Test data
    schema = StructType([
        StructField("test_ip", StringType(), False)
    ])
    testDF = spark.createDataFrame(
        [('1.1.4.4',), ('8.8.8.8',)],
        schema
    )

    # network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider
    ipv4CountryDF = spark.read.csv(os.path.join(geolitePath, ipv4CountryFile),
                                   header=True
                                   )

    networkToIpUdf = udf(networkToIp, IntegerType())
    networkToEndIpUdf = udf(networkToEndIp, IntegerType())
    lookupUdf = udf(lookup, StringType())

    # add integer representations of start and end IP addresses for Network
    ipv4CountryDF = ipv4CountryDF\
        .withColumn('ip_start', networkToIpUdf(col('network')))\
        .withColumn('ip_end', networkToEndIpUdf(col('network')))

    # collect the keys and ranges to enable bisect
    ranges = ipv4CountryDF.collect()
    keys = ipv4CountryDF.select('ip_start').rdd.map(lambda n: n['ip_start']).collect()

    # lookup
    # This part is currently failing with an Exception:
    # `py4j.Py4JException: Method col([class java.util.ArrayList]) does not exist`
    testDF = testDF.withColumn('geoname_id', lookupUdf(col('test_ip'), ranges, keys))
    #testDF = testDF.withColumn('geoname_id', lookupUdf(testDF['test_ip'], ranges, keys))
    testDF.show()

    # geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name
    countryNamesDF = spark.read.csv(geolitePath + countryNamesFile,
                                    header=True
                                    )

