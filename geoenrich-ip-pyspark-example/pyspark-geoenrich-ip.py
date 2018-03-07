import bisect
import csv
import os.path
from sys import argv

from ipaddr import IPv4Network, IPAddress
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, StringType

ipv4CountryFile = "GeoLite2-Country-Blocks-IPv4.csv"
countryNamesFile = "GeoLite2-Country-Locations-en.csv"
keys = []
ranges = []
ip_to_country_db = None
ip_to_country_keys = None


# use 'ipaddr' module to convert from CIDR network to Start IP
def networkToIp(network):
    start = IPv4Network(network)[0]
    return int(start)


# use 'ipaddr' module to convert from CIDR network to End IP
def networkToEndIp(network):
    end = IPv4Network(network)[-1]
    return int(end)


# Use bisect method to lookup IP address in geo database
def lookup(ip):
    # get data out of the broadcast variables
    r = ip_to_country_db.value
    k = ip_to_country_keys.value

    # convert IP String to Integer
    i_ip = int(IPAddress(ip))

    index = bisect.bisect(k, i_ip)
    result = None
    if index > 0:
        index -= 1
        found_range = r[index]
        if found_range[1] > i_ip:
            result = found_range[2]
        else:
            result = "wrong"
    else:
        result = "unknown"

    return result


# geolocate IP addresses using 'bisect' and the MaxMind GeoLite2 database
if __name__ == "__main__":

    if (len(argv) < 1):
        quit()

    # need path for all of the MaxMind Geo files
    geolitePath = argv[1]

    spark = SparkSession \
        .builder \
        .getOrCreate()

    # The bisect method works because the 'keys' and 'ranges' are sorted lists (sorted by start_ip)
    # network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider
    with open(os.path.join(geolitePath, ipv4CountryFile), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ip_start = networkToIp(row['network'])
            ip_end = networkToEndIp(row['network'])
            ranges.append((ip_start, ip_end, row['geoname_id']))
            keys.append(ip_start)

    # broadcast these lists for availability on all worker nodes.
    # need to be mindful of memory usage
    ip_to_country_db = spark.sparkContext.broadcast(ranges)
    ip_to_country_keys = spark.sparkContext.broadcast(keys)

    # Sample Test data
    schema = StructType([
        StructField("test_ip", StringType(), False)
    ])
    testDF = spark.createDataFrame(
        [('1.1.4.4',), ('8.8.8.8',)],
        schema
    )

    lookupUdf = udf(lookup, StringType())

    # lookup
    testDF = testDF.withColumn('geoname_id', lookupUdf(col('test_ip')))

    # geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name
    countryNamesDF = spark.read.csv(os.path.join(geolitePath, countryNamesFile),
                                    header=True
                                    )

    # join with Country Names to convert 'geoname_id' to human readable name
    testDF = testDF.join(countryNamesDF, testDF.geoname_id == countryNamesDF.geoname_id)
    testDF.show()
