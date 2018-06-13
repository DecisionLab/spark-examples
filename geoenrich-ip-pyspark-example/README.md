Enrich records by ip fields with MaxMind Data (Spark)

Using the [GeoLite2](https://dev.maxmind.com/geoip/geoip2/geolite2/) Free Downloadable database(s).

## Approach

Several approaches are possible for looking up the Geolocation information for an IP Address in Spark.
The method using `bisect` is generally accepted to be the most performant.
An example of this can be found in the following blog post:
[Geographic report with Apache Spark from Nginx access log](http://tranvictor.github.io/2015/03/13/geographic-report-with-apache-spark-from-nginx-access-log/).

Most of the code in this example replicate that method, but with modifications to support the data format provided by MaxMind / GeoLite2.

Alternative approaches:
1. The referenced blog post also shows how to use the Python `geoip.geolite2` module.
This does not perform well in Spark because the module import needs to occur for each record lookup.
1. Expand the input data to allow direct joins against every possible CIDR/netmask.
Direct joins between input data and the geolocation databases cannot be performed,
because the geolocation databases provide lookups at the network level.
It is infeasible to expand the database to provide geolocation lookups for every possible IP address.
Instead, the unknown IP address records can be replicated to provide all possible (32) networks that could include that IP address.
A direct (inner) join can then be performed between the possible network values and the geolocation database records.

At this time, the above approaches have not all been implemented for direct performance comparison with the MaxMind geolocation database.
It is recommended that any analytic that needs high performance should conduct it's own evaluation of the potential approaches.

Performance improvements could also be made by storing any modifications to the geolocation database to disk, instead of recomputing for each run of the analytic.

## Attribution

This product includes GeoLite2 data created by MaxMind, available from http://www.maxmind.com.
