A sample project that demonstrates Kerberos _Re-Login_ (not _renewal_), by using the Hadoop project's in-VM Kerberos Controller for tests, MiniKDC.
Using MiniKDC allows one to use short Kerberos ticket lifetimes, in order to more effectively test _Re-Login_ after the maximum Renewable Lifetime has been reached.
## Build
This project builds using Maven:
- `mvn clean package`

The resulting jar file will be located in the `./target/` directory.

## Run
- `java -jar target/uber-hadoop-kerberos-relogin-0.1-SNAPSHOT.jar`


## Example Output
```
2018-04-06 10:56:49,873 INFO  MiniKdc - Configuration:
2018-04-06 10:56:49,873 INFO  MiniKdc - ---------------------------------------------------------------
2018-04-06 10:56:49,874 INFO  MiniKdc -   debug: false
2018-04-06 10:56:49,874 INFO  MiniKdc -   transport: TCP
2018-04-06 10:56:49,874 INFO  MiniKdc -   max.ticket.lifetime: 240000
2018-04-06 10:56:49,874 INFO  MiniKdc -   org.name: DECISIONLAB
2018-04-06 10:56:49,874 INFO  MiniKdc -   kdc.port: 0
2018-04-06 10:56:49,874 INFO  MiniKdc -   org.domain: IO
2018-04-06 10:56:49,874 INFO  MiniKdc -   max.renewable.lifetime: 250000
2018-04-06 10:56:49,874 INFO  MiniKdc -   instance: DefaultKrbServer
2018-04-06 10:56:49,874 INFO  MiniKdc -   kdc.bind.address: localhost
2018-04-06 10:56:49,874 INFO  MiniKdc - ---------------------------------------------------------------
2018-04-06 10:56:51,925 INFO  KdcServer - Kerberos service started.
2018-04-06 10:56:51,929 INFO  MiniKdc - MiniKdc listening at port: 41605
2018-04-06 10:56:51,930 INFO  MiniKdc - MiniKdc setting JVM krb5.conf to: /tmp/KeytabRelogin7731070387355688405/1523026609873/krb5.conf
2018-04-06 10:56:52,083 WARN  NativeCodeLoader - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
# [Break added]
2018-04-06 10:56:52,197 INFO  KeytabRelogin - Starting Test 1
2018-04-06 10:56:52,328 INFO  KeytabRelogin - UGI should be configured to login from keytab: true
2018-04-06 10:56:52,331 INFO  KeytabRelogin - new UGI should be configured to login from keytab: false
2018-04-06 10:56:52,331 INFO  KeytabRelogin - new UGI has Kerberos Credentials? false
2018-04-06 10:56:52,331 INFO  KeytabRelogin - First Login: 0
2018-04-06 11:01:07,331 INFO  KeytabRelogin - new UGI should be configured to login from keytab: false
2018-04-06 11:01:07,331 INFO  KeytabRelogin - new UGI has Kerberos Credentials? false
2018-04-06 11:01:07,331 INFO  KeytabRelogin - Attempting Relogin, from Test 2
2018-04-06 11:01:07,332 INFO  KeytabRelogin - new UGI should be configured to login from keytab: false
2018-04-06 11:01:07,332 INFO  KeytabRelogin - new UGI has Kerberos Credentials? false
2018-04-06 11:01:07,332 INFO  KeytabRelogin - Second Login: 0
2018-04-06 11:01:07,332 INFO  KeytabRelogin - Test 1 has completed.
# [Break added]
2018-04-06 11:01:07,332 INFO  KeytabRelogin - Starting Test 2
2018-04-06 11:01:07,343 INFO  KeytabRelogin - UGI should be configured to login from keytab: true
2018-04-06 11:01:07,343 INFO  KeytabRelogin - new UGI should be configured to login from keytab: true
2018-04-06 11:01:07,343 INFO  KeytabRelogin - new UGI has Kerberos Credentials? true
2018-04-06 11:01:07,343 INFO  KeytabRelogin - First Login: 0
2018-04-06 11:05:22,344 INFO  KeytabRelogin - new UGI should be configured to login from keytab: true
2018-04-06 11:05:22,344 INFO  KeytabRelogin - new UGI has Kerberos Credentials? true
2018-04-06 11:05:22,344 INFO  KeytabRelogin - Attempting Relogin, from Test 2
2018-04-06 11:05:22,363 INFO  KeytabRelogin - new UGI should be configured to login from keytab: true
2018-04-06 11:05:22,364 INFO  KeytabRelogin - new UGI has Kerberos Credentials? true
2018-04-06 11:05:22,364 INFO  KeytabRelogin - Second Login: 1523027122344
2018-04-06 11:05:22,364 INFO  KeytabRelogin - Test 2 has completed.
```
### Keytab login
The method `ugi.isFromKeytab()` indicates whether the User was logged in from a keytab file.

The method `ugi.hasKerberosCredentials()` indicates whether the user was logged in using Kerberos.

These two methods to *not* indicate whether the Kerberos authentication is _currently_ valid.  However, they do inidicate a pre-requisite for being able to _Re-Login_ via keytab.

### Valid and Current Kerberos Authentication
Hadoop's `UserGroupInformation` class does not seem to include a method of determining whether a Kerberos authentication is _currently_ valid.  
Most other examples seem to simply attempt access to Hadoop, with success or failure of the Hadoop interaction indicating active Kerberos authentication.

This project uses Last Login time to indicate that re-authentication (_Re-Login_) was successful.
Access to protected methods within `UserGroupInformation` is required, in order to retreive the `Subject` user of the `ugi` object:
`newUgi.getSubject().getPrincipals(User.class).iterator().next().getLastLogin()`

### Interpreting the Example Output
#### MiniKDC
The MiniKDC configuration section indicates the `max.ticket.lifetime` and `max.renewable.lifetime`, in milliseconds, currently 240 seconds (4 minutes) and 250 seconds (4:10), respectively.

#### Test 1
Test 1 does not call `UserGroupInformation.setLoginUser(ugi)`.
Note that new UserGroupInformation references indicate that `ugi.isFromKeytab()` and `ugi.hasKerberosCredentials()` are false.
Finally, note that the Second Login time remains `0`, indicating that _Re-Login_ was unsuccessful, even though no errors were reported.

#### Test 2
Test 2 _does_ call `UserGroupInformation.setLoginUser(ugi)`.
New UserGroupInformation references indicate that `ugi.isFromKeytab()` and `ugi.hasKerberosCredentials()` are true, as desired.
Finally, note that the Login time has been updated, to `1523027122344` (a value of milliseconds since the epoch, equivalent to `Fri Apr 06 2018 11:05:22.344` localtime).

In this example, initial login was at `11:01:07,343`.  
The renewable lifetime for that ticket would have ended after 250 seconds, at `11:05:17`.
_Re-Login_ was attempted at `11:05:22,344`, which matches the `getLastLogin()` time reported. 
## References
- Example code using MiniKdc: https://github.com/nucypher/hadoop-oss/blob/master/nucypher-hadoop-common/src/test/java/org/apache/hadoop/security/TestUGILoginFromKeytab.java
- MiniKdc code, for configuration options: https://github.com/c9n/hadoop/blob/master/hadoop-common-project/hadoop-minikdc/src/main/java/org/apache/hadoop/minikdc/MiniKdc.java
- Example code using `UserGroupInformation.setLoginUser(ugi)`: http://richardstartin.uk/perpetual-kerberos-login-in-hadoop/
- Compiling when using MiniKdc, using Maven. [Why can't maven find a bundle dependency?
](https://stackoverflow.com/a/20555114)
>The problem is that maven doesn't know what the type "bundle" is. So you need to add a plugin that defines it, namely the maven-bundle-plugin. Notice that you also need to set the extensions property to true. So the POM should have something like
>
```xml
<plugin>
      <groupId>org.apache.felix</groupId>
      <artifactId>maven-bundle-plugin</artifactId>
      <version>2.4.0</version>
      <extensions>true</extensions>
</plugin>
```
