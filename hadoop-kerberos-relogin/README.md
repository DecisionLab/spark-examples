A sample project that demonstrates Kerberos _Re-Login_ (not simple _renewal_), by using the Hadoop project's in-VM Kerberos Controller for tests, MiniKDC.
Using MiniKDC allows one to use short Kerberos ticket lifetimes, in order to more effectively test _Re-Login_ after the maximum Renewable Lifetime has been reached.
## Build
This project builds using Maven:
- `mvn clean package`

## Test
By default, the Tests are not run when compiling, since they take 20+ minutes to run to completion.
To run the Tests, and demonstrate functionality, run Maven with the `test` Profile enabled:
- `mvn clean package -Ptest`

## Methodology
The `KeytabRelogin` class demonstrates several methods of acquiring and renewing Kerberos logins, one of which is known to not work.
The JUnit tests provided in `KeytabReloginTest` exercise those methods, checking for the _expected_ behavior, using MiniKDC as the Kerberos controller.
(The test for the method that is known to not work will _pass_, which here indicates that Kerberos Relogin was _unsuccessful_, as expected.)

### Keytab Relogin
The method `UserGroupInformation.loginUserFromKeytabAndReturnUGI()` can be used to initiate Keytab login, by providing the Principal (user) and Keytab file.
This login can then be renewed using the method `checkTGTAndReloginFromKeytab()`.
Since the renewal is often handled by a separate thread that periodicially requests renewal, it is *required* to complete the UserGroupInformation configuration step of `UserGroupInformation.setLoginUser(ugi)` after the initial call to `loginUserFromKeytabAndReturnUGI()`.

### JAAS Relogin
It is possible to perform the Kerberos login using a JAAS configuration file, specifying the Keytab and Principal in this configuration file.
This method uses the GSS-API and a `LoginContext` for the initial Kerberos login, followed by `UserGroupInformation.loginUserFromSubject()`.
Subsequent renewals are achieved by `ugi.reloginFromTicketCache()`. 

## Verification

### Keytab login
The method `ugi.isFromKeytab()` indicates whether the User was logged in from a keytab file.

The method `ugi.hasKerberosCredentials()` indicates whether the user was logged in using Kerberos.

These two methods to *not* indicate whether the Kerberos authentication is _currently_ valid.  However, they do inidicate a pre-requisite for being able to _Re-Login_ via keytab.

### Valid and Current Kerberos Authentication
Hadoop's `UserGroupInformation` class does not seem to include a method of determining whether a Kerberos authentication is _currently_ valid.  
Most other examples seem to simply attempt access to Hadoop, with success or failure of the Hadoop interaction indicating active Kerberos authentication.

This project uses Last Login time to indicate that re-authentication (_Re-Login_) was successful.
Access to protected methods within `UserGroupInformation` is required, in order to retreive the `Subject` user of the `ugi` object:
`UserGroupInformation.getCurrentUser().getSubject().getPrincipals(User.class).iterator().next().getLastLogin()`

## References
- Example code using MiniKdc: https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/security/TestUGILoginFromKeytab.java
- MiniKdc code, for configuration options: https://github.com/apache/hadoop/blob/trunk/hadoop-common-project/hadoop-minikdc/src/main/java/org/apache/hadoop/minikdc/MiniKdc.java
- Example code using `UserGroupInformation.setLoginUser(ugi)`: http://richardstartin.uk/perpetual-kerberos-login-in-hadoop/
- Example code using JAAS login: https://community.hortonworks.com/articles/56702/a-secure-hdfs-client-example.html
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
