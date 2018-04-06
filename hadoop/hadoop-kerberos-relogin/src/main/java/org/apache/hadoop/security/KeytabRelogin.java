package org.apache.hadoop.security;
//package io.decisionlab;
/**
 * We need access to protected methods in UserGroupInformation, in order to determine
 * whether a Re-Login has been successful.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KeytabRelogin {

    // 240 seconds seems to be the absolute minimum Ticket Lifetime supported.
    public static final int TICKET_LIFETIME_SECONDS = 240;
    public static final int RENEWABLE_LIFETIME_SECONDS = 250;
    public static final String ORG_NAME = "DECISIONLAB";
    public static final String ORG_DOMAIN = "IO";

    private MiniKdc kdc;
    private File workDir;

    protected final Logger logger = Logger.getLogger(this.getClass());


    public KeytabRelogin() throws Exception {

        final Path folder = Files.createTempDirectory(this.getClass().getSimpleName());
        workDir = folder.toFile();

        // Want very short ticket lifetimes to test Re-Logins
        Properties kdcConf = MiniKdc.createConf();
        kdcConf.setProperty(MiniKdc.MAX_TICKET_LIFETIME,
                String.valueOf(TimeUnit.SECONDS.toMillis(TICKET_LIFETIME_SECONDS)));
        kdcConf.setProperty(MiniKdc.MAX_RENEWABLE_LIFETIME,
                String.valueOf(TimeUnit.SECONDS.toMillis(RENEWABLE_LIFETIME_SECONDS)));
        kdcConf.setProperty(MiniKdc.ORG_DOMAIN, ORG_DOMAIN);
        kdcConf.setProperty(MiniKdc.ORG_NAME, ORG_NAME);

        kdc = new MiniKdc(kdcConf, workDir);
        kdc.start();

        // Need to do this After the MiniKdc setup, in order to have working krb5.conf file
        // setup Hadoop for Kerberos
        // This is required, otherwise UGI will abort any attempt to loginUserFromKeytab
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.setConfiguration(conf);

    }

    public void testUGILoginFromKeytab1(File keytab, String principal) throws IOException {
        logger.info("Starting Test 1");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());

        logger.info("UGI should be configured to login from keytab: " + ugi.isFromKeytab());

        // Check the UGI for the "current user"
        initialKeytabStatus();

        // test renewal
        sleepAndTestRelogin();

        logger.info("Test 1 has completed.");
    }

    public void testUGILoginFromKeytab2(File keytab, String principal) throws IOException {
        logger.info("Starting Test 2");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());
        UserGroupInformation.setLoginUser(ugi); // This is the magic!

        logger.info("UGI should be configured to login from keytab: " + ugi.isFromKeytab());

        // Check the UGI for the "current user"
        initialKeytabStatus();

        // test renewal
        sleepAndTestRelogin();


        logger.info("Test 2 has completed.");
    }

    private void initialKeytabStatus() throws IOException {
        // Check the UGI for the "current user"
        UserGroupInformation newUgi = UserGroupInformation.getCurrentUser();

        // These two methods to not indicate whether the Kerberos authentication is currently valid.
        // However, they do inidicate a pre-requisite for being able to Re-Login via keytab.
        logger.info("new UGI should be configured to login from keytab: " + newUgi.isFromKeytab());
        logger.info("new UGI has Kerberos Credentials? " + newUgi.hasKerberosCredentials());

        User user = newUgi.getSubject().getPrincipals(User.class).iterator().next();
        final long firstLogin = user.getLastLogin();
        logger.info("First Login: " + firstLogin);
    }

    private void sleepAndTestRelogin() throws IOException {
        UserGroupInformation newUgi = UserGroupInformation.getCurrentUser();
        User user = newUgi.getSubject().getPrincipals(User.class).iterator().next();

        // test renewal

        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(RENEWABLE_LIFETIME_SECONDS + 5));

            logger.info("new UGI should be configured to login from keytab: " + newUgi.isFromKeytab());
            logger.info("new UGI has Kerberos Credentials? " + newUgi.hasKerberosCredentials());

            logger.info("Attempting Relogin, from Test 2");
            newUgi.checkTGTAndReloginFromKeytab();

            logger.info("new UGI should be configured to login from keytab: " + newUgi.isFromKeytab());
            logger.info("new UGI has Kerberos Credentials? " + newUgi.hasKerberosCredentials());

            // Updated Last Login time indicates that Re-Login was successful
            final long secondLogin = user.getLastLogin();

            logger.info("Second Login: " + secondLogin);

        } catch (InterruptedException e) {
            logger.warn("Interrupted, in Test 2", e);
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        KeytabRelogin keytabRelogin = new KeytabRelogin();

        // setup Keytab
        String principal = "testUser";
        File keytab = new File(keytabRelogin.workDir, "testUser.keytab");
        keytabRelogin.kdc.createPrincipal(keytab, principal);

        keytabRelogin.testUGILoginFromKeytab1(keytab, principal);
        keytabRelogin.testUGILoginFromKeytab2(keytab, principal);


    }
}
