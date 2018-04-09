package org.apache.hadoop.security;

import com.sun.security.auth.module.Krb5LoginModule;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.security.auth.login.AppConfigurationEntry;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class KeytabReloginTest {
    // 240 seconds seems to be the absolute minimum Ticket Lifetime supported.
    public static final int TICKET_LIFETIME_SECONDS = 240;
    public static final int RENEWABLE_LIFETIME_SECONDS = 250;
    public static final String ORG_NAME = "DECISIONLAB";
    public static final String ORG_DOMAIN = "IO";
    public static final String PRINCIPAL = "testUser";
    public static final String REALM = ORG_NAME + "." + ORG_DOMAIN;
    public static final String REALMED_PRINCIPAL = PRINCIPAL + "@" + REALM;
    public static final long REQUEST_TGT_FREQUENCY_SECONDS = 65;
    public static final long TEST_KEYTAB_WAIT_SECONDS = RENEWABLE_LIFETIME_SECONDS + REQUEST_TGT_FREQUENCY_SECONDS;
    public static final long TEST_JAAS_WAIT_SECONDS = 600 + REQUEST_TGT_FREQUENCY_SECONDS; // UserGroupInformation.MIN_TIME_BEFORE_RELOGIN = 600

    private MiniKdc kdc;
    private File workDir;

    protected final Logger logger = Logger.getLogger(this.getClass());


    @Before
    public void setUp() throws Exception {
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
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        UserGroupInformation.reset();
        UserGroupInformation.setConfiguration(conf);

        // This only affects the Keytab Logins.
        // The JAAS (Ticket Cache) logins are not affected by '.setShouldRenewImmediatelyForTests'
        UserGroupInformation.setShouldRenewImmediatelyForTests(true);
    }

    @After
    public void tearDown() throws Exception {
        // Close the MiniKDC VM
        if (null != kdc) {
            kdc.stop();
        }

        // make sure UGI is reset between tests
        UserGroupInformation.setLoginUser(null);
    }

    /**
     * In this test, UGI Keytab login was setup incorrectly.
     * The _expected_ value is that we did _not_ successfully relogin to Kerberos
     * @throws Exception
     */
    @Test
    public void testKeytabLoginIncorrectly() throws Exception {
        KeytabRelogin keytabRelogin = new KeytabRelogin();

        // setup Keytab
        File keytab = new File(workDir, "testUser.keytab");
        kdc.createPrincipal(keytab, PRINCIPAL);

        // perform initial keytab login
        keytabRelogin.initKeytabLoginIncorrectly(keytab, PRINCIPAL);

        // check value of (initial) Last Login
        long initialLogin = getUgiLastLogin();

        // start thread to perform periodic Re-Login
        keytabRelogin.startKeytabReloginThread(REQUEST_TGT_FREQUENCY_SECONDS);

        logger.info("Started renew thread, sleeping test until Renewable Lifetime has passed (" + TEST_KEYTAB_WAIT_SECONDS + " seconds) ...");
        Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_KEYTAB_WAIT_SECONDS));
        logger.info("Renewable Lifetime has passed.");

        // check value of (latest) Last Login
        long lastLogin = getUgiLastLogin();

        // In this test, UGI Keytab login was setup incorrectly.
        // The _expected_ value is that we did _not_ successfully relogin to Kerberos
        assertTrue("Kerberos TGT login time should not have been updated.", lastLogin == initialLogin);

        // need to stop the relogin thread before the next test
        keytabRelogin.stopRefreshing();
    }

    /**
     * In this test, our UGI Keytab login was setup correctly,
     * so we should be able to update the Login time past the Renewable Lifetime
     * @throws Exception
     */
    @Test
    public void testKeytabLoginCorrectly() throws Exception {

        KeytabRelogin keytabRelogin = new KeytabRelogin();

        // setup Keytab
        File keytab = new File(workDir, "testUser.keytab");
        kdc.createPrincipal(keytab, PRINCIPAL);

        // perform initial keytab login
        keytabRelogin.initKeytabLoginCorrectly(keytab, PRINCIPAL);

        // check value of (initial) Last Login
        long initialLogin = getUgiLastLogin();
        long maxRenewableLogin = getMaxRenewableLogin(initialLogin);

        // start thread to perform periodic Re-Login
        keytabRelogin.startKeytabReloginThread(REQUEST_TGT_FREQUENCY_SECONDS);

        logger.info("Started renew thread, sleeping test until Renewable Lifetime has passed (" +
                TEST_KEYTAB_WAIT_SECONDS + " seconds) ...");
        Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_KEYTAB_WAIT_SECONDS));
        logger.info("Renewable Lifetime has passed.");

        // check value of (latest) Last Login
        long lastLogin = getUgiLastLogin();

        // In this test, our UGI Keytab login was setup correctly,
        // so we should be able to update the Login time past the Renewable Lifetime
        assertTrue("Kerberos TGT login time was not updated.", lastLogin > initialLogin);
        assertTrue("Kerberos TGT login time was not updated past the Renewable Lifetime: " + maxRenewableLogin, lastLogin > maxRenewableLogin);

        // need to stop the relogin thread before the next test
        keytabRelogin.stopRefreshing();
    }

    /**
     * In this test, our JAAS (Keytab) login was setup correctly,
     * so we should be able to update the Login time past the Renewable Lifetime
     * @throws Exception
     */
    @Test
    public void testJaasLogin() throws Exception {
        KeytabRelogin keytabRelogin = new KeytabRelogin();

        // setup Keytab
        File keytab = new File(workDir, "testUser.keytab");
        kdc.createPrincipal(keytab, PRINCIPAL);

        // Should we just write out the jaas.conf file? or try to read one in?
        javax.security.auth.login.Configuration jaasConfig = createJaasConfig(keytab);
        javax.security.auth.login.Configuration.setConfiguration(jaasConfig);

        keytabRelogin.initJaasLogin();

        // check value of (initial) Last Login
        long initialLogin = getUgiLastLogin();
        long maxRenewableLogin = getMaxRenewableLogin(initialLogin);

        // start thread to perform periodic Re-Login
        keytabRelogin.startTicketCacheReloginThread(REQUEST_TGT_FREQUENCY_SECONDS);

        logger.info("Started renew thread, sleeping test until Renewable Lifetime has passed (" +
                TEST_JAAS_WAIT_SECONDS + " seconds) ...");
        Thread.sleep(TimeUnit.SECONDS.toMillis(TEST_JAAS_WAIT_SECONDS));
        logger.info("Renewable Lifetime has passed.");

        // check value of (latest) Last Login
        long lastLogin = getUgiLastLogin();

        // In this test, our JAAS (Keytab) login was setup correctly,
        // so we should be able to update the Login time past the Renewable Lifetime
        assertTrue("Kerberos TGT login time was not updated.", lastLogin > initialLogin);
        assertTrue("Kerberos TGT login time was not updated past the Renewable Lifetime: " + maxRenewableLogin, lastLogin > maxRenewableLogin);

        // need to stop the relogin thread before the next test
        keytabRelogin.stopRefreshing();

    }

    /**
     * Helper function to get LastLogin time from UGI
     * @return
     * @throws IOException
     */
    private long getUgiLastLogin() throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        return ugi.getSubject().getPrincipals(User.class).iterator().next().getLastLogin();
    }

    /**
     * get an approximation of the end Time of the Renewable Lifetime
     * @param initialLogin
     * @return
     */
    private long getMaxRenewableLogin(long initialLogin) {
        // initial time is often 0.  if so, set it to 'now'
        if (0 == initialLogin){
            initialLogin = ZonedDateTime.now().toInstant().toEpochMilli();
            logger.info("Using 'now' for our initial time: " + initialLogin);
        }

        // make sure the new ticket login is a ticket Relogin, and not just a Renewal
        long renewableLifetime = initialLogin + TimeUnit.SECONDS.toMillis(RENEWABLE_LIFETIME_SECONDS);
        logger.info("Using renewable lifetime indicator: " + renewableLifetime);

        return renewableLifetime;
    }

    /**
     * Setup JAAS configuration, without needing a JAAS.conf file, for testing purposes.
     * https://coderanch.com/t/134541/engineering/Setting-JAAS-Configuration-file-programmatically#3174490
     *
     * @param keytab
     * @return
     */
    private javax.security.auth.login.Configuration createJaasConfig(File keytab) {

        // Create entry options.
        final Map<String, Object> options = new HashMap<>();
        options.put("useFirstPass", "false");    // Do *not* use javax.security.auth.login.{name,password} from shared state.
        options.put("debug", "true");             // Output debug (including plain text username and password!) messages.

        // options usually found in jaas.conf file
        options.put("Krb5LoginModule", "required");
        options.put("doNotPrompt", "true");
        options.put("principal", REALMED_PRINCIPAL);
        options.put("useKeyTab", "true");
        options.put("keyTab", keytab.getPath());
        options.put("storeKey", "true");


        // Create entries.
        AppConfigurationEntry[] entries = {
                new AppConfigurationEntry(
                        Krb5LoginModule.class.getCanonicalName(),
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                        options)
        };

        // Create configuration.
        return new javax.security.auth.login.Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Using JAAS Config Name " + name);
                }

                return entries;
            }
        };

    }

}