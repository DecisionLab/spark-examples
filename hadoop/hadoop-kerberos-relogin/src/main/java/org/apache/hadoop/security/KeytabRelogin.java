package org.apache.hadoop.security;
//package io.decisionlab;
/**
 * We need access to protected methods in UserGroupInformation, in order to determine
 * whether a Re-Login has been successful.
 */

import org.apache.log4j.Logger;

import javax.security.auth.callback.*;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class KeytabRelogin {

    public static final String PRINCIPAL = "testUser";

    private final ScheduledExecutorService refresher;
    private volatile ScheduledFuture<?> renewal;

    protected static final Logger logger = Logger.getLogger(KeytabRelogin.class);


    public KeytabRelogin() {
        this.refresher = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * This example of logging in from Keytab looks correct, but it's missing an important step.
     *
     * @param keytab
     * @param principal
     * @throws IOException
     */
    public void initKeytabLoginIncorrectly(File keytab, String principal) throws IOException {
        logger.info("Initializing Keytab Login, _without_ setting Login User ...");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());

        logger.info("Initial UGI is configured to login from keytab? " + ugi.isFromKeytab());

        // the intial Login time always seems to be 0 here.
        // trying to do a checkTGTAndReloginFromKeytab() doesn't update it.

    }

    /**
     * This example both logs in via UGI Keytab and sets the LoginUser in UGI.
     * Both steps are required.
     * @param keytab
     * @param principal
     * @throws IOException
     */
    public void initKeytabLoginCorrectly(File keytab, String principal) throws IOException {
        logger.info("Initializing Keytab Login, including setting Login User ...");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());
        UserGroupInformation.setLoginUser(ugi); // This is the magic!

        logger.info("Initial UGI is configured to login from keytab? " + ugi.isFromKeytab());

        // the intial Login time always seems to be 0 here.
        // trying to do a checkTGTAndReloginFromKeytab() doesn't update it.

    }

    /**
     * Start a thread to periodically Relogin to Kerberos via UGI
     * http://richardstartin.uk/perpetual-kerberos-login-in-hadoop/
     *
     * @param requestTGTFrequencySeconds
     */
    public void startKeytabReloginThread(long requestTGTFrequencySeconds) {
        this.renewal = refresher.scheduleWithFixedDelay(() -> {
            try {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                // log status before
                logKeytabStatus();

                // attempt a renewal / relogin
                logger.info("Calling ugi.checkTGTAndReloginFromKeytab() ...");
                ugi.checkTGTAndReloginFromKeytab();

                // log status after
                logKeytabStatus();

            } catch (Exception e) {
                // this is probably not good.  rethrow it so it doesn't get lost
                throw new RuntimeException(e);
            }
        }, requestTGTFrequencySeconds, requestTGTFrequencySeconds, TimeUnit.SECONDS);
    }

    /**
     * Start a thread to periodically Relogin to Kerberos via UGI.
     * http://richardstartin.uk/perpetual-kerberos-login-in-hadoop/
     *
     * @param requestTGTFrequencySeconds
     */
    public void startTicketCacheReloginThread(long requestTGTFrequencySeconds) {
        this.renewal = refresher.scheduleWithFixedDelay(() -> {
            try {
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

                // log status before
                logKeytabStatus();

                // attempt a renewal / relogin
                logger.info("Calling ugi.reloginFromTicketCache() ...");
                ugi.reloginFromTicketCache();

                // log status after
                logKeytabStatus();

            } catch (Exception e) {
                // this is probably not good.  rethrow it so it doesn't get lost
                throw new RuntimeException(e);
            }
        }, requestTGTFrequencySeconds, requestTGTFrequencySeconds, TimeUnit.SECONDS);
    }

    /**
     * Log a bunch of information about our UGI status
     *
     * @throws IOException
     */
    private void logKeytabStatus() throws IOException {
        // Check the UGI for the "current user"
        UserGroupInformation newUgi = UserGroupInformation.getCurrentUser();

        // These two methods to not indicate whether the Kerberos authentication is currently valid.
        // However, they do inidicate a pre-requisite for being able to Re-Login via keytab.
        logger.info("new UGI is configured to login from keytab? " + newUgi.isFromKeytab());
        logger.info("new UGI has Kerberos Credentials? " + newUgi.hasKerberosCredentials());

        // Log the (latest) Last Login time
        User user = newUgi.getSubject().getPrincipals(User.class).iterator().next();
        logger.info("Latest Login: " + user.getLastLogin());
    }

    /**
     * This example logs in via the GSS-API (using JAAS), and then configures UGI for future relogin.
     * https://community.hortonworks.com/articles/56702/a-secure-hdfs-client-example.html
     *
     * @throws IOException
     * @throws LoginException
     */
    public void initJaasLogin() throws IOException, LoginException {
        logger.info("Initializing JAAS Login");

        // Pull the login (keytab + principal) from the JAAS config file
        LoginContext lc = kinit();
        UserGroupInformation.loginUserFromSubject(lc.getSubject());

        // force set login time. make it easier to check Test pass/failure.
        // only doing the initial loginUserFromSubject() seems to leave the time at 0.
        // later, the first time we do a reloginFromTicketCache(), the time gets updated.
        // We need to be comparing that the initial time is less than the final time,
        // which can be difficult when the intial time is 0.
        UserGroupInformation.getCurrentUser().reloginFromTicketCache();
    }

    /**
     * Need to stop the renewal thread
     */
    public void stopRefreshing() {
        if (null != this.renewal) {
            this.renewal.cancel(true);
        }
    }

    /**
     * Use the GSS-API to do a `kinit`, using the JAAS config
     * https://community.hortonworks.com/articles/56702/a-secure-hdfs-client-example.html
     *
     * @return
     * @throws LoginException
     */
    private LoginContext kinit() throws LoginException {
        LoginContext lc = new LoginContext(this.getClass().getSimpleName(), new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback c : callbacks) {
                    //if(c instanceof )
                    if (c instanceof NameCallback)
                        ((NameCallback) c).setName(PRINCIPAL);
                    if (c instanceof PasswordCallback)
                        ((PasswordCallback) c).setPassword("".toCharArray()); // empty password -- use keytab ?
                }
            }
        });
        lc.login();
        return lc;
    }

    public static void main(String[] args) throws Exception {

    }
}
