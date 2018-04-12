package org.apache.hadoop.security;
//package io.decisionlab;
/**
 * We need access to protected methods in UserGroupInformation, in order to determine
 * whether a Re-Login has been successful.
 */

import org.apache.log4j.Logger;

import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class KeytabRelogin {

    private final ScheduledExecutorService refresher;
    private volatile ScheduledFuture<?> renewal;

    protected static final Logger logger = Logger.getLogger(KeytabRelogin.class);


    public KeytabRelogin() {
        this.refresher = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Keytab login Globally to UserGroupInformation
     *
     * @param keytab
     * @param principal
     * @throws IOException
     */
    public void initKeytabLoginGlobally(File keytab, String principal) throws IOException {
        logger.info("Initializing Keytab Login globally ...");

        UserGroupInformation.loginUserFromKeytab(principal, keytab.getPath());

        logger.info("Initial UGI is configured to login from keytab? " + UserGroupInformation.isLoginKeytabBased());

        // the intial Login time always seems to be 0 here.
        // trying to do a checkTGTAndReloginFromKeytab() doesn't update it.

    }

    /**
     * Keytab login Locally to UserGroupInformation
     * You will need to reference the returned UGI in order to renew / relogin
     *
     * @param keytab
     * @param principal
     * @return
     * @throws IOException
     */
    public UserGroupInformation initKeytabLoginLocally(File keytab, String principal) throws IOException {
        logger.info("Initializing Keytab Login locally ...");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());

        logger.info("Initial UGI is configured to login from keytab? " + ugi.isFromKeytab());

        return ugi;

    }

    /**
     * Start a thread to periodically Relogin to Kerberos via UGI
     * http://richardstartin.uk/perpetual-kerberos-login-in-hadoop/
     *
     * @param requestTGTFrequencySeconds
     */
    public void startKeytabReloginThread(final UserGroupInformation ugi, long requestTGTFrequencySeconds) {
        this.renewal = refresher.scheduleWithFixedDelay(() -> {
            try {
                // log status before
                logKeytabStatus(ugi);

                // attempt a renewal / relogin
                logger.info("Calling ugi.checkTGTAndReloginFromKeytab() ...");
                ugi.checkTGTAndReloginFromKeytab();

                // log status after
                logKeytabStatus(ugi);

            } catch (Exception e) {
                // this is probably not good.  rethrow it so it doesn't get lost
                logger.error("Exception in Keytab Relogin Thread." , e);
                throw new RuntimeException(e);
            }
        }, requestTGTFrequencySeconds, requestTGTFrequencySeconds, TimeUnit.SECONDS);
    }

    /**
     * Start a thread to periodically Relogin to Kerberos via UGI
     * http://richardstartin.uk/perpetual-kerberos-login-in-hadoop/
     *
     * @param requestTGTFrequencySeconds
     */
    public void startKeytabReloginThread(long requestTGTFrequencySeconds) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();

        startKeytabReloginThread(ugi, requestTGTFrequencySeconds);
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
                logger.error("Exception in Ticket Cache Relogin Thread." , e);
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

        logKeytabStatus(newUgi);
    }

    /**
     * Log a bunch of information about our UGI status
     *
     * @throws IOException
     */
    private void logKeytabStatus(UserGroupInformation ugi) throws IOException {
        // These two methods to not indicate whether the Kerberos authentication is currently valid.
        // However, they do inidicate a pre-requisite for being able to Re-Login via keytab.
        logger.info("new UGI is configured to login from keytab? " + ugi.isFromKeytab());
        logger.info("new UGI has Kerberos Credentials? " + ugi.hasKerberosCredentials());

        // Log the (latest) Last Login time
        User user = ugi.getSubject().getPrincipals(User.class).iterator().next();
        logger.info("Latest Login: " + user.getLastLogin());
    }

    /**
     * This example logs in via the GSS-API (using JAAS), and then configures UGI for future relogin.
     * https://community.hortonworks.com/articles/56702/a-secure-hdfs-client-example.html
     *
     * @throws IOException
     * @throws LoginException
     */
    public void initJaasLogin(String principal) throws IOException, LoginException {
        logger.info("Initializing JAAS Login");

        // Pull the login (keytab + principal) from the JAAS config file
        LoginContext lc = kinit(principal);
        UserGroupInformation.loginUserFromSubject(lc.getSubject());

        // force set login time. make it easier to check Test pass/failure.
        // only doing the initial loginUserFromSubject() seems to leave the time at 0.
        // later, the first time we do a reloginFromTicketCache(), the time gets updated.
        // We need to be comparing that the initial time is less than the final time,
        // which can be difficult when the initial time is 0.
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
    private LoginContext kinit(String principal) throws LoginException {
        LoginContext lc = new LoginContext(this.getClass().getSimpleName(), new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for (Callback c : callbacks) {
                    //if(c instanceof )
                    if (c instanceof NameCallback)
                        ((NameCallback) c).setName(principal);
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
