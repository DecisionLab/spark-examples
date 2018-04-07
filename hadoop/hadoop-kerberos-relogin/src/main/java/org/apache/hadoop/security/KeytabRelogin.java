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

    public void initKeytabLoginIncorrectly(File keytab, String principal) throws IOException {
        logger.info("Initializing Keytab Login, _without_ setting Login User ...");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());

        logger.info("Initial UGI is configured to login from keytab? " + ugi.isFromKeytab());

    }

    public void initKeytabLoginCorrectly(File keytab, String principal) throws IOException {
        logger.info("Initializing Keytab Login, including setting Login User ...");

        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab.getPath());
        UserGroupInformation.setLoginUser(ugi); // This is the magic!

        logger.info("Initial UGI is configured to login from keytab? " + ugi.isFromKeytab());

    }

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
                //TODO: production code needs to handle this failure
                logger.error("TODO: Exception from UGI checkTGTAndReloginFromKeytab needs to be handled better", e);
            }
        }, requestTGTFrequencySeconds, requestTGTFrequencySeconds, TimeUnit.SECONDS);
    }



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
                //TODO: production code needs to handle this failure
                logger.error("TODO: Exception from UGI checkTGTAndReloginFromKeytab needs to be handled better", e);
            }
        }, requestTGTFrequencySeconds, requestTGTFrequencySeconds, TimeUnit.SECONDS);
    }

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

    public void initJaasLogin() throws IOException, LoginException {
        logger.info("Initializing JAAS Login");

        // Pull the login (keytab + principal) from the JAAS config file
        LoginContext lc = kinit();
        UserGroupInformation.loginUserFromSubject(lc.getSubject());

        // force set login time. make it easier to check Test pass/failure.
        UserGroupInformation.getCurrentUser().reloginFromTicketCache();

        // If we could get the Path of the Keytab, we could configure UGI for keytab login
        // But it doesn't seem to be possible
        Set<KeyTab> keyTabs = lc.getSubject().getPrivateCredentials(KeyTab.class);
        for (KeyTab keyTab : keyTabs) {
            logger.info("KeyTab! " + keyTab.toString());
        }
        //UserGroupInformation.loginUserFromKeytab(keyTabs); // NOPE!
    }

    private LoginContext kinit() throws LoginException {
        LoginContext lc = new LoginContext(this.getClass().getSimpleName(), new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                for(Callback c : callbacks){
                    //if(c instanceof )
                    if(c instanceof NameCallback)
                        ((NameCallback) c).setName(PRINCIPAL);
                    if(c instanceof PasswordCallback)
                        ((PasswordCallback) c).setPassword("".toCharArray()); // empty password -- use keytab ?
                }
            }});
        lc.login();
        return lc;
    }

    public static void main(String[] args) throws Exception {

    }
}
