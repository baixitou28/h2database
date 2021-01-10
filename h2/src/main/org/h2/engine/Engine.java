/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.command.dml.SetTypes;
import org.h2.message.DbException;
import org.h2.message.Trace;
import org.h2.security.auth.AuthenticationException;
import org.h2.security.auth.AuthenticationInfo;
import org.h2.security.auth.Authenticator;
import org.h2.store.fs.FileUtils;
import org.h2.util.DateTimeUtils;
import org.h2.util.MathUtils;
import org.h2.util.ParserUtil;
import org.h2.util.StringUtils;
import org.h2.util.ThreadDeadlockDetector;
import org.h2.util.TimeZoneProvider;
import org.h2.util.Utils;

/**
 * The engine contains a map of all open databases.
 * It is also responsible for opening and creating new databases.
 * This is a singleton class.
 */
public final class Engine {//TODO: TIGER 理解Database大体机制

    private static final Map<String, Database> DATABASES = new HashMap<>();

    private static volatile long WRONG_PASSWORD_DELAY = SysProperties.DELAY_WRONG_PASSWORD_MIN;

    private static boolean JMX;

    static {
        if (SysProperties.THREAD_DEADLOCK_DETECTOR) {
            ThreadDeadlockDetector.init();
        }
    }

    private static SessionLocal openSession(ConnectionInfo ci, boolean ifExists, boolean forbidCreation,
            String cipher) {
        String name = ci.getName();
        Database database;
        ci.removeProperty("NO_UPGRADE", false);
        boolean openNew = ci.getProperty("OPEN_NEW", false);
        boolean opened = false;
        User user = null;
        synchronized (DATABASES) {
            if (openNew || ci.isUnnamedInMemory()) {
                database = null;
            } else {
                database = DATABASES.get(name);
            }
            if (database == null) {
                if (ci.isPersistent()) {
                    String p = ci.getProperty("MV_STORE");
                    String fileName;
                    if (p == null) {//如果是mv
                        fileName = name + Constants.SUFFIX_MV_FILE;
                        if (!FileUtils.exists(fileName)) {
                            fileName = name + Constants.SUFFIX_PAGE_FILE;
                            if (FileUtils.exists(fileName)) {
                                ci.setProperty("MV_STORE", "false");
                            } else {
                                throwNotFound(ifExists, forbidCreation, name);
                                fileName = name + Constants.SUFFIX_OLD_DATABASE_FILE;
                                if (FileUtils.exists(fileName)) {
                                    throw DbException.getFileVersionError(fileName);
                                }
                                fileName = null;
                            }
                        }
                    } else {//如果不是mv
                        fileName = name + (Utils.parseBoolean(p, true, false) ? Constants.SUFFIX_MV_FILE
                                : Constants.SUFFIX_PAGE_FILE);
                        if (!FileUtils.exists(fileName)) {
                            throwNotFound(ifExists, forbidCreation, name);
                            fileName = null;
                        }
                    }
                    if (fileName != null && !FileUtils.canWrite(fileName)) {
                        ci.setProperty("ACCESS_MODE_DATA", "r");
                    }
                } else {
                    throwNotFound(ifExists, forbidCreation, name);//找不到
                }
                database = new Database(ci, cipher);//tiger 关键结构
                opened = true;
                checkUserExists: {//如果用户存在
                    for (RightOwner rightOwner : database.getAllUsersAndRoles()) {
                        if (rightOwner instanceof User) {//如果
                            break checkUserExists;
                        }
                    }
                    // users is the last thing we add, so if no user is around,
                    // the database is new (or not initialized correctly)
                    user = new User(database, database.allocateObjectId(), ci.getUserName(), false);//第一次启动创建管理员用户，否则无法进行后续操作
                    user.setAdmin(true);
                    user.setUserPasswordHash(ci.getUserPasswordHash());
                    database.setMasterUser(user);
                }
                if (!ci.isUnnamedInMemory()) {
                    DATABASES.put(name, database);
                }
            }
        }
        if (opened) {
            // start the thread when already synchronizing on the database
            // otherwise a deadlock can occur when the writer thread
            // opens a new database (as in recovery testing)
            database.opened();
        }
        if (database.isClosing()) {
            return null;
        }
        if (user == null) {
            if (database.validateFilePasswordHash(cipher, ci.getFilePasswordHash())) {//验证
                if (ci.getProperty("AUTHREALM")== null) {
                    user = database.findUser(ci.getUserName());
                    if (user != null) {
                        if (!user.validateUserPasswordHash(ci.getUserPasswordHash())) {
                            user = null;
                        }
                    }
                } else {
                    Authenticator authenticator = database.getAuthenticator();
                    if (authenticator==null) {
                        throw DbException.get(ErrorCode.AUTHENTICATOR_NOT_AVAILABLE, name);
                    } else {
                        try {
                            AuthenticationInfo authenticationInfo=new AuthenticationInfo(ci);
                            user = database.getAuthenticator().authenticate(authenticationInfo, database);
                        } catch (AuthenticationException authenticationError) {
                            database.getTrace(Trace.DATABASE).error(authenticationError,
                                "an error occurred during authentication; user: \"" +
                                ci.getUserName() + "\"");
                        }
                    }
                }
            }
            if (opened && (user == null || !user.isAdmin())) {
                // reset - because the user is not an admin, and has no
                // right to listen to exceptions
                database.setEventListener(null);//tiger learn admin 还有特殊的功能
            }
        }
        if (user == null) {//找不到用户
            DbException er = DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            database.getTrace(Trace.DATABASE).error(er, "wrong user or password; user: \"" +
                    ci.getUserName() + "\"");
            database.removeSession(null);
            throw er;
        }
        //Prevent to set _PASSWORD
        ci.cleanAuthenticationInfo();//避免用户信息泄露
        checkClustering(ci, database);
        SessionLocal session = database.createSession(user, ci.getNetworkConnectionInfo());//创建session
        if (session == null) {
            // concurrently closing
            return null;
        }
        if (ci.getProperty("OLD_INFORMATION_SCHEMA", false)) {
            session.setOldInformationSchema(true);
        }
        if (ci.getProperty("JMX", false)) {
            try {
                Utils.callStaticMethod(
                        "org.h2.jmx.DatabaseInfo.registerMBean", ci, database);
            } catch (Exception e) {
                database.removeSession(session);
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
            JMX = true;
        }
        return session;
    }

    private static void throwNotFound(boolean ifExists, boolean forbidCreation, String name) {
        if (ifExists) {
            throw DbException.get(ErrorCode.DATABASE_NOT_FOUND_WITH_IF_EXISTS_1, name);
        }
        if (forbidCreation) {
            throw DbException.get(ErrorCode.REMOTE_DATABASE_NOT_FOUND_1, name);
        }
    }

    /**
     * Open a database connection with the given connection information.
     *
     * @param ci the connection information
     * @return the session
     */
    public static SessionLocal createSession(ConnectionInfo ci) {
        try {
            SessionLocal session = openSession(ci);//打开session
            validateUserAndPassword(true);//密码验证后，是否delay
            return session;
        } catch (DbException e) {
            if (e.getErrorCode() == ErrorCode.WRONG_USER_OR_PASSWORD) {
                validateUserAndPassword(false);
            }
            throw e;
        }
    }

    private static synchronized SessionLocal openSession(ConnectionInfo ci) {
        boolean ifExists = ci.removeProperty("IFEXISTS", false);//删除历史信息
        boolean forbidCreation = ci.removeProperty("FORBID_CREATION", false);
        boolean ignoreUnknownSetting = ci.removeProperty(
                "IGNORE_UNKNOWN_SETTINGS", false);
        String cipher = ci.removeProperty("CIPHER", null);
        String init = ci.removeProperty("INIT", null);
        SessionLocal session;
        long start = System.nanoTime();
        for (;;) {//反复尝试
            session = openSession(ci, ifExists, forbidCreation, cipher);
            if (session != null) {
                break;
            }
            // we found a database that is currently closing
            // wait a bit to avoid a busy loop (the method is synchronized)
            if (System.nanoTime() - start > DateTimeUtils.NANOS_PER_MINUTE) {//tiger
                throw DbException.get(ErrorCode.DATABASE_ALREADY_OPEN_1,
                        "Waited for database closing longer than 1 minute");
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw DbException.get(ErrorCode.DATABASE_CALLED_AT_SHUTDOWN);
            }
        }
        synchronized (session) {
            session.setAllowLiterals(true);
            DbSettings defaultSettings = DbSettings.DEFAULT;
            for (String setting : ci.getKeys()) {
                if (defaultSettings.containsKey(setting)) {
                    // database setting are only used when opening the database
                    continue;
                }
                String value = ci.getProperty(setting);
                StringBuilder builder = new StringBuilder("SET ").append(setting).append(' ');
                if (!ParserUtil.isSimpleIdentifier(setting, false, false)) {
                    if (!setting.equalsIgnoreCase("TIME ZONE")) {
                        throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, setting);
                    }
                    StringUtils.quoteStringSQL(builder, value);
                } else {
                    builder.append(value);
                }
                try {
                    CommandInterface command = session.prepareLocal(builder.toString());
                    command.executeUpdate(null);
                } catch (DbException e) {
                    if (e.getErrorCode() == ErrorCode.ADMIN_RIGHTS_REQUIRED) {
                        session.getTrace().error(e, "admin rights required; user: \"" +
                                ci.getUserName() + "\"");
                    } else {
                        session.getTrace().error(e, "");
                    }
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
            TimeZoneProvider timeZone = ci.getTimeZone();
            if (timeZone != null) {
                session.setTimeZone(timeZone);
            }
            if (init != null) {
                try {
                    CommandInterface command = session.prepareLocal(init);
                    command.executeUpdate(null);
                } catch (DbException e) {
                    if (!ignoreUnknownSetting) {
                        session.close();
                        throw e;
                    }
                }
            }
            session.setAllowLiterals(false);
            session.commit(true);
        }
        return session;
    }

    private static void checkClustering(ConnectionInfo ci, Database database) {
        String clusterSession = ci.getProperty(SetTypes.CLUSTER, null);
        if (Constants.CLUSTERING_DISABLED.equals(clusterSession)) {
            // in this case, no checking is made
            // (so that a connection can be made to disable/change clustering)
            return;
        }
        String clusterDb = database.getCluster();
        if (!Constants.CLUSTERING_DISABLED.equals(clusterDb)) {
            if (!Constants.CLUSTERING_ENABLED.equals(clusterSession)) {
                if (!Objects.equals(clusterSession, clusterDb)) {
                    if (clusterDb.equals(Constants.CLUSTERING_DISABLED)) {
                        throw DbException.get(
                                ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_ALONE);
                    }
                    throw DbException.get(
                            ErrorCode.CLUSTER_ERROR_DATABASE_RUNS_CLUSTERED_1,
                            clusterDb);
                }
            }
        }
    }

    /**
     * Called after a database has been closed, to remove the object from the
     * list of open databases.
     *
     * @param name the database name
     */
    static void close(String name) {
        if (JMX) {
            try {
                Utils.callStaticMethod("org.h2.jmx.DatabaseInfo.unregisterMBean", name);
            } catch (Exception e) {
                throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, e, "JMX");
            }
        }
        synchronized (DATABASES) {
            DATABASES.remove(name);
        }
    }

    /**
     * This method is called after validating user name and password. If user
     * name and password were correct, the sleep time is reset, otherwise this
     * method waits some time (to make brute force / rainbow table attacks
     * harder) and then throws a 'wrong user or password' exception. The delay
     * is a bit randomized to protect against timing attacks. Also the delay
     * doubles after each unsuccessful logins, to make brute force attacks
     * harder.
     *
     * There is only one exception message both for wrong user and for
     * wrong password, to make it harder to get the list of user names. This
     * method must only be called from one place, so it is not possible from the
     * stack trace to see if the user name was wrong or the password.
     *
     * @param correct if the user name or the password was correct
     * @throws DbException the exception 'wrong user or password'
     */
    private static void validateUserAndPassword(boolean correct) {//tiger learn 看注释
        int min = SysProperties.DELAY_WRONG_PASSWORD_MIN;
        if (correct) {
            long delay = WRONG_PASSWORD_DELAY;//WRONG_PASSWORD_DELAY时间随着攻击增加，会不停增加，导致delay增加
            if (delay > min && delay > 0) {//如果小于DELAY_WRONG_PASSWORD_MIN 就不会产生延时了
                // the first correct password must be blocked,
                // otherwise parallel attacks are possible
                synchronized (Engine.class) {//第一次一定要延时
                    // delay up to the last delay
                    // an attacker can't know how long it will be
                    delay = MathUtils.secureRandomInt((int) delay);//随机时间
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    WRONG_PASSWORD_DELAY = min;
                }
            }
        } else {
            // this method is not synchronized on the Engine, so that
            // regular successful attempts are not blocked
            synchronized (Engine.class) {
                long delay = WRONG_PASSWORD_DELAY;
                int max = SysProperties.DELAY_WRONG_PASSWORD_MAX;
                if (max <= 0) {
                    max = Integer.MAX_VALUE;//不能太大
                }
                WRONG_PASSWORD_DELAY += WRONG_PASSWORD_DELAY;//加倍，//这样等于所有的延时都增加，
                if (WRONG_PASSWORD_DELAY > max || WRONG_PASSWORD_DELAY < 0) {
                    WRONG_PASSWORD_DELAY = max;
                }
                if (min > 0) {
                    // a bit more to protect against timing attacks
                    delay += Math.abs(MathUtils.secureRandomLong() % 100);//100的余数，这样最大可能有1分半多了
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }
                throw DbException.get(ErrorCode.WRONG_USER_OR_PASSWORD);
            }
        }
    }

    private Engine() {
    }

}
