/*
 * NorthRidge Software, LLC - Copyright (c) 2015.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nridge.core.app.mgr;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.NSException;
import org.apache.commons.cli.*;
import org.apache.commons.configuration.*;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Application Manager provides a number of services to a parent application
 * while simplifying the interfaces needed to use them.  The list of services
 * provided include:
 * <ul>
 *     <li>Command line processing</li>
 *     <li>Configuration property management</li>
 *     <li>Robust logging facility</li>
 *     <li>Task (a.k.a. thread) management</li>
 * </ul>
 * <p>
 * Much of this framework was designed around the application manager to ensure
 * consistency and code reuse.
 * </p>
 *
 * @see <a href="http://logback.qos.ch/">Logback Project</a>
 * @see <a href="http://commons.apache.org/cli/index.html">Apache Commons CLI</a>
 * @see <a href="http://commons.apache.org/configuration/">Apache Commons Configuration</a>
 * @see <a href="http://www.ibm.com/developerworks/library/j-jtp05236/">Dealing with InterruptedException</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class AppMgr
{
    public final String LOGMSG_TRACE_ENTER = "Enter";
    public final String LOGMSG_TRACE_DEPART = "Depart";

    public final String CMDARG_RUNALL_TASKS = "all";
    public final String CMDARG_TESTALL_TASKS = "all";

    public final String LOG_PROPERTY_FILE_NAME = "logback.xml";
    public final String APP_PROPERTY_FILE_NAME = "application.properties";

    public final String APP_PROPERTY_DS_PATH = "app.ds_path";
    public final String APP_PROPERTY_INS_PATH = "app.ins_path";
    public final String APP_PROPERTY_CFG_PATH = "app.cfg_path";
    public final String APP_PROPERTY_LOG_PATH = "app.log_path";
    public final String APP_PROPERTY_RDB_PATH = "app.rdb_path";
    public final String APP_PROPERTY_GDB_PATH = "app.gdb_path";

    private CommandLine mCmdLine;
    private AtomicBoolean mIsAlive;
    private boolean mIsPathsExplicit;
    private ArrayList<Task> mTaskList;
    private boolean mIsAbortHandlerEnabled;
    private HashMap<String, Object> mPropertyMap;
    private CompositeConfiguration mConfiguration;
    private String mInsPathName, mCfgPathName, mLogPathName;
    private String mDSPathName, mRDBMSPathName, mGraphPathName;

    /**
     * Default constructor.
     */
    public AppMgr()
    {
        mIsPathsExplicit = false;
        mIsAlive = new AtomicBoolean(true);
        mPropertyMap = new HashMap<String, Object>();
        mInsPathName = System.getProperty("user.dir");
        mCfgPathName = String.format("%s%ccfg", mInsPathName, File.separatorChar);
        mLogPathName = String.format("%s%clog", mInsPathName, File.separatorChar);
        mDSPathName = String.format("%s%cds", mInsPathName, File.separatorChar);
        mRDBMSPathName = String.format("%s%crdb", mInsPathName, File.separatorChar);
        mGraphPathName = String.format("%s%cgdb", mInsPathName, File.separatorChar);
    }

    /**
     * Constructor suitable for use within Servlet Container where the default
     * paths for the application are difficult to derive automatically.
     *
     * @param aInsPathName Installation path name.
     * @param aCfgPathName Configuration path name.
     * @param aLogPathName Log file output path name.
     * @param aDSPathName Data source path name.
     */
    public AppMgr(String aInsPathName, String aCfgPathName,
                  String aLogPathName, String aDSPathName)
    {
        mIsPathsExplicit = true;
        mIsAlive = new AtomicBoolean(true);
        mPropertyMap = new HashMap<String, Object>();
        if (StringUtils.isNotEmpty(aInsPathName))
            mInsPathName = aInsPathName;
        else
            mInsPathName = System.getProperty("user.dir");
        if (StringUtils.isNotEmpty(aCfgPathName))
            mCfgPathName = aCfgPathName;
        else
            mCfgPathName = String.format("%s%ccfg", mInsPathName, File.separatorChar);
        if (StringUtils.isNotEmpty(aLogPathName))
            mLogPathName = aLogPathName;
        else
            mLogPathName = String.format("%s%clog", mInsPathName, File.separatorChar);
        if (StringUtils.isNotEmpty(aDSPathName))
            mDSPathName = aDSPathName;
        else
            mDSPathName = String.format("%s%cds", mInsPathName, File.separatorChar);
        mRDBMSPathName = String.format("%s%crdb", mInsPathName, File.separatorChar);
        mGraphPathName = String.format("%s%cgdb", mInsPathName, File.separatorChar);
    }

    /**
     * Constructor suitable for use within Servlet Container where the default
     * paths for the application are difficult to derive automatically.
     *
     * @param aInsPathName Installation path name.
     * @param aCfgPathName Configuration path name.
     * @param aLogPathName Log file output path name.
     * @param aDSPathName Data source path name.
     * @param anRDBMSPathName RDBMS path name.
     * @param aGraphPathName Graph path name.
     */
    public AppMgr(String aInsPathName, String aCfgPathName,
                  String aLogPathName, String aDSPathName,
                  String anRDBMSPathName, String aGraphPathName)
    {
        mIsPathsExplicit = true;
        mIsAlive = new AtomicBoolean(true);
        mPropertyMap = new HashMap<String, Object>();
        if (StringUtils.isNotEmpty(aInsPathName))
            mInsPathName = aInsPathName;
        else
            mInsPathName = System.getProperty("user.dir");
        if (StringUtils.isNotEmpty(aCfgPathName))
            mCfgPathName = aCfgPathName;
        else
            mCfgPathName = String.format("%s%ccfg", mInsPathName, File.separatorChar);
        if (StringUtils.isNotEmpty(aLogPathName))
            mLogPathName = aLogPathName;
        else
            mLogPathName = String.format("%s%clog", mInsPathName, File.separatorChar);
        if (StringUtils.isNotEmpty(aDSPathName))
            mDSPathName = aDSPathName;
        else
            mDSPathName = String.format("%s%cds", mInsPathName, File.separatorChar);
        if (StringUtils.isNotEmpty(anRDBMSPathName))
            mRDBMSPathName = aGraphPathName;
        else
            mRDBMSPathName = String.format("%s%crdb", mInsPathName, File.separatorChar);
        if (StringUtils.isNotEmpty(aGraphPathName))
            mGraphPathName = aGraphPathName;
        else
            mGraphPathName = String.format("%s%cgdb", mInsPathName, File.separatorChar);
    }

    /**
     * Returns <i>true</i> if the task manager is active and not in the process
     * of shutting down and <i>false</i> otherwise.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAlive()
    {
        return mIsAlive.get();
    }

    /**
     * Assigns the boolean flag to the internally managed "is alive"
     * boolean value.  This method should only be called by the
     * shutdown handler method.
     *
     * @param aFlag Boolean flag.
     */
    public void setIsAliveFlag(boolean aFlag)
    {
        mIsAlive.set(aFlag);
    }

    /**
     * Returns <i>true</i> if the task manager is configured to use an internal
     * abort (a.k.a. JVM addShutdownHook method) handler.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isAbortHandlerEnabled()
    {
        return mIsAbortHandlerEnabled;
    }

    /**
     * Enables/disables the use of an internal abort (a.k.a. JVM addShutdownHook
     * method) handler.
     *
     * @param anIsEnabled Boolean flag.
     */
    public void setAbortHandlerEnabledFlag(boolean anIsEnabled)
    {
        mIsAbortHandlerEnabled = anIsEnabled;
    }

    /**
     * Adds an application manager task to the internal queue of managed task
     * threads.
     *
     * @param aTask Application manager task.
     */
    public void addTask(Task aTask)
    {
        if (mTaskList == null)
            mTaskList = new ArrayList<Task>();
        if (aTask != null)
            mTaskList.add(aTask);
    }

    /**
     * Returns a {@link Task} matching the parameter run name.
     *
     * @param aName Run name of the task.
     * @return A {@link Task} or <i>null</i> if not found.
     */
    public Task getTaskByRunName(String aName)
    {
        if (mTaskList != null)
        {
            for (Task appTask : mTaskList)
            {
                if (appTask.getRunName().equals(aName))
                    return appTask;
            }
        }

        return null;
    }

    /**
     * Returns a <i>Task</i> matching the parameter test name.
     *
     * @param aName Test name of the task.
     * @return A {@link Task} or <i>null</i> if not found.
     */
    public Task getTaskByTestName(String aName)
    {
        if (mTaskList != null)
        {
            for (Task appTask : mTaskList)
            {
                if (appTask.getTestName().equals(aName))
                    return appTask;
            }
        }
        return null;
    }

    private String getInsPathName()
    {
        return mInsPathName;
    }

    private String deriveCfgPathName()
        throws NSException
    {
        String cfgFileName, cfgPathName, cfgPathFileName;

        String installPathName = getInsPathName();
        if (mCmdLine == null)
            cfgFileName = APP_PROPERTY_FILE_NAME;
        else
            cfgFileName = mCmdLine.getOptionValue("cfgfile", APP_PROPERTY_FILE_NAME);

// Did we get an absolute path/file name to cfgfile?

        File cfgFile = new File(cfgFileName);
        if (cfgFile.exists())
            cfgPathName = FilenameUtils.getFullPathNoEndSeparator(cfgFileName);
        else
        {

// Is our default path good enough to locate the property file?

            cfgPathFileName = String.format("%s%c%s", mCfgPathName, File.separatorChar, cfgFileName);
            cfgFile = new File(cfgPathFileName);
            if (cfgFile.exists())
                cfgPathName = FilenameUtils.getFullPathNoEndSeparator(cfgFile.getAbsolutePath());
            else
            {

// Our we running from a maven source tree?

                cfgPathFileName = String.format("%s%csrc%cmain%cresources%c%s", installPathName,
                                                File.separatorChar, File.separatorChar,
                                                File.separatorChar, File.separatorChar,
                                                cfgFileName);
                cfgFile = new File(cfgPathFileName);
                if (cfgFile.exists())
                    cfgPathName = FilenameUtils.getFullPathNoEndSeparator(cfgFile.getAbsolutePath());
                else
                {

// Last chance - are we running from a standard install area?

                    cfgPathFileName = String.format("cfg%c%s", File.separatorChar, cfgFileName);
                    cfgFile = new File(cfgPathFileName);
                    if (cfgFile.exists())
                        cfgPathName = FilenameUtils.getFullPathNoEndSeparator(cfgFile.getAbsolutePath());
                    else
                        throw new NSException("Unable to locate application properties file: " + cfgPathFileName);
                }
            }
        }

        mCfgPathName = cfgPathName;

        return mCfgPathName;
    }

    private String deriveCfgPathFileName()
        throws NSException
    {
        String cfgFileName, cfgPathFileName;

        if (mCmdLine == null)
            cfgFileName = APP_PROPERTY_FILE_NAME;
        else
            cfgFileName = mCmdLine.getOptionValue("cfgfile", APP_PROPERTY_FILE_NAME);

        File cfgFile = new File(cfgFileName);
        if (cfgFile.exists())
        {
            cfgPathFileName = cfgFileName;
            mCfgPathName = FilenameUtils.getFullPathNoEndSeparator(cfgFile.getAbsolutePath());
        }
        else
        {
            cfgPathFileName = String.format("%s%c%s", deriveCfgPathName(),
                                            File.separatorChar, cfgFileName);
        }

        return cfgPathFileName;
    }

    private String deriveLogPathName()
        throws NSException
    {
        String logPathName;

        if (mCmdLine == null)
            logPathName = mLogPathName;
        else
            logPathName = mCmdLine.getOptionValue(APP_PROPERTY_LOG_PATH, mLogPathName);

        File logPathFile = new File(logPathName);
        if (logPathFile.exists())
            mLogPathName = logPathName;
        else
        {
            logPathName = String.format("%s%clog", mInsPathName, File.separatorChar);
            logPathFile = new File(logPathName);
            if (logPathFile.exists())
                mLogPathName = logPathName;
            else
            {

// Are we within a servlet - if yes, then try a relative path.

                String catalinaHome = System.getProperty("catalina.home");
                if (StringUtils.isNotEmpty(catalinaHome))
                {
                    logPathName = String.format("%s%clogs", catalinaHome, File.separatorChar);
                    logPathFile = new File(logPathName);
                    if (logPathFile.exists())
                        mLogPathName = logPathName;
                }
            }
        }

        return mLogPathName;
    }

// Note: A DS path is optional - so we will not generate an exception if it is missing.

    private String deriveDSPathName()
    {
        String dsPathName;

        if (mCmdLine == null)
            dsPathName = mDSPathName;
        else
            dsPathName = mCmdLine.getOptionValue(APP_PROPERTY_DS_PATH, mDSPathName);

        File dsPathFile = new File(dsPathName);
        if (dsPathFile.exists())
            mDSPathName = dsPathName;
        else
        {
            dsPathName = String.format("%s%cds", mInsPathName, File.separatorChar);
            dsPathFile = new File(dsPathName);
            if (dsPathFile.exists())
                mDSPathName = dsPathName;
            else
            {

// Our we running from a maven source tree?

                dsPathName = String.format("%s%csrc%cmain%cresources%cds", mInsPathName,
                                           File.separatorChar, File.separatorChar,
                                           File.separatorChar, File.separatorChar);
                dsPathFile = new File(dsPathName);
                if (dsPathFile.exists())
                    mDSPathName = dsPathName;
                else
                {

// Are we within a servlet - if yes, then try a relative path.

                    String catalinaHome = System.getProperty("catalina.home");
                    if (StringUtils.isNotEmpty(catalinaHome))
                    {
                        dsPathName = String.format("..%c..%cds", File.separatorChar, File.separatorChar);
                        dsPathFile = new File(dsPathName);
                        if (dsPathFile.exists())
                            mDSPathName = dsPathName;
                    }
                }
            }
        }

        return mDSPathName;
    }

// Note: An RDBMS path is optional - so we will not generate an exception if it is missing.

    private String deriveRDBMSPathName()
    {
        String rdbmsPathName;

        if (mCmdLine == null)
            rdbmsPathName = mRDBMSPathName;
        else
            rdbmsPathName = mCmdLine.getOptionValue(APP_PROPERTY_RDB_PATH, mRDBMSPathName);

        File dsPathFile = new File(rdbmsPathName);
        if (dsPathFile.exists())
            mRDBMSPathName = rdbmsPathName;
        else
        {
            rdbmsPathName = String.format("%s%crdb", mInsPathName, File.separatorChar);
            dsPathFile = new File(rdbmsPathName);
            if (dsPathFile.exists())
                mRDBMSPathName = rdbmsPathName;
            else
            {

// Are we within a servlet - if yes, then try a relative path.

                String catalinaHome = System.getProperty("catalina.home");
                if (StringUtils.isNotEmpty(catalinaHome))
                {
                    rdbmsPathName = String.format("..%c..%crdb", File.separatorChar, File.separatorChar);
                    dsPathFile = new File(rdbmsPathName);
                    if (dsPathFile.exists())
                        mRDBMSPathName = rdbmsPathName;
                }
            }
        }

        return mRDBMSPathName;
    }

// Note: A Graph DB path is optional - so we will not generate an exception if it is missing.

    private String deriveGraphPathName()
    {
        String graphPathName;

        if (mCmdLine == null)
            graphPathName = mGraphPathName;
        else
            graphPathName = mCmdLine.getOptionValue(APP_PROPERTY_GDB_PATH, mGraphPathName);

        File dsPathFile = new File(graphPathName);
        if (dsPathFile.exists())
            mGraphPathName = graphPathName;
        else
        {
            graphPathName = String.format("%s%cgdb", mInsPathName, File.separatorChar);
            dsPathFile = new File(graphPathName);
            if (dsPathFile.exists())
                mGraphPathName = graphPathName;
            else
            {

// Are we within a servlet - if yes, then try a relative path.

                String catalinaHome = System.getProperty("catalina.home");
                if (StringUtils.isNotEmpty(catalinaHome))
                {
                    graphPathName = String.format("..%c..%cgdb", File.separatorChar, File.separatorChar);
                    dsPathFile = new File(graphPathName);
                    if (dsPathFile.exists())
                        mGraphPathName = graphPathName;
                }
            }
        }

        return mGraphPathName;
    }

    /**
     * Loads the default property files ("application.properties" and
     * "logback.xml") from the file system and assigns default application
     * properties.
     * <p>
     * <b>Note:</b>&nbsp;This method will establish a default 5 minute reloading
     * policy for the "application.properties" file.  Therefore, any
     * changes to this property file while the application is running
     * will be recognized within a 5 minute period.
     * </p>
     *
     * @throws NSException Typically thrown for I/O related issues.
     */
    public void loadPropertyFiles()
        throws NSException
    {
        String logFileName;

        try
        {

// First, we will read our application properties.

            mConfiguration = new CompositeConfiguration();
            mConfiguration.setDelimiterParsingDisabled(true);
            mConfiguration.addConfiguration(new SystemConfiguration());
            mConfiguration.addConfiguration(new EnvironmentConfiguration());
            PropertiesConfiguration propertyCfg = new PropertiesConfiguration(deriveCfgPathFileName());
            FileChangedReloadingStrategy reloadingStrategy = new FileChangedReloadingStrategy();
            reloadingStrategy.setRefreshDelay(DateUtils.MILLIS_PER_MINUTE * 2L);
            propertyCfg.setReloadingStrategy(reloadingStrategy);
            mConfiguration.addConfiguration(propertyCfg);

// Next, we will load our Logback properties file and configure our application logger.

            if (mCmdLine == null)
                logFileName = LOG_PROPERTY_FILE_NAME;
            else
                logFileName = mCmdLine.getOptionValue("logfile", LOG_PROPERTY_FILE_NAME);
            File logFile = new File(logFileName);
            if (! logFile.exists())
            {
                String logPathFileName = String.format("%s%c%s", deriveCfgPathName(),
                    File.separatorChar, logFileName);
                logFile = new File(logPathFileName);
                if (logFile.exists())
                    logFileName = logPathFileName;
                else
                    throw new NSException("Unable to locate logging properties file: " + logFileName);
            }

            if (StringUtils.isEmpty(getString(APP_PROPERTY_INS_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_INS_PATH, getInsPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_CFG_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_CFG_PATH, deriveCfgPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_LOG_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_LOG_PATH, deriveLogPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_DS_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_DS_PATH, deriveDSPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_RDB_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_RDB_PATH, deriveRDBMSPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_GDB_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_GDB_PATH, deriveGraphPathName());

            LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator joranConfigurator = new JoranConfigurator();
            joranConfigurator.setContext(loggerContext);
            loggerContext.reset();
            joranConfigurator.doConfigure(logFileName);
        }
        catch (ConfigurationException e)
        {
            throw new NSException("Configuration parsing error: " + e.getMessage());
        }
        catch (JoranException e)
        {
            throw new NSException("Logback parsing error: " + e.getMessage());
        }
        catch (ClassCastException ignored)
        {

/* Note that a WARNING related to java.lang.ClassCastException will trigger due to
Smart GWT and NSD being dependent on different releases of the same logging
framework.  http://www.slf4j.org/codes.html */

        }
    }

    /**
     * Initializes the application manager command line processing
     * subsystem using the arguments provided from the parent
     * application.
     *
     * @param aCmdLineArgs An array of command line arguments (typically
     *                     handed down from the application main method).
     *
     * @throws NSException Thrown when the command line arguments are invalid.
     */
    public void init(String[] aCmdLineArgs)
        throws NSException
    {
        if (mTaskList == null)
            mTaskList = new ArrayList<Task>();

        Options cliOptions = new Options();
        cliOptions.addOption("help", false, "Generates a command line usage summary.");
        cliOptions.addOption("logfile", true, "Identifies the application logging properties file name.");
        cliOptions.addOption("cfgfile", true, "Identifies the application configuration properties file name.");
        cliOptions.addOption("run", true,
                             String.format("Identifies the task name to execute (use '%s' for all tasks).",
                             CMDARG_RUNALL_TASKS));
        cliOptions.addOption("test", true,
                             String.format("Identifies the test name to execute (use '%s' for all tests).",
                                           CMDARG_TESTALL_TASKS));

        CommandLineParser cliParser = new DefaultParser();
        try
        {

// First, we will process our command line arguments.

            mCmdLine = cliParser.parse(cliOptions, aCmdLineArgs);
            if (mCmdLine.hasOption("help"))
            {
                HelpFormatter helpFormatter = new HelpFormatter();
                helpFormatter.printHelp(aCmdLineArgs[0], cliOptions);
            }
            else
            {
                if ((! mCmdLine.hasOption("run")) && (! mCmdLine.hasOption("test")))
                    throw new NSException("You specify a 'run' or 'test' name to start this application.");
                else
                {
                    if (mCmdLine.hasOption("run"))
                    {
                        String taskName = mCmdLine.getOptionValue("run");
                        if (! taskName.equals(CMDARG_RUNALL_TASKS))
                        {
                            Task appTask = getTaskByRunName(taskName);
                            if (appTask == null)
                                throw new NSException("Name does not match list of known tasks to execute.");
                        }
                    }
                    else
                    {
                        String taskName = mCmdLine.getOptionValue("test");
                        if (! taskName.equals(CMDARG_TESTALL_TASKS))
                        {
                            Task appTask = getTaskByTestName(taskName);
                            if (appTask == null)
                                throw new NSException("Name does not match list of known tasks to test.");
                        }
                    }
                }
            }
        }
        catch (ParseException e)
        {
            throw new NSException("Command line parsing error: " + e.getMessage());
        }

// Next, we will load our application properties

        loadPropertyFiles();
    }

    /**
     * Initializes the application manager property management
     * subsystem using the composite configuration parameter
     * instance.
     *
     * @param aConfiguration Apache Commons configuration instance.
     *
     * @throws NSException Thrown when the command line arguments are invalid.
     * @see <a href="http://commons.apache.org/configuration/">Apache Commons Configuration</a>
     */
    public void init(CompositeConfiguration aConfiguration)
        throws NSException
    {
        String logPathName;
        mConfiguration = aConfiguration;

        if (mIsPathsExplicit)
            logPathName = mLogPathName;
        else
            logPathName = deriveLogPathName();
        String logFileName = mConfiguration.getString("app.log_file", "logback.xml");
        File logFile = new File(logFileName);
        if (! logFile.exists())
        {
            String logPathFileName = String.format("%s%c%s", logPathName, File.separatorChar, logFile.getName());
            logFile = new File(logPathFileName);
            if (logFile.exists())
                logFileName = logPathFileName;
            else
                return;     // Does not exist - return without error.
        }
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator joranConfigurator = new JoranConfigurator();
        joranConfigurator.setContext(loggerContext);
        loggerContext.reset();
        try
        {
            joranConfigurator.doConfigure(logFileName);
        }
        catch (JoranException e)
        {
            throw new NSException("Logback parsing error: " + e.getMessage());
        }

        if (mIsPathsExplicit)
        {
            mConfiguration.addProperty(APP_PROPERTY_INS_PATH, mInsPathName);
            mConfiguration.addProperty(APP_PROPERTY_CFG_PATH, mCfgPathName);
            mConfiguration.addProperty(APP_PROPERTY_LOG_PATH, mLogPathName);
            mConfiguration.addProperty(APP_PROPERTY_DS_PATH, mDSPathName);
            mConfiguration.addProperty(APP_PROPERTY_RDB_PATH, mRDBMSPathName);
            mConfiguration.addProperty(APP_PROPERTY_GDB_PATH, mGraphPathName);
        }
        else
        {
            if (StringUtils.isEmpty(getString(APP_PROPERTY_INS_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_INS_PATH, getInsPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_CFG_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_CFG_PATH, deriveCfgPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_LOG_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_LOG_PATH, deriveLogPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_DS_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_DS_PATH, deriveDSPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_RDB_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_RDB_PATH, deriveRDBMSPathName());
            if (StringUtils.isEmpty(getString(APP_PROPERTY_GDB_PATH)))
                mConfiguration.addProperty(APP_PROPERTY_GDB_PATH, deriveGraphPathName());
        }
    }

    /**
     * Executes one or more application tasks based on the the run/test command
     * line argument.
     * <p>
     * <b>Note:</b>If the word "all" is specified for the run/test argument
     * parameter, then all tasks defined for this application manager
     * will be executed in parallel.  Otherwise, only the task that matches
     * the run/test name will be executed.
     * </p>
     *
     * @throws NSException Could be thrown by the executing task.
     */
    public void execute()
        throws NSException
    {
        Thread appThread;
        Logger appLogger = getLogger(this, "execute");

        appLogger.trace(LOGMSG_TRACE_ENTER);

        if (mTaskList == null)
            throw new NSException("The task list is undefined and cannot be executed.");

        if (mIsAbortHandlerEnabled)
        {
            Runtime osRuntime = Runtime.getRuntime();
            osRuntime.addShutdownHook(new TaskAbort(this, mTaskList));
        }

// http://www.vogella.com/articles/JavaConcurrency/article.html

        if (mCmdLine.hasOption("run"))
        {
            String taskName = mCmdLine.getOptionValue("run");
            if (taskName.equals(CMDARG_RUNALL_TASKS))
            {
                ArrayList<Thread> threadList = new ArrayList<Thread>();
                for (Task appTask : mTaskList)
                {
                    appTask.init(this);
                    appThread = new Thread(appTask);
                    threadList.add(appThread);
                    appThread.start();
                }

// Next, we will wait for each task thread to complete.

                for (Thread thread : threadList)
                {
                    try
                    {
                        thread.join();
                    }
                    catch (InterruptedException e)
                    {
                        appLogger.warn("Interrupted Thread: " + e.getMessage());
                    }
                }
            }
            else
            {
                Task appTask = getTaskByRunName(taskName);
                if (appTask != null)
                {
                    appTask.init(this);
                    appTask.run();
                }
            }
        }
        else
        {
            if (mCmdLine.hasOption("test"))
            {
                String taskName = mCmdLine.getOptionValue("test");
                if (taskName.equals(CMDARG_TESTALL_TASKS))
                {
                    for (Task appTask : mTaskList)
                    {
                        appTask.init(this);
                        appTask.test();
                    }
                }
                else
                {
                    Task appTask = getTaskByTestName(taskName);
                    if (appTask != null)
                    {
                        appTask.init(this);
                        appTask.test();
                    }
                }
            }
        }

        appLogger.trace(LOGMSG_TRACE_DEPART);
    }

    /**
     * Shutdowns all active tasks associated with this application manager.
     */
    public void shutdown()
    {
        Logger appLogger = getLogger(this, "shutdown");

        appLogger.trace(LOGMSG_TRACE_ENTER);

        if (mTaskList != null)
        {
            for (Task appTask : mTaskList)
            {
                if (appTask.isAlive())
                    appTask.shutdown();
            }
        }

        appLogger.trace(LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns the command line instance.  You would typically use
     * this method if your application requires specialized logic
     * for command line processing.
     *
     * @return Apache Commons Command Line instance.
     * @see <a href="http://commons.apache.org/cli/index.html">Apache Commons CLI</a>
     */
    public CommandLine getCommandLine()
    {
        return mCmdLine;
    }

    /**
     * Convenience method that wraps the property name and its value into
     * a {@link DataField} (assuming that one matches the field name
     * parameter).
     *
     * @param aFieldName Property name.
     *
     * @return Data field if the property name matches or <i>null</i>
     * otherwise.
     */
    public DataField getField(String aFieldName)
    {
        String fieldValue = mConfiguration.getString(aFieldName);
        if (StringUtils.isEmpty(fieldValue))
            return null;
        else
            return new DataTextField(aFieldName, StringUtils.EMPTY, fieldValue);
    }

    /**
     * Identifies whether the property parsing logic should recognize
     * multi-values (comma delimiter) when parsing values.  The default
     * is <i>true</i>.
     *
     * @param aFlag If <i>false</i> multi-value parsing is disabled.
     */
    public void setMultiValueParsingFlag(boolean aFlag)
    {
        mConfiguration.setDelimiterParsingDisabled(aFlag);
    }

    /**
     * Returns the composite configuration instance. You would typically use
     * this method if your application requires specialized logic for property
     * management.
     *
     * @return Apache Commons Configuration Composite instance.
     * @see <a href="http://commons.apache.org/configuration/">Apache Commons Configuration</a>
     */
    public CompositeConfiguration getConfiguration()
    {
        return mConfiguration;
    }

    /**
     * Returns a typed value for the property name identified.
     *
     * @param aName Name of the property.
     *
     * @return Value of the property.
     */
    public boolean getBoolean(String aName)
    {
        return mConfiguration.getBoolean(aName);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aName Name of the property.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public boolean getBoolean(String aName, boolean aDefaultValue)
    {
        return mConfiguration.getBoolean(aName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified.
     *
     * @param aName Name of the property.
     *
     * @return Value of the property.
     */
    public double getDouble(String aName)
    {
        return mConfiguration.getDouble(aName);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aName Name of the property.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public double getDouble(String aName, double aDefaultValue)
    {
        return mConfiguration.getDouble(aName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified.
     *
     * @param aName Name of the property.
     *
     * @return Value of the property.
     */
    public float getFloat(String aName)
    {
        return mConfiguration.getFloat(aName);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aName Name of the property.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public float getFloat(String aName, float aDefaultValue)
    {
        return mConfiguration.getFloat(aName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified.
     *
     * @param aName Name of the property.
     *
     * @return Value of the property.
     */
    public int getInt(String aName)
    {
        return mConfiguration.getInt(aName);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aName Name of the property.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public int getInt(String aName, int aDefaultValue)
    {
        return mConfiguration.getInt(aName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified.
     *
     * @param aName Name of the property.
     *
     * @return Value of the property.
     */
    public long getLong(String aName)
    {
        return mConfiguration.getLong(aName);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aName Name of the property.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public long getLong(String aName, long aDefaultValue)
    {
        return mConfiguration.getLong(aName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified.
     *
     * @param aName Name of the property.
     *
     * @return Value of the property.
     */
    public String getString(String aName)
    {
        return mConfiguration.getString(aName);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aName Name of the property.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public String getString(String aName, String aDefaultValue)
    {
        return mConfiguration.getString(aName, aDefaultValue);
    }

    /**
     * Sets the character that is used as a multi-value delimiter.
     *
     * @param aDelimiter Delimiter character (comma is the default).
     */
    public void setMultiValueDelimiter(char aDelimiter)
    {
        mConfiguration.setListDelimiter(aDelimiter);
    }

    /**
     * Return <i>true</i> if the property name represents multiple
     * values or <i>false</i> otherwise.
     *
     * @param aName Name of the property.
     *
     * @return <i>true</i> or <i>false</i>
     */
    @SuppressWarnings({"RedundantIfStatement"})
    public boolean isPropertyMultiValue(String aName)
    {
        String[] valueList = mConfiguration.getStringArray(aName);
        if ((valueList != null) && (valueList.length > 1))
            return true;
        else
            return false;
    }

    /**
     * Returns an array of values for a multi-value property.
     *
     * @param aName Name of the property.
     * @return Array of property values.
     */
    public String[] getStringArray(String aName)
    {
        return mConfiguration.getStringArray(aName);
    }

    /**
     * Collapses a multi-value array into a single string
     * with each value separated by the multi-value
     * delimiter character.
     *
     * @param aName Name of the property.
     *
     * @return Collapsed string representing one or more values.
     */
    public String getStringArrayAsSingleValue(String aName)
    {
        if (isPropertyMultiValue(aName))
        {
            boolean isFirst = true;
            StringBuilder stringBuilder = new StringBuilder();
            String[] multiValues = getStringArray(aName);
            for (String pValue : multiValues)
            {
                if (isFirst)
                {
                    isFirst = false;
                    stringBuilder.append(pValue);
                }
                else
                {
                    stringBuilder.append(mConfiguration.getListDelimiter());
                    stringBuilder.append(pValue);
                }
            }

            return stringBuilder.toString();
        }
        else
            return getString(aName);
    }

    /**
     * Writes an application identity summary to the output stream
     * associated with the logger instance.
     *
     * @param aLogger Output logger instance.
     * @see <a href="http://logback.qos.ch/">Logback Project</a>
     */
    public void writeIdentity(Logger aLogger)
    {
        String appBuild = "1";
        String appVersion = "1.0";
        String appName = "Undefined Application";
        String appDate = "Mon Jan 06 00:00:00 EDT 2017";
        String appDescription = "The application properties file did not provide a description.";

        if (mConfiguration != null)
        {
            appName = mConfiguration.getString("app.name", appName);
            appDate = mConfiguration.getString("app.date", appDate);
            appBuild = mConfiguration.getString("app.build", appBuild);
            appVersion = mConfiguration.getString("app.version", appVersion);
            appDescription = mConfiguration.getString("app.description", appDescription);
        }

        String appDetails = String.format("%s - %s [build %s] - %s", appName, appVersion, appBuild, appDate);
        aLogger.info(appDetails);
        aLogger.info(appDescription);
    }

    /**
     * Returns a logger instance matching the identity parameter.
     *
     * @param anIdentity An identity is usually a fully qualified
     *                   package.class name.
     *
     * @return An output logger instance.
     *
     * @see <a href="http://logback.qos.ch/">Logback Project</a>
     */
    public Logger getLogger(String anIdentity)
    {
        return LoggerFactory.getLogger(anIdentity);
    }

    /**
     * Returns a logger instance matching the class parameter.  The
     * name associated with the class instance is used for the logger
     * identity.
     *
     * @param aClass Class instance to base identity on.
     *
     * @return An output logger instance.
     *
     * @see <a href="http://logback.qos.ch/">Logback Project</a>
     */
    public Logger getLogger(Class<?> aClass)
    {
        return LoggerFactory.getLogger(aClass.getClass().getName());
    }

    /**
     * Returns a logger instance matching the class parameter.  The
     * name associated with the class instance and the method name
     * are used for the logger identity.
     *
     * @param aClass Class instance to base identity on.
     * @param aMethod Method name to base identity on.
     *
     * @return An output logger instance.
     * @see <a href="http://logback.qos.ch/">Logback Project</a>
     */
    public Logger getLogger(Object aClass, String aMethod)
    {
        return LoggerFactory.getLogger(aClass.getClass().getName() + "." + aMethod);
    }

    /**
     * Writes the collection of property names and their values to
     * the output logger instance.
     *
     * @param aLogger An output logger instance.
     *
     * @see <a href="http://logback.qos.ch/">Logback Project</a>
     */
    public void writeCfgProperties(Logger aLogger)
    {
        if (mConfiguration != null)
        {
            String keyName, keyValue;
            Iterator<String> keyList = mConfiguration.getKeys();
            while (keyList.hasNext())
            {
                keyName = keyList.next();
                keyValue = mConfiguration.getString(keyName);
                if (StringUtils.isEmpty(keyValue))
                    keyValue = "undefined";
                aLogger.debug(String.format("%s = %s", keyName, keyValue));
            }
        }
    }

    /**
     * Add an application defined property to the application manager.
     * <p>
     * <b>Notes:</b>
     * </p>
     * <ul>
     *     <li>The goal of the AppMgr is to strike a balance between
     *     providing enough properties to adequately model application
     *     related data without overloading it.</li>
     *     <li>This method offers a mechanism to capture additional
     *     (application specific) properties that may be needed.</li>
     *     <li>Properties added with this method are transient and
     *     will not be persisted when saved.</li>
     * </ul>
     *
     * @param aName Property name (duplicates are not supported).
     * @param anObject Instance of an object.
     */
    public void addProperty(String aName, Object anObject)
    {
        if ((StringUtils.isNotEmpty(aName)) && (anObject != null))
            mPropertyMap.put(aName, anObject);
    }

    /**
     * Removes the property from the internally managed map.
     *
     * @param aName Property name.
     */
    public void removeProperty(String aName)
    {
        if (StringUtils.isNotEmpty(aName))
            mPropertyMap.remove(aName);
    }

    /**
     * Returns the object associated with the property name or
     * <i>null</i> if the name could not be matched.
     *
     * @param aName Name of the property.
     * @return Instance of an object.
     */
    public Object getProperty(String aName)
    {
        return mPropertyMap.get(aName);
    }

    /**
     * Removes all application defined properties assigned to
     * this application manager.
     */
    public void clearProperties()
    {
        mPropertyMap.clear();
    }
}
