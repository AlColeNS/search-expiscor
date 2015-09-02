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

package com.nridge.core.base.std;

import org.apache.commons.lang3.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

/**
 * The Platform class provides utility methods for platform-related operations.
 *
 * @author Al Cole
 * @version 1.0 Jan 4, 2014
 * @since 1.0
 */
public class Platform
{
    public static final double FORMAT_SIZE_IN_KB = 1024;
    public static final double FORMAT_SIZE_IN_MB = 1024 * FORMAT_SIZE_IN_KB;
    public static final double FORMAT_SIZE_IN_GB = 1024 * FORMAT_SIZE_IN_MB;
    public static final double FORMAT_SIZE_IN_TB = 1024 * FORMAT_SIZE_IN_GB;

    public static final String PLATFORM_UNIX = "UNIX";
    public static final String PLATFORM_LINUX = "Linux";
    public static final String PLATFORM_MACOS = "Mac OS";
    public static final String PLATFORM_WINDOWS = "Windows";

    /**
     * Determines if the current platform that the JVM is executing within is
     * a Windows-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isWindows()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_WINDOWS);
    }

    /**
     * Determines if the current platform that the JVM is executing within is
     * a Mac-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isMac()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_MACOS);
    }

    /**
     * Determines if the current platform that the JVM is executing within is
     * a Linux-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isLinux()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_LINUX);
    }

    /**
     * Determines if the current platform that the JVM is executing within is
     * a UNIX-based operating system.
     * @return <i>true</i> if it is or <i>false</i> otherwise.
     */
    public static boolean isUNIX()
    {
        String osName;
        Properties osProperties;

        osProperties = System.getProperties();
        osName = (String) osProperties.get("os.name");
        return StringUtils.isNotEmpty(osName) && osName.startsWith(PLATFORM_UNIX);
    }

    /**
     * Convenience method that returns the host name of the current machine.
     *
     * @return Host name.
     */
    public static String getHostName()
    {
        String hostName;

        try
        {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostName = inetAddress.getHostName();
        }
        catch (UnknownHostException e)
        {
            hostName = "localhost";
        }

        return hostName;
    }

    /**
     * Convenience method that returns the host IP address of the current machine.
     *
     * @return Host IP address.
     */
    public static String getHostIPAddress()
    {
        String hostAddress;

        try
        {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostAddress = inetAddress.getHostAddress();
        }
        catch (UnknownHostException e)
        {
            hostAddress = "127.0.0.1";
        }

        return hostAddress;
    }

    /**
     * Convenience method that returns the full qualified domain name of the current machine.
     *
     * @return Fully Qualified Domain Name
     */
    public static String getFullyQualifiedDomainName()
    {
        String hostName;

        try
        {
            InetAddress inetAddress = InetAddress.getLocalHost();
            hostName = inetAddress.getCanonicalHostName();
        }
        catch (UnknownHostException e)
        {
            hostName = "localhost";
        }

        return hostName;
    }

    public static String bytesToString(long aSizeInBytes)
    {

        NumberFormat numberFormat = new DecimalFormat();
        numberFormat.setMaximumFractionDigits(2);
        try
        {
            if (aSizeInBytes < FORMAT_SIZE_IN_KB)
            {
                return numberFormat.format(aSizeInBytes) + " byte(s)";
            }
            else if (aSizeInBytes < FORMAT_SIZE_IN_MB)
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_KB) + " KB";
            }
            else if (aSizeInBytes < FORMAT_SIZE_IN_GB)
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_MB) + " MB";
            }
            else if (aSizeInBytes < FORMAT_SIZE_IN_TB)
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_GB) + " GB";
            }
            else
            {
                return numberFormat.format(aSizeInBytes / FORMAT_SIZE_IN_TB) + " TB";
            }
        }
        catch (Exception e)
        {
            return aSizeInBytes + " byte(s)";
        }
    }

    /**
     * Create a log message containing JVM Heap Memory statistics.
     * <p>totalMemory(): Returns the total amount of memory in the
     * Java virtual machine. The value returned by this method may
     * vary over time, depending on the host environment. Note that
     * the amount of memory required to hold an object of any given
     * type may be implementation-dependent.</p>
     * <p>maxMemory(): Returns the maximum amount of memory that the
     * Java virtual machine will attempt to use. If there is no inherent
     * limit then the value Long.MAX_VALUE will be returned.</p>
     * <p>freeMemory(): Returns the amount of free memory in the Java
     * Virtual Machine. Calling the gc method may result in increasing
     * the value returned by freeMemory.</p>
     * <p>In reference to your question, maxMemory() returns the -Xmx value.
     * You may be wondering why there is a totalMemory() AND a maxMemory().
     * The answer is that the JVM allocates memory lazily.</p>
     *
     * @param aTitle Title to save with log entry.
     *
     * @return Log message.
     *
     * @see <a href="http://stackoverflow.com/questions/3571203/what-is-the-exact-meaning-of-runtime-getruntime-totalmemory-and-freememory">Runtime Memory</a>
     * @see <a href="http://www.mkyong.com/java/find-out-your-java-heap-memory-size/">Heap Memory</a>
     * @see <a href="http://docs.oracle.com/javase/7/docs/api/java/lang/Runtime.html">JavaDoc Runtime</a>
     */
    public static String jvmLogMessage(String aTitle)
    {
        Runtime jvmRuntime = Runtime.getRuntime();

        if (StringUtils.isEmpty(aTitle))
            aTitle = "JVM";

        long maxMemory = jvmRuntime.maxMemory();
        long freeMemory = jvmRuntime.freeMemory();
        long totalMemory = jvmRuntime.totalMemory();
        long usedMemory = totalMemory - freeMemory;
        long availMemory = maxMemory - usedMemory;

        String logMsg = String.format("%s: Processors: %d, Mem Max: %s, Mem Total: %s, Mem Used: %s, Mem Avail: %s",
                                      aTitle, jvmRuntime.availableProcessors(),
                                      bytesToString(maxMemory),
                                      bytesToString(totalMemory),
                                      bytesToString(usedMemory),
                                      bytesToString(availMemory));
        return logMsg;
    }
}
