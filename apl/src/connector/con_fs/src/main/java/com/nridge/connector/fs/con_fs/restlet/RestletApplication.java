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

package com.nridge.connector.fs.con_fs.restlet;

import com.nridge.connector.fs.con_fs.core.Constants;
import com.nridge.core.app.mgr.AppMgr;
import org.apache.commons.lang3.StringUtils;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;
import org.slf4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The RestletApplication class is responsible for managing a coherent set
 * of resources and services. Applications are guaranteed to receive calls
 * with their base reference set relatively to the VirtualHost that served
 * them. This class is both a descriptor able to create the root Restlet
 * and the actual Restlet that can be attached to one or more VirtualHost
 * instances.
 *
 * @see <a href="http://restlet.org/">Restlet Framework</a>
 */
public class RestletApplication extends Application
{
    private AppMgr mAppMgr;
    private int mPortNumber;

    /**
     * Constructor that accepts an instance to the application manager.
     *
     * @param anAppMgr Application manager instance.
     */
    public RestletApplication(AppMgr anAppMgr)
    {
        super();

// General application initialization.

        mAppMgr = anAppMgr;
        setAuthor("Al Cole");
        setOwner("NorthRidge Software, LLC");
        setName(mAppMgr.getString("app.name"));
        setDescription(mAppMgr.getString("app.description"));
        mPortNumber = mAppMgr.getInt(Constants.CFG_PROPERTY_PREFIX + ".restlet.port_number",
                                     Constants.APPLICATION_PORT_NUMBER_DEFAULT);

        mAppMgr.addProperty(Constants.PROPERTY_RESTLET_APPLICATION, this);
    }

    /**
     * Returns the application manager instance.
     *
     * @return Application manager instance.
     */
    public AppMgr getAppMgr()
    {
        return mAppMgr;
    }

    /* Some thoughts:

        ../admin/op/target
            /admin/ping/service
            /admin/status/all
            /admin/status/extract
            /admin/status/transform
            /admin/status/publish
        ../doc/view?id=1234
     */

    private void routerAttachEndPoints(Router aRouter, String aHostName)
    {
        String uriName;
        Logger appLogger = mAppMgr.getLogger(this, "routerAttachEndPoints");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mPortNumber == Constants.HTTP_PORT_NUMBER_DEFAULT)
        {
            uriName = String.format("http://%s/{dsType}/ping", aHostName);
            aRouter.attach(uriName, ResourceAdminPing.class);
            appLogger.debug("Router URI: " + uriName);
            uriName = String.format("http://%s/{dsType}/view", aHostName);
            aRouter.attach(uriName, ResourceDocView.class);
            appLogger.debug("Router URI: " + uriName);
        }
        else
        {
            uriName = String.format("http://%s:%d/{dsType}/ping", aHostName, mPortNumber);
            aRouter.attach(uriName, ResourceAdminPing.class);
            appLogger.debug("Router URI: " + uriName);
            uriName = String.format("http://%s:%d/{dsType}/view", aHostName, mPortNumber);
            aRouter.attach(uriName, ResourceDocView.class);
            appLogger.debug("Router URI: " + uriName);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns a Restlet instance used to identify inbound requests for the
     * web service endpoints.
     *
     * @return Restlet instance.
     */
    @Override
    public Restlet createInboundRoot()
    {
        Restlet restletRoot;
        Logger appLogger = mAppMgr.getLogger(this, "createInboundRoot");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        Context restletContext = getContext();
        Router restletRouter = new Router(restletContext);

        String propertyName = Constants.CFG_PROPERTY_PREFIX + ".restlet.host_names";
        String hostNames = mAppMgr.getString(propertyName);
        if (StringUtils.isEmpty(hostNames))
        {
            try
            {
                InetAddress inetAddress = InetAddress.getLocalHost();

                routerAttachEndPoints(restletRouter, Constants.HOST_NAME_DEFAULT);
                routerAttachEndPoints(restletRouter, inetAddress.getHostName());
                routerAttachEndPoints(restletRouter, inetAddress.getHostAddress());
                routerAttachEndPoints(restletRouter, inetAddress.getCanonicalHostName());
            }
            catch (UnknownHostException e)
            {
                appLogger.error(e.getMessage(), e);
                routerAttachEndPoints(restletRouter, Constants.HOST_NAME_DEFAULT);
            }
        }
        else
        {
            if (mAppMgr.isPropertyMultiValue(propertyName))
            {
                String[] hostNameList = mAppMgr.getStringArray(propertyName);
                for (String hostName : hostNameList)
                    routerAttachEndPoints(restletRouter, hostName);
            }
            else
                routerAttachEndPoints(restletRouter, hostNames);
        }

        RestletFilter restletFilter = new RestletFilter(mAppMgr, restletContext);
        propertyName = Constants.CFG_PROPERTY_PREFIX + ".restlet.allow_addresses";
        String allowAddresses = mAppMgr.getString(propertyName);
        if (StringUtils.isNotEmpty(allowAddresses))
        {
            if (mAppMgr.isPropertyMultiValue(propertyName))
            {
                String[] allowAddressList = mAppMgr.getStringArray(propertyName);
                for (String allowAddress : allowAddressList)
                {
                    restletFilter.add(allowAddress);
                    appLogger.debug("Filter Allow Address: " + allowAddress);
                }
            }
            else
            {
                restletFilter.add(allowAddresses);
                appLogger.debug("Filter Allow Address: " + allowAddresses);
            }
            restletFilter.setNext(restletRouter);
            restletRoot = restletFilter;
        }
        else
            restletRoot = restletRouter;

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return restletRoot;
    }
}
