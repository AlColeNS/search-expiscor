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

import com.nridge.core.app.mgr.AppMgr;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.Status;
import org.restlet.routing.Filter;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * The RestletFilter class filters calls before passing them to an attached Restlet.
 * The purpose is to do some pre-processing or post-processing on the calls going
 * through it before or after they are actually handled by an attached Restlet.
 * Also note that you can attach and detach targets while handling incoming calls
 * as the filter is ensured to be thread-safe.
 *
 * @see <a href="http://restlet.org/">Restlet Framework</a>
 */
public class RestletFilter extends Filter
{
    private AppMgr mAppMgr;
    private Set<String> mAcceptAddresses;

    public RestletFilter(AppMgr anAppMgr, Context aContext)
    {
        super(aContext);
        mAppMgr = anAppMgr;
        mAcceptAddresses = new CopyOnWriteArraySet<String>();
    }

    public void add(String anIPAddress)
    {
        mAcceptAddresses.add(anIPAddress);
    }

    public Set<String> getAddresses()
    {
        return mAcceptAddresses;
    }

    @Override
    protected int beforeHandle(Request aRequest, Response aResponse)
    {
        Logger appLogger = mAppMgr.getLogger(this, "beforeHandle");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        int restletStatus = CONTINUE;
        if (mAcceptAddresses.size() > 0)
        {
            String clientAddress = aRequest.getClientInfo().getAddress();
            if (! mAcceptAddresses.contains(clientAddress))
            {
                String errMsg = "Your client IP address is not on the accepted list.";
                aResponse.setStatus(Status.CLIENT_ERROR_FORBIDDEN, errMsg);
                restletStatus = STOP;

                appLogger.debug(clientAddress + ": " + errMsg);
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return restletStatus;
    }
}
