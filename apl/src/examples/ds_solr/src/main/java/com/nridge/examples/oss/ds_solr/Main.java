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

package com.nridge.examples.oss.ds_solr;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.std.NSException;
import org.slf4j.Logger;

public class Main
{
    private static final String APPLICATION_IDENTITY = "com.nridge.examples.oss.ds_solr.Main";

    public static void main(String[] anArgs)
    {
        AppMgr appMgr = new AppMgr();

        appMgr.setAbortHandlerEnabledFlag(true);
        appMgr.addTask(new DSSolrTask());
        try
        {
            appMgr.init(anArgs);
            Logger appLogger = appMgr.getLogger(APPLICATION_IDENTITY);
            appMgr.writeIdentity(appLogger);
            appMgr.execute();
        }
        catch (NSException e)
        {
            System.err.printf("%nAppMgr Error: %s%n", e.getMessage());
        }
        finally
        {
            appMgr.shutdown();
        }
    }
}

