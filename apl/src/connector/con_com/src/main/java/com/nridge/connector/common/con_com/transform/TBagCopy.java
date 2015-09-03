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

package com.nridge.connector.common.con_com.transform;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.std.NSException;
import org.slf4j.Logger;

/**
 * The TBagCopy document transformer simply copies the fields
 * from source document to the destination document.
 *
 * @since 1.0
 * @author Al Cole
 */
public class TBagCopy implements TransformInterface
{
    private final AppMgr mAppMgr;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     */
    public TBagCopy(final AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
    }

    /**
     * Validates that the transformation feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void validate()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "validate");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Applies a transformation against the source document to produce
     * the returned destination document.  If the document has a field
     * containing content, then this method may update it as part of its
     * processing logic.
     *
     * @param aSrcDoc Source document instance.
     *
     * @return New destination document instance.
     *
     * @throws NSException Indicates a data validation error condition.
     */
    public Document process(Document aSrcDoc)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aSrcDoc == null)
            throw new NSException("Source data bag is null.");

        Document dstDoc = new Document(aSrcDoc);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dstDoc;
    }
}
