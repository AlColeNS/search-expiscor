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
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.std.NSException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import java.util.ArrayList;

/**
 * The TContentClean document transformer a content field
 * by cleaning it (e.g. extra spaces, dots and control
 * characters).
 *
 * @since 1.0
 * @author Al Cole
 */
public class TContentClean implements TransformInterface
{
    private final AppMgr mAppMgr;

    /**
     * Default constructor that accepts an AppMgr instance for
     * configuration and logging purposes.
     *
     * @param anAppMgr Application manager instance.
     */
    public TContentClean(final AppMgr anAppMgr)
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

    private boolean isASCII(char aChar)
    {
        return aChar < 128;
    }

    private String cleanControl(String aValue)
    {
        String cleanValue;

        if (StringUtils.isNotEmpty(aValue))
        {
            String[] nlReplace = new String[]{" ", " ", " "};
            String[] nlPattern = new String[]{"\r", "\n", "\t"};

            String replaceValue = StringUtils.replaceEach(aValue, nlPattern, nlReplace);
            replaceValue = StringUtils.trim(replaceValue);
            cleanValue = replaceValue.replaceAll("\\p{Cntrl}", "");

            StringBuilder asciiValue = new StringBuilder(cleanValue.length());
            int strLength = cleanValue.length();
            for (int i = 0; i < strLength; i++)
            {
                if (isASCII(cleanValue.charAt(i)))
                    asciiValue.append(cleanValue.charAt(i));
            }
            cleanValue = asciiValue.toString();
        }
        else
            cleanValue = StringUtils.EMPTY;

        return cleanValue;
    }

    private String cleanSpaces(String aValue)
    {
        String cleanValue;

        if (StringUtils.isNotEmpty(aValue))
        {
            String trimValue = StringUtils.trim(aValue);
            cleanValue = trimValue.replaceAll("\\s+", " ");
        }
        else
            cleanValue = StringUtils.EMPTY;

        return cleanValue;
    }

    private String cleanDots(String aValue)
    {
        String cleanValue;

        if (StringUtils.isNotEmpty(aValue))
            cleanValue = aValue.replaceAll("\\.+", ".");
        else
            cleanValue = StringUtils.EMPTY;

        return cleanValue;
    }

    private String cleanValue(String aValue)
    {
        String cleanControl = cleanControl(aValue);
        String cleanSpaces = cleanSpaces(cleanControl);

        return cleanDots(cleanSpaces);
    }

    private void process(DataBag aBag)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "process");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (aBag == null)
            throw new NSException("Source bag is null.");

        DataField dataField = aBag.getFirstFieldByFeatureName(Field.FEATURE_IS_CONTENT);
        if (dataField != null)
            dataField.setValue(cleanValue(dataField.getValue()));

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
            throw new NSException("Source document is null.");

        Document dstDoc = new Document(aSrcDoc);
        process(dstDoc.getBag());

        ArrayList<Relationship> relationshipList = dstDoc.getRelationships();
        for (Relationship relationship : relationshipList)
        {
            process(relationship.getBag());
            for (Document document : relationship.getDocuments())
                process(document.getBag());
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return dstDoc;
    }
}
