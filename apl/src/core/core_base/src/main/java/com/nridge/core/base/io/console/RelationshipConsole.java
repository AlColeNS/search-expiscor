/*
 * NorthRidge Software, LLC - Copyright (c) 2019.
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

package com.nridge.core.base.io.console;

import com.nridge.core.base.doc.Doc;
import com.nridge.core.base.doc.Document;
import com.nridge.core.base.doc.Relationship;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.io.IO;

import java.io.PrintWriter;

/**
 * The RelationshipConsole class provides a convenient way for an application
 * to generate a presentation for the console.  The presentation of the
 * columns are aligned based on display widths.
 * <p>
 * <b>Note:</b> The width of the column will be derived from the field
 * display width or the field name length or the field value length.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class RelationshipConsole
{
    private Relationship mRelationship;

    /**
     * Creates a document relationship console object.
     *
     * @param aRelationship A relationship instance.
     */
    public RelationshipConsole(Relationship aRelationship)
    {
        mRelationship = aRelationship;
    }

    /**
     * Writes the contents of the relationship to the console.
     *
     * @param aPW Print writer instance.
     * @param aTitle An optional presentation title.
     * @param aMaxWidth Maximum width of any column (zero means unlimited).
     * @param aColSpace Number of spaces between columns.
     */
    public void write(PrintWriter aPW, String aTitle,
                      int aMaxWidth, int aColSpace)
    {
        DataBag dataBag;
        DocumentConsole documentConsole;

        dataBag = mRelationship.getBag();
        if (dataBag.assignedCount() > 0)
        {
            DataBagConsole dataBagConsole = new DataBagConsole(dataBag);
            dataBagConsole.writeBag(aPW, IO.XML_PROPERTIES_NODE_NAME);
        }
        else
            aPW.printf("%n%s%n", aTitle);

        for (Document relatedDocument : mRelationship.getDocuments())
        {
            documentConsole = new DocumentConsole(relatedDocument);
            documentConsole.write(aPW, relatedDocument.getTitle(), aMaxWidth, aColSpace);
            aPW.printf("%n");
        }
    }

    /**
     * Writes the contents of the attribute table to the console.
     *
     * @param aPW Printer write instance.
     * @param aTitle An optional presentation title.
     */
    public void write(PrintWriter aPW, String aTitle)
    {
        write(aPW, aTitle, 0, 2);
    }
}
