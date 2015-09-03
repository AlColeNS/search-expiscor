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

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.std.NSException;

/**
 * The TransformInterface provides a collection of methods that can
 * manipulate a document in a transform pipeline.
 *
 * @since 1.0
 * @author Al Cole
 */
public interface TransformInterface
{
    /**
     * Validates that the transformation feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws NSException Indicates a configuration issue.
     */
    public void validate() throws NSException;

    /**
     * Applies a transformation against the source document to produce
     * the returned destination document.
     *
     * @param aSrcDoc Source document instance.
     *
     * @return New destination document instance.
     *
     * @throws NSException Indicates a data validation error condition.
     */
    public Document process(Document aSrcDoc) throws NSException;
}
