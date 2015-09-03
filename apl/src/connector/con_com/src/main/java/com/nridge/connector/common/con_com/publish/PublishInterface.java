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

package com.nridge.connector.common.con_com.publish;

import com.nridge.core.base.doc.Document;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.std.NSException;
import com.nridge.ds.ds_common.DSDocument;

import javax.activation.DataSource;

/**
 * The PublishInterface provides a collection of methods that can
 * publish a document to configured endpoint (e.g. search index,
 * NoSQL storage).
 *
 * @since 1.0
 * @author Al Cole
 */
public interface PublishInterface
{
    /**
     * Validates that the publish feature is properly configured
     * to run as part of the parent application pipeline.
     *
     * @throws com.nridge.core.base.std.NSException Indicates a configuration issue.
     */
    public void validate() throws NSException;

    /**
     * Returns the data source document instance currently being managed by the
     * publisher component.
     *
     * @return Data source document instance.
     */
    public DSDocument getDS();

    /**
     * Initializes the publishing process by configuring the data source
     * components and initializing the internal state of the tracking
     * variables.
     *
     * @param aBag Data bag instance defining a schema.
     *
     * @throws NSException Indicates a configuration or I/O error condition.
     */
    public void initialize(DataBag aBag) throws NSException;

    /**
     * Adds the document to the configured publishing endpoint (e.g. search index,
     * NoSQL storage)..
     *
     * @param aDocument Document instance.
     *
     * @throws NSException Indicates a data validation error condition.
     */
    public void add(Document aDocument) throws NSException;

    /**
     * Completes the publishing process by flushing the internally managed
     * memory list of documents to the search service index and resetting
     * the internal state variables.
     *
     * @throws NSException Indicates a configuration error condition.
     */
    public void shutdown() throws NSException;
}
