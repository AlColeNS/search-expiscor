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

package com.nridge.ds.content.ds_content;

import com.nridge.core.base.std.DigitalHash;

import java.io.IOException;
import java.util.UUID;

/**
 * The Content class captures the constants, enumerated types
 * and utility methods for the content text extraction package.
 *
 * @author Al Cole
 * @since 1.0
 */
public class Content
{
    public static final String CFG_PROPERTY_PREFIX = "ds.content";

    public static final String CFG_CONTENT_LIMIT = "content_limit";
    public static final String CFG_CONTENT_ENCODING = "content_encoding";

    public static final int CONTENT_LIMIT_DEFAULT = 250000;

    public static final String CONTENT_TYPE_HTML = "text/html";
    public static final String CONTENT_TYPE_UNKNOWN = "Unknown";
    public static final String CONTENT_TYPE_TXT_CSV = "text/csv";
    public static final String CONTENT_TYPE_APP_CSV = "application/csv";
    public static final String CONTENT_TYPE_DEFAULT = CONTENT_TYPE_UNKNOWN;

    public static final String CONTENT_FIELD_METADATA = "nsd_md_";

    public static final String CONTENT_DOCTYPE_FILE_DEFAULT = "document_types.csv";

    private Content()
    {
    }

    /**
     * Generates a unique hash string using the MD5 algorithm using
     * the path/file name.
     *
     * @param aPathFileName Name of path/file to base hash on.
     *
     * @return Unique hash string.
     */
    public static String hashId(String aPathFileName)
    {
        String hashId;

        DigitalHash digitalHash = new DigitalHash();
        try
        {
            digitalHash.processBuffer(aPathFileName);
            hashId = digitalHash.getHashSequence();
        }
        catch (IOException e)
        {
            UUID uniqueId = UUID.randomUUID();
            hashId = uniqueId.toString();
        }

        return hashId;
    }
}
