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

package com.nridge.core.base.doc;

import java.util.ArrayList;

/**
 * The Doc class captures the constants, enumerated types and utility methods
 * for the document package.
 *
 * @since 1.0
 * @author Al Cole
 */
public class Doc
{
    public static final int SCHEMA_VERSION_DEFAULT = 1;

// ACL and ACE constants.

    public static final String ACE_USER_ID = "U";
    public static final String ACE_GROUP_ID = "G";
    
    public static final String ACE_PERMISSION_VIEW = "V";
    public static final String ACE_PERMISSION_PREVIEW = "P";

    public static final String ACE_PERMISSION_PUBLIC = "public";

    public static final String FIELD_ACL_NAME = "acl_name";
    public static final String FIELD_ACL_TITLE = "ACL Name";
    public static final String FIELD_PERMISSION_NAME = "acl_permission";
    public static final String FIELD_PERMISSION_TITLE = "ACL Permission";

    public static final String FEATURE_OP_NAME = "name";
    public static final String FEATURE_OP_COUNT = "count";
    public static final String FEATURE_OP_LIMIT = "limit";
    public static final String FEATURE_OP_OFFSET = "offset";
    public static final String FEATURE_OP_SESSION = "session";
    public static final String FEATURE_OP_STATUS_CODE = "statusCode";
    public static final String FEATURE_OP_TRANSACTION = "transaction";
    public static final String FEATURE_OP_ACCOUNT_NAME = "accountName";
    public static final String FEATURE_OP_ACCOUNT_PASSWORD = "accountPassword";
    public static final String FEATURE_OP_ACCOUNT_PASSHASH = "accountPasshash";

    public static final String OPERATION_ADD = "add";
    public static final String OPERATION_COUNT = "count";
    public static final String OPERATION_FETCH = "fetch";
    public static final String OPERATION_UPDATE = "update";
    public static final String OPERATION_DELETE = "delete";

    public static final String FEATURE_REPLY_CODE = "code";
    public static final String FEATURE_REPLY_DETAIL = "detail";
    public static final String FEATURE_REPLY_MESSAGE = "message";
    public static final String FEATURE_REPLY_SESSION_ID = "sessionId";

    public static final int STATUS_CODE_SUCCESS = 0;
    public static final int STATUS_CODE_FAILURE = 1;

    private Doc()
    {
    }

    public static ArrayList<Document> documentList(Document aDocument)
    {
        ArrayList<Document> documentList = new ArrayList<Document>();
        documentList.add(aDocument);

        return documentList;
    }

    public static Document firstDocumentInList(ArrayList<Document> aList)
    {
        if ((aList != null) && (aList.size() > 0))
            return aList.get(0);

        return null;
    }

    public static String defaultApplicationSecret()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('P');
        stringBuilder.append('@');
        stringBuilder.append('5');
        stringBuilder.append('5');
        stringBuilder.append('w');
        stringBuilder.append('0');
        stringBuilder.append('r');
        stringBuilder.append('d');

        return stringBuilder.toString();
    }
}
