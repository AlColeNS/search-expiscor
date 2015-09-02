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

package com.nridge.core.app.ldap;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.data.DataBag;
import com.nridge.core.base.field.data.DataField;
import com.nridge.core.base.field.data.DataTable;
import com.nridge.core.base.field.data.DataTextField;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.util.Hashtable;

/**
 * The ADQuery offers a collection of Active Directory query services
 * that can used to authenticate a user within an application.  The
 * logic was designed to support the pooling of <i>LdapContext</i>
 * objects and manages the closing of a {@link NamingEnumeration}
 * after query completes.
 * <p>
 * <b>Note:</b> Since a site may define one-or-more custom
 * attributes for their organization, the <code>schemaUserBag</code>
 * and <code>schemaGroupBag</code> methods focus on the a
 * collection of fields most likely to be present in their
 * Active Directory repository by default.  You should consider
 * using a tool like an LDAP Browser to identify a more complete
 * list of fields and append them to the default bags.
 * </p>
 *
 * @since 1.0
 * @author Al Cole
 *
 * @see <a href="http://www.ldapbrowser.com/">Softerra LDAP Browser</a>
 */
public class ADQuery
{
    public final String LDAP_COMMON_NAME = "cn";
    public final String LDAP_OBJECT_SID = "objectSid";
    public final String LDAP_ACCOUNT_NAME = "sAMAccountName";
    public final String LDAP_DISTINGUISHED_NAME = "distinguishedName";

    private AppMgr mAppMgr;
    private String mPropertyPrefix;
    private LdapContext mLdapContext;

    /**
     * Constructor accepts an application manager(for property
     * and logging).
     *
     * @param anAppMgr Application manager.
     */
    public ADQuery(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mPropertyPrefix = "ldap.default";
    }

    /**
     * Constructor accepts an application manager(for property
     * and logging) and a property prefix string.
     * <p>
     * The follow properties will be derived using the property
     * prefix:
     * </p>
     * <ul>
     *     <li>domain_url Defines the connection URI</li>
     *     <li>authentication Defines the LDAP authentication method</li>
     *     <li>account_name Defines the LDAP account name (DN format)</li>
     *     <li>account_password Defines the LDAP account password</li>
     *     <li>referral_handling Defines referral handling (follow, throw, ignore)</li>
     * </ul>
     *
     * @param anAppMgr Application manager.
     * @param aPropertyPrefix Property prefix string.
     *
     * @see <a href="http://docs.oracle.com/javase/1.5.0/docs/guide/jndi/jndi-ldap-gl.html">LDAP Reference</a>
     */
    public ADQuery(AppMgr anAppMgr, String aPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Returns a bag of default fields representing an Active Directory
     * user object.
     *
     * @return Bag of fields.
     */
    public DataBag schemaUserBag()
    {
        DataBag dataBag = new DataBag("LDAP Active Directory User");

        dataBag.add(new DataTextField(LDAP_ACCOUNT_NAME, "Account Name"));
        dataBag.add(new DataTextField(LDAP_COMMON_NAME, "Common Name"));
        dataBag.add(new DataTextField(LDAP_DISTINGUISHED_NAME, "Distinguished Name"));
        dataBag.add(new DataTextField("givenName", "First Name"));
        dataBag.add(new DataTextField("sn", "Last Name"));
        dataBag.add(new DataTextField("displayName", "Display Name"));
        dataBag.add(new DataTextField("description", "Description"));
        dataBag.add(new DataTextField("info", "Notes"));
        dataBag.add(new DataTextField("mail", "Email"));
        dataBag.add(new DataTextField("wWWHomePage", "WWW Home Page"));
        dataBag.add(new DataTextField("title", "Title"));
        dataBag.add(new DataTextField("department", "Department"));
        dataBag.add(new DataTextField("company", "Company"));
        dataBag.add(new DataTextField(LDAP_OBJECT_SID, "Security Identifier"));

        return dataBag;
    }

    /**
     * Returns a bag of default fields representing an Active Directory
     * group object.
     *
     * @return Bag of fields.
     */
    public DataBag schemaGroupBag()
    {
        DataBag dataBag = new DataBag("LDAP Active Directory Group");

        dataBag.add(new DataTextField(LDAP_ACCOUNT_NAME, "Account Name"));
        dataBag.add(new DataTextField(LDAP_COMMON_NAME, "Common Name"));
        dataBag.add(new DataTextField(LDAP_DISTINGUISHED_NAME, "Distinguished Name"));
        dataBag.add(new DataTextField("description", "Description"));
        dataBag.add(new DataTextField("info", "Notes"));
        dataBag.add(new DataTextField(LDAP_OBJECT_SID, "Security Identifier"));

        return dataBag;
    }

    private String getPropertyValue(String aFieldName, String aDefaultValue)
        throws NSException
    {
        String completeFieldName = mPropertyPrefix + "." + aFieldName;
        String fieldValue = mAppMgr.getString(completeFieldName);
        if (StringUtils.isEmpty(fieldValue))
        {
            if (StringUtils.isEmpty(aDefaultValue))
                throw new NSException(completeFieldName + ": LDAP field is undefined.");
            else
                fieldValue = aDefaultValue;
        }
        else
        {
            if (mAppMgr.isPropertyMultiValue(completeFieldName))
                fieldValue = mAppMgr.getStringArrayAsSingleValue(completeFieldName);
        }

        return fieldValue;
    }

    /**
     * Opens a connection to Active Directory by establishing an initial LDAP
     * context.  The security principal and credentials are assigned the
     * account name and password parameters.
     *
     * @param anAcountDN Active Directory account name (DN format).
     * @param anAccountPassword Active Directory account password.
     *
     * @throws NSException Thrown if an LDAP naming exception is occurs.
     */
    @SuppressWarnings("unchecked")
    public void open(String anAcountDN, String anAccountPassword)
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "open");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

// LDAP Reference - http://docs.oracle.com/javase/1.5.0/docs/guide/jndi/jndi-ldap-gl.html

        Hashtable environmentalVariables = new Hashtable();
        environmentalVariables.put("com.sun.jndi.ldap.connect.pool", StrUtl.STRING_TRUE);
        environmentalVariables.put(Context.PROVIDER_URL, getPropertyValue("domain_url", null));
        environmentalVariables.put("java.naming.ldap.attributes.binary", "tokenGroups objectSid");
        environmentalVariables.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environmentalVariables.put(Context.SECURITY_PRINCIPAL, anAcountDN);
        environmentalVariables.put(Context.SECURITY_CREDENTIALS, anAccountPassword);

// Referral options: follow, throw, ignore (default)

        environmentalVariables.put(Context.REFERRAL, getPropertyValue("referral_handling", "ignore"));

// Authentication options: simple, DIGEST-MD5 CRAM-MD5

        environmentalVariables.put(Context.SECURITY_AUTHENTICATION, getPropertyValue("authentication", "simple"));

        try
        {
            mLdapContext = new InitialLdapContext(environmentalVariables, null);
        }
        catch (NamingException e)
        {
            String msgStr = String.format("LDAP Context Error: %s", e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Opens a connection to Active Directory by establishing an initial LDAP
     * context.
     *
     * @throws NSException Thrown if an LDAP naming exception is occurs.
     */
    public void open()
        throws NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "open");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        open(getPropertyValue("account_name", null), getPropertyValue("account_password", null));

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Returns <i>true</i> if the Active Directory account and password are
     * valid (e.g. a context can be successfully established) or <i>false</i>
     * otherwise.
     *
     * @param anAccountName An Active Directory account name.
     * @param anAccountPassword An Active Directory account passowrd.
     *
     * @return <i>true</i> or <i>false</i>
     */
    @SuppressWarnings("unchecked")
    public boolean isAccountValid(String anAccountName, String anAccountPassword)
    {
        boolean isValid = false;
        Logger appLogger = mAppMgr.getLogger(this, "isAccountValid");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag userBag = schemaUserBag();
        userBag.setValueByName(LDAP_ACCOUNT_NAME, anAccountName);

        try
        {
            loadUserByAccountName(userBag);
            Hashtable environmentalVariables = new Hashtable();
            environmentalVariables.put("com.sun.jndi.ldap.connect.pool", StrUtl.STRING_TRUE);
            environmentalVariables.put(Context.PROVIDER_URL, getPropertyValue("domain_url", null));
            environmentalVariables.put("java.naming.ldap.attributes.binary", "tokenGroups objectSid");
            environmentalVariables.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            environmentalVariables.put(Context.SECURITY_PRINCIPAL, userBag.getValueAsString(LDAP_DISTINGUISHED_NAME));
            environmentalVariables.put(Context.SECURITY_CREDENTIALS, anAccountPassword);
            environmentalVariables.put(Context.REFERRAL, getPropertyValue("referral_handling", "ignore"));
            environmentalVariables.put(Context.SECURITY_AUTHENTICATION, getPropertyValue("authentication", "simple"));

            LdapContext ldapContext = new InitialLdapContext(environmentalVariables, null);
            ldapContext.close();

            isValid = true;
        }
        catch (Exception ignored)
        {
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return isValid;
    }

    /**
     * Converts a security identifier from its binary representation
     * to a string one.  This approach to the conversion was never
     * tested.
     *
     * @param anObjectSid Binary representation of a security identifier.
     *
     * @return String representation of the security identifier.
     */
    private String objectSidToString1(byte[] anObjectSid)
    {
        StringBuilder stringObjectSid = new StringBuilder("S-");

// bytes[0] : Is the version (assume 0 for now could change in the future.

        stringObjectSid.append(anObjectSid[0]).append('-');

// bytes[2..7] : The Authority

        StringBuilder authorityBuffer = new StringBuilder();
        for (int t = 2; t <= 7; t++)
        {
            String hexString = Integer.toHexString(anObjectSid[t] & 0xFF);
            authorityBuffer.append(hexString);
        }
        stringObjectSid.append(Long.parseLong(authorityBuffer.toString(), 16));

// bytes[1] : The sub authorities count

        int count = anObjectSid[1];

// bytes[8..end] : The sub authorities (these are Integers - notice the endian)

        int curSubAuthorityOffset;
        for (int i = 0; i < count; i++)
        {
            curSubAuthorityOffset = i * 4;
            authorityBuffer.setLength(0);
            authorityBuffer.append(String.format("%02X%02X%02X%02X",
                (anObjectSid[11 + curSubAuthorityOffset] & 0xFF),
                (anObjectSid[10 + curSubAuthorityOffset] & 0xFF),
                (anObjectSid[9 + curSubAuthorityOffset] & 0xFF),
                (anObjectSid[8 + curSubAuthorityOffset] & 0xFF)));

            stringObjectSid.append('-').append(Long.parseLong(authorityBuffer.toString(), 16));
        }

        return stringObjectSid.toString();
    }

    /**
     * Converts a security identifier from its binary representation
     * to a string one.  This approach to the conversion is confirmed
     * as working.
     *
     * @param anObjectSid Binary representation of a security identifier.
     *
     * @return String representation of the security identifier.
     */
    private String objectSidToString2(byte[] anObjectSid)
    {
        String stringObjectSid = "S";
        long version = anObjectSid[0];
        stringObjectSid = stringObjectSid + "-" + Long.toString(version);

        long primaryAuthority = anObjectSid[4];
        for (int i = 0; i < 4; i++)
        {
            primaryAuthority <<= 8;
            primaryAuthority += anObjectSid[4 + i] & 0xFF;
        }
        stringObjectSid = stringObjectSid + "-" + Long.toString(primaryAuthority);

        long rid;
        long subAuthorityCount = anObjectSid[2];
        subAuthorityCount <<= 8;
        subAuthorityCount += anObjectSid[1] & 0xFF;
        for (int j = 0; j < subAuthorityCount; j++)
        {
            rid = anObjectSid[11 + (j * 4)] & 0xFF;
            for (int k = 1; k < 4; k++)
            {
                rid <<= 8;
                rid += anObjectSid[11 - k + (j * 4)] & 0xFF;
            }
            stringObjectSid = stringObjectSid + "-" + Long.toString(rid);
        }

        return stringObjectSid;
    }

    /**
     * Queries Active Directory for attributes defined within the bag.
     * The LDAP_ACCOUNT_NAME field must be populated prior to invoking
     * this method.  Any site specific fields can be assigned to the
     * bag will be included in the attribute query.
     *
     * @param aUserBag Active Directory user fields.
     *
     * @throws NSException Thrown if an LDAP naming exception is occurs.
     */
    public void loadUserByAccountName(DataBag aUserBag)
        throws NSException
    {
        byte[] objectSid;
        Attribute responseAttribute;
        String fieldName, fieldValue;
        Attributes responseAttributes;
        Logger appLogger = mAppMgr.getLogger(this, "loadUserByAccountName");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mLdapContext == null)
        {
            String msgStr = "LDAP context has not been established.";
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        int field = 0;
        String accountName = null;
        int attrCount = aUserBag.count();
        String[] ldapAttrNames = new String[attrCount];
        for (DataField complexField : aUserBag.getFields())
        {
            fieldName = complexField.getName();
            if (fieldName.equals(LDAP_ACCOUNT_NAME))
                accountName = complexField.getValueAsString();
            ldapAttrNames[field++] = fieldName;
        }
        searchControls.setReturningAttributes(ldapAttrNames);

        if (accountName == null)
        {
            String msgStr = String.format("LDAP account name '%s' is unassigned.", LDAP_ACCOUNT_NAME);
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        String userSearchBaseDN = getPropertyValue("user_searchbasedn", null);
        String userSearchFilter = String.format("(&(objectClass=user)(%s=%s))",
                                                LDAP_ACCOUNT_NAME, accountName);
        try
        {
            NamingEnumeration searchResponse = mLdapContext.search(userSearchBaseDN, userSearchFilter, searchControls);
            if ((searchResponse != null) && (searchResponse.hasMore()))
            {
                responseAttributes = ((SearchResult) searchResponse.next()).getAttributes();
                for (DataField complexField : aUserBag.getFields())
                {
                    fieldName = complexField.getName();
                    responseAttribute = responseAttributes.get(fieldName);
                    if (responseAttribute != null)
                    {
                        if (fieldName.equals(LDAP_OBJECT_SID))
                        {
                            objectSid = (byte[]) responseAttribute.get();
                            fieldValue = objectSidToString2(objectSid);
                        }
                        else
                            fieldValue = (String) responseAttribute.get();
                        if (StringUtils.isNotEmpty(fieldValue))
                            complexField.setValue(fieldValue);
                    }
                }
                searchResponse.close();
            }
        }
        catch (NamingException e)
        {
            String msgStr = String.format("LDAP Search Error (%s): %s", userSearchFilter, e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Queries Active Directory for attributes defined within the bag.
     * The LDAP_COMMON_NAME field must be populated prior to invoking
     * this method.  Any site specific fields can be assigned to the
     * bag will be included in the attribute query.
     *
     * @param aUserBag Active Directory user fields.
     *
     * @throws NSException Thrown if an LDAP naming exception is occurs.
     */
    public void loadUserByCommonName(DataBag aUserBag)
        throws NSException
    {
        byte[] objectSid;
        Attribute responseAttribute;
        String fieldName, fieldValue;
        Attributes responseAttributes;
        Logger appLogger = mAppMgr.getLogger(this, "loadUserByCommonName");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mLdapContext == null)
        {
            String msgStr = "LDAP context has not been established.";
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        int field = 0;
        String commonName = null;
        int attrCount = aUserBag.count();
        String[] ldapAttrNames = new String[attrCount];
        for (DataField complexField : aUserBag.getFields())
        {
            fieldName = complexField.getName();
            if (fieldName.equals(LDAP_COMMON_NAME))
                commonName = complexField.getValueAsString();
            ldapAttrNames[field++] = fieldName;
        }
        searchControls.setReturningAttributes(ldapAttrNames);

        if (commonName == null)
        {
            String msgStr = String.format("LDAP common name '%s' is unassigned.", LDAP_COMMON_NAME);
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        String userSearchBaseDN = getPropertyValue("user_searchbasedn", null);
        String userSearchFilter = String.format("(&(objectClass=user)(%s=%s))",
            LDAP_COMMON_NAME, commonName);
        try
        {
            NamingEnumeration searchResponse = mLdapContext.search(userSearchBaseDN, userSearchFilter, searchControls);
            if ((searchResponse != null) && (searchResponse.hasMore()))
            {
                responseAttributes = ((SearchResult) searchResponse.next()).getAttributes();
                for (DataField complexField : aUserBag.getFields())
                {
                    fieldName = complexField.getName();
                    responseAttribute = responseAttributes.get(fieldName);
                    if (responseAttribute != null)
                    {
                        if (fieldName.equals(LDAP_OBJECT_SID))
                        {
                            objectSid = (byte[]) responseAttribute.get();
                            fieldValue = objectSidToString2(objectSid);
                        } else
                            fieldValue = (String) responseAttribute.get();
                        if (StringUtils.isNotEmpty(fieldValue))
                            complexField.setValue(fieldValue);
                    }
                }
                searchResponse.close();
            }
        }
        catch (NamingException e)
        {
            String msgStr = String.format("LDAP Search Error (%s): %s", userSearchFilter, e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * Queries Active Directory for attributes defined within the bag.
     * The LDAP_ACCOUNT_NAME field must be populated prior to invoking
     * this method.  Any site specific fields can be assigned to the
     * bag will be included in the attribute query.
     *
     * @param aGroupBag Active Directory group fields.
     *
     * @throws NSException Thrown if an LDAP naming exception is occurs.
     */
    public void loadGroupByAccountName(DataBag aGroupBag)
        throws NSException
    {
        byte[] objectSid;
        Attribute responseAttribute;
        String fieldName, fieldValue;
        Attributes responseAttributes;
        Logger appLogger = mAppMgr.getLogger(this, "loadGroupByAccountName");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mLdapContext == null)
        {
            String msgStr = "LDAP context has not been established.";
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

        int field = 0;
        String accountName = null;
        int attrCount = aGroupBag.count();
        String[] ldapAttrNames = new String[attrCount];
        for (DataField complexField : aGroupBag.getFields())
        {
            fieldName = complexField.getName();
            if (fieldName.equals(LDAP_ACCOUNT_NAME))
                accountName = complexField.getValueAsString();
            ldapAttrNames[field++] = fieldName;
        }
        searchControls.setReturningAttributes(ldapAttrNames);

        if (accountName == null)
        {
            String msgStr = String.format("LDAP account name '%s' is unassigned.", LDAP_ACCOUNT_NAME);
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        String groupSearchBaseDN = getPropertyValue("group_searchbasedn", null);
        String groupSearchFilter = String.format("(&(objectClass=group)(%s=%s))",
                                                  LDAP_ACCOUNT_NAME, accountName);
        try
        {
            NamingEnumeration searchResponse = mLdapContext.search(groupSearchBaseDN, groupSearchFilter, searchControls);
            if ((searchResponse != null) && (searchResponse.hasMore()))
            {
                responseAttributes = ((SearchResult) searchResponse.next()).getAttributes();
                for (DataField complexField : aGroupBag.getFields())
                {
                    fieldName = complexField.getName();
                    responseAttribute = responseAttributes.get(fieldName);
                    if (responseAttribute != null)
                    {
                        if (fieldName.equals(LDAP_OBJECT_SID))
                        {
                            objectSid = (byte[]) responseAttribute.get();
                            fieldValue = objectSidToString2(objectSid);
                        }
                        else
                            fieldValue = (String) responseAttribute.get();
                        if (StringUtils.isNotEmpty(fieldValue))
                            complexField.setValue(fieldValue);
                    }
                }
                searchResponse.close();
            }
        }
        catch (NamingException e)
        {
            String msgStr = String.format("LDAP Search Error (%s): %s", groupSearchFilter, e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private String extractCommonName(String aMemberName)
    {
        String commonName = aMemberName;

        int startOffset = aMemberName.indexOf(StrUtl.CHAR_EQUAL);
        if (startOffset != -1)
        {
            int endOffset = aMemberName.indexOf(StrUtl.CHAR_COMMA);
            if (endOffset != -1)
                commonName = aMemberName.substring(startOffset + 1, endOffset);
        }

        return commonName;
    }

    /**
     * This method will perform multiple queries into Active Directory
     * in order to resolve what groups a user is a member of.  The
     * logic will identify nested groups and add them to the table.
     * <p>
     * The LDAP_ACCOUNT_NAME field must be populated in the user bag
     * prior to invoking this method.  Any site specific fields can be
     * assigned to the user bag will be included in the attribute query.
     * </p>
     * <p>
     * Any site specific fields can be assigned to the group bag will
     * be included in the attribute query.
     * </p>
     *
     * @param aUserBag Active Directory user attributes.
     * @param aGroupBag Active Directory group attributes.
     *
     * @return Table of groups that the user is a member of.
     *
     * @throws NSException Thrown if an LDAP naming exception is occurs.
     */
    @SuppressWarnings("StringConcatenationInsideStringBufferAppend")
    public DataTable loadUserGroupsByAccountName(DataBag aUserBag, DataBag aGroupBag)
        throws NSException
    {
        byte[] objectSid;
        DataBag groupBag;
        Attribute responseAttribute;
        String fieldName, fieldValue;
        Logger appLogger = mAppMgr.getLogger(this, "loadUserGroupsByAccountName");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mLdapContext == null)
        {
            String msgStr = "LDAP context has not been established.";
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

// First, we will populate our user bag so that we can obtain the distinguished name.

        loadUserByAccountName(aUserBag);

// Now we will use the DN to find all of the groups the user is a member of.

        String distinguishedName = aUserBag.getValueAsString(LDAP_DISTINGUISHED_NAME);
        if (StringUtils.isEmpty(distinguishedName))
            distinguishedName = getPropertyValue("user_searchbasedn", null);

// Next, we will initialize our group membership table.

        DataTable memberTable = new DataTable(aUserBag);
        memberTable.setName(String.format("%s Group Membership", aUserBag.getValueAsString(LDAP_COMMON_NAME)));

// The next logic section will query AD for all of the groups the user is a member
// of.  Because we are following tokenGroups, we will gain access to nested groups.

        String groupSearchBaseDN = getPropertyValue("group_searchbasedn", null);

        SearchControls userSearchControls = new SearchControls();
        userSearchControls.setSearchScope(SearchControls.OBJECT_SCOPE);

        StringBuffer groupsSearchFilter = null;
        String ldapAttrNames[] = {"tokenGroups"};
        userSearchControls.setReturningAttributes(ldapAttrNames);

        try
        {
            NamingEnumeration userSearchResponse = mLdapContext.search(distinguishedName, "(objectClass=user)",
                                                                       userSearchControls);
            if ((userSearchResponse != null) && (userSearchResponse.hasMoreElements()))
            {
                groupsSearchFilter = new StringBuffer();
                groupsSearchFilter.append("(|");

                SearchResult userSearchResult = (SearchResult) userSearchResponse.next();
                Attributes userResultAttributes = userSearchResult.getAttributes();
                if (userResultAttributes != null)
                {
                    try
                    {
                        for (NamingEnumeration searchResultAttributesAll = userResultAttributes.getAll();
                             searchResultAttributesAll.hasMore();)
                        {
                            Attribute attr = (Attribute) searchResultAttributesAll.next();
                            for (NamingEnumeration namingEnumeration = attr.getAll(); namingEnumeration.hasMore();)
                            {
                                objectSid = (byte[]) namingEnumeration.next();
                                groupsSearchFilter.append("(objectSid=" + objectSidToString2(objectSid) + ")");
                            }
                            groupsSearchFilter.append(")");
                        }
                    }
                    catch (NamingException e)
                    {
                        String msgStr = String.format("LDAP Listing Member Exception: %s", e.getMessage());
                        appLogger.error(msgStr, e);
                        throw new NSException(msgStr);
                    }
                }
                userSearchResponse.close();

// Finally, we will query each group in the search filter and add it to the table.

                SearchControls groupSearchControls = new SearchControls();
                groupSearchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                int field = 0;
                int attrCount = aGroupBag.count();
                String[] groupsReturnedAtts = new String[attrCount];
                for (DataField complexField : aGroupBag.getFields())
                {
                    fieldName = complexField.getName();
                    groupsReturnedAtts[field++] = fieldName;
                }
                groupSearchControls.setReturningAttributes(groupsReturnedAtts);
                NamingEnumeration groupSearchResponse = mLdapContext.search(groupSearchBaseDN,
                    groupsSearchFilter.toString(),
                    groupSearchControls);
                while ((groupSearchResponse != null) && (groupSearchResponse.hasMoreElements()))
                {
                    SearchResult groupSearchResult = (SearchResult) groupSearchResponse.next();
                    Attributes groupResultAttributes = groupSearchResult.getAttributes();
                    if (groupResultAttributes != null)
                    {
                        groupBag = new DataBag(aGroupBag);
                        for (DataField complexField : groupBag.getFields())
                        {
                            fieldName = complexField.getName();
                            responseAttribute = groupResultAttributes.get(fieldName);
                            if (responseAttribute != null)
                            {
                                if (fieldName.equals(LDAP_OBJECT_SID))
                                {
                                    objectSid = (byte[]) responseAttribute.get();
                                    fieldValue = objectSidToString2(objectSid);
                                } else
                                    fieldValue = (String) responseAttribute.get();
                                if (StringUtils.isNotEmpty(fieldValue))
                                    complexField.setValue(fieldValue);
                            }
                        }
                        memberTable.addRow(groupBag);
                    }
                }
                if (groupSearchResponse != null)
                    groupSearchResponse.close();
            }
        }
        catch (NamingException e)
        {
            String msgStr = String.format("LDAP Search Error (%s): %s", distinguishedName, e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return memberTable;
    }

    /**
     * Closes a previously opened <i>LdapContext</i> and releases
     * any resources associated with naming enumerations used as
     * cursors into Active Directory result sets.
     */
    public void close()
    {
        Logger appLogger = mAppMgr.getLogger(this, "close");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mLdapContext != null)
        {
            try
            {
                mLdapContext.close();
            }
            catch (NamingException e)
            {
                String msgStr = String.format("LDAP Close Exception: %s", e.getMessage());
                appLogger.warn(msgStr);
            }
            finally
            {
                mLdapContext = null;
            }
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
