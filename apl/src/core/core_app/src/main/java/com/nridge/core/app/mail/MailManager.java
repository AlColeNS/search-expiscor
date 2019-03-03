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

package com.nridge.core.app.mail;

import com.nridge.core.app.mgr.AppMgr;
import com.nridge.core.base.field.Field;
import com.nridge.core.base.field.data.*;
import com.nridge.core.base.io.console.DataBagConsole;
import com.nridge.core.base.io.console.DataTableConsole;
import com.nridge.core.base.std.NSException;
import com.nridge.core.base.std.StrUtl;
import freemarker.template.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import freemarker.template.DefaultObjectWrapper;
import freemarker.template.Template;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The MailManager is responsible capturing messages and
 * sending them to email recipients based on configuration
 * settings. The class offers a flexible template approach
 * to message generation via Freemarker Templates.
 *
 * @see <a href="http://javamail.kenai.com/nonav/javadocs/com/sun/mail/smtp/package-summary.html">Mail Reference</a>
 * @see <a href="http://crunchify.com/java-mailapi-example-send-an-email-via-gmail-smtp/">GMail SMTP (TLS Authentication)</a>
 * @see <a href="http://freemarker.org/">Freemarker Template</a>
 */
public class MailManager
{
    private AppMgr mAppMgr;
    private DataTable mTable;
    private Session mMailSession;
    private Configuration mConfiguration;
    private String mCfgPropertyPrefix = Mail.CFG_PROPERTY_PREFIX;

    public MailManager(AppMgr anAppMgr)
    {
        mAppMgr = anAppMgr;
        mTable = new DataTable(schemaServiceMailBag());
    }

    public MailManager(AppMgr anAppMgr, DataBag aBag)
    {
        mAppMgr = anAppMgr;
        mTable = new DataTable(aBag);
    }

    public MailManager(AppMgr anAppMgr, String aPropertyPrefix)
    {
        mAppMgr = anAppMgr;
        mCfgPropertyPrefix = aPropertyPrefix;
        mTable = new DataTable(schemaServiceMailBag());
    }

    /**
     * Returns the configuration property prefix string.
     *
     * @return Property prefix string.
     */
    public String getCfgPropertyPrefix()
    {
        return mCfgPropertyPrefix;
    }

    /**
     * Assigns the configuration property prefix to the document data source.
     *
     * @param aPropertyPrefix Property prefix.
     */
    public void setCfgPropertyPrefix(String aPropertyPrefix)
    {
        mCfgPropertyPrefix = aPropertyPrefix;
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.
     *
     * @param aSuffix Property name suffix.
     * @return Matching property value.
     */
    public String getCfgString(String aSuffix)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName);
    }

    /**
     * Convenience method that returns the value of an application
     * manager configuration property using the concatenation of
     * the property prefix and suffix values.  If the property is
     * not found, then the default value parameter will be returned.
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value.
     *
     * @return Matching property value or the default value.
     */
    public String getCfgString(String aSuffix, String aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getString(propertyName, aDefaultValue);
    }

    /**
     * Returns a typed value for the property name identified
     * or the default value (if unmatched).
     *
     * @param aSuffix Property name suffix.
     * @param aDefaultValue Default value to return if property
     *                      name is not matched.
     *
     * @return Value of the property.
     */
    public int getCfgInteger(String aSuffix, int aDefaultValue)
    {
        String propertyName;

        if (StringUtils.startsWith(aSuffix, "."))
            propertyName = mCfgPropertyPrefix + aSuffix;
        else
            propertyName = mCfgPropertyPrefix + "." + aSuffix;

        return mAppMgr.getInt(propertyName, aDefaultValue);
    }

    /**
     * Returns <i>true</i> if the application manager configuration
     * property value evaluates to <i>true</i>.
     *
     * @param aSuffix Property name suffix.
     *
     * @return <i>true</i> or <i>false</i>
     */
    public boolean isCfgStringTrue(String aSuffix)
    {
        String propertyValue = getCfgString(aSuffix);
        return StrUtl.stringToBoolean(propertyValue);
    }

    /**
     * Performs a property lookup for the from address.
     *
     * @return Email address from property file.
     *
     * @throws NSException Property is undefined.
     */
    public String lookupFromAddress()
        throws NSException
    {
        String propertyName = "address_from";
        String mailAddressFrom = getCfgString(propertyName);
        if (StringUtils.isEmpty(mailAddressFrom))
        {
            String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
            throw new NSException(msgStr);
        }

        return mailAddressFrom;
    }

    /**
     * Convenience method that assigns the recipient email address to an array
     * list.
     *
     * @param anEmailAddress Single email address.
     *
     * @return Array list of email address strings.
     */
    public ArrayList<String> createRecipientList(String anEmailAddress)
    {
        ArrayList<String> recipientList = new ArrayList<String>();
        recipientList.add(anEmailAddress);

        return recipientList;
    }

    /**
     * Convenience method that assigns the recipient email addresses
     * defined in the application property file to an array list.
     *
     * @return Array list of email address strings.
     */
    public ArrayList<String> createRecipientList()
        throws NSException
    {
        ArrayList<String> recipientList = new ArrayList<String>();

        String propertyName = "address_to";
        if (mAppMgr.isPropertyMultiValue(getCfgString(propertyName)))
        {
            String[] addressToArray = mAppMgr.getStringArray(getCfgString(propertyName));
            for (String mailAddressTo : addressToArray)
                recipientList.add(mailAddressTo);
        }
        else
        {
            String mailAddressTo = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailAddressTo))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                throw new NSException(msgStr);
            }
        }

        return recipientList;
    }

    /**
     * Convenience method that assigns a single attachment to an array
     * list.
     *
     * @param anAttachmentPathFileName Attachment path file name.
     *
     * @return Array list of attachment path/file names.
     */
    public ArrayList<String> createAttachmentList(String anAttachmentPathFileName)
    {
        ArrayList<String> attachmentPathFileList = new ArrayList<String>();
        attachmentPathFileList.add(anAttachmentPathFileName);

        return attachmentPathFileList;
    }

    /**
     * This method will create a mail bag with one field for a
     * message description.
     *
     * @return Data bag instance.
     */
    public DataBag schemaMailBag()
    {
        DataBag dataBag = new DataBag("Application Mail Manager");

        DataTextField dataTextField = new DataTextField("msg_description", "Message Description");
        dataBag.add(dataTextField);

        return dataBag;
    }

    /**
     * This method will create a mail bag of fields suitable for
     * capturing a table of messages that could describe the result
     * of a batch operation like a connector service.
     *
     * @return Data bag instance.
     */
    public DataBag schemaServiceMailBag()
    {
        DataBag dataBag = new DataBag("Application Service Mail Manager");

        DataDateTimeField dataDateTimeField = new DataDateTimeField("msg_ts", "Message Timestamp");
        dataDateTimeField.setDefaultValue(Field.VALUE_DATETIME_TODAY);
        dataBag.add(dataDateTimeField);

        DataTextField dataTextField = new DataTextField("msg_operation", "Message Operation");
        dataTextField.enableFeature(Field.FEATURE_IS_REQUIRED);
        dataBag.add(dataTextField);

        dataTextField = new DataTextField("msg_status", "Message Status");
        dataTextField.setDefaultValue(Mail.STATUS_SUCCESS);
        dataBag.add(dataTextField);

        dataTextField = new DataTextField("msg_description", "Message Description");
        dataTextField.setDefaultValue(Mail.MESSAGE_NONE);
        dataBag.add(dataTextField);

        dataTextField = new DataTextField("msg_detail", "Message Detail");
        dataTextField.setDefaultValue(Mail.MESSAGE_NONE);
        dataBag.add(dataTextField);

        dataBag.resetValuesWithDefaults();

        return dataBag;
    }

    private void initialize()
        throws IOException, NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "initialize");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (mMailSession == null)
        {
            String propertyName = "account_name";
            String mailAccountName = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailAccountName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                appLogger.error(msgStr);
                throw new NSException(msgStr);
            }
            propertyName = "account_password";
            String mailAccountPassword = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailAccountPassword))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                appLogger.error(msgStr);
                throw new NSException(msgStr);
            }

            MailAuthenticator mailAuthenticator = new MailAuthenticator(mailAccountName, mailAccountPassword);

            propertyName = "smtp_host";
            String smtpHostName = getCfgString(propertyName);
            if (StringUtils.isEmpty(smtpHostName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                appLogger.error(msgStr);
                throw new NSException(msgStr);
            }
            propertyName = "smtp_port";
            String smtpPortNumber = getCfgString(propertyName);
            if (StringUtils.isEmpty(smtpHostName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                appLogger.error(msgStr);
                throw new NSException(msgStr);
            }

            Properties systemProperties = new Properties();

            systemProperties.setProperty("mail.smtp.submitter", mailAccountName);
            systemProperties.setProperty("mail.smtp.host", smtpHostName);
            systemProperties.setProperty("mail.smtp.port", smtpPortNumber);
            if (isCfgStringTrue("authn_enabled"))
            {
                systemProperties.setProperty("mail.smtp.auth", "true");
                systemProperties.setProperty("mail.smtp.starttls.enable", "true");
                mMailSession = Session.getInstance(systemProperties, mailAuthenticator);
            }
            else
                mMailSession = Session.getInstance(systemProperties);

            mConfiguration = new Configuration(Configuration.VERSION_2_3_21);
            String cfgPathName = mAppMgr.getString(mAppMgr.APP_PROPERTY_CFG_PATH);
            File cfgPathFile = new File(cfgPathName);
            try
            {
                mConfiguration.setDirectoryForTemplateLoading(cfgPathFile);
            }
            catch (IOException e)
            {
                appLogger.error(cfgPathName, e);
            }
            mConfiguration.setObjectWrapper(new DefaultObjectWrapper(Configuration.VERSION_2_3_21));
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    private String createMessage(DataBag aBag)
        throws IOException, NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createMessage");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        DataBagConsole dataBagConsole = new DataBagConsole(aBag);
        dataBagConsole.setUseTitleFlag(true);
        dataBagConsole.writeBag(printWriter, aBag.getTitle());
        printWriter.close();

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    private String createMessage(DataBag aBag, String aTemplateFileName)
        throws IOException, NSException
    {
        Logger appLogger = mAppMgr.getLogger(this, "createMessage");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        initialize();

        Map<String, Object> dataModel = new HashMap<String, Object>();
        for (DataField dataField : aBag.getFields())
            dataModel.put(dataField.getName(), dataField.getValue());

        StringWriter stringWriter = new StringWriter();
        try
        {
            Template fmTemplate = mConfiguration.getTemplate(aTemplateFileName);
            fmTemplate.process(dataModel, stringWriter);
        }
        catch (Exception e)
        {
            String msgStr = String.format("%s: %s", aTemplateFileName, e.getMessage());
            appLogger.error(msgStr, e);
            throw new NSException(msgStr);
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return stringWriter.toString();
    }

    /**
     * Returns the number of messages previously stored in the internally
     * managed table.
     *
     * @return Count of messages.
     */
    public int messageCount()
    {
        return mTable.rowCount();
    }

    /**
     * Empties the internally managed table of any messages.
     */
    public void reset()
    {
        synchronized(this)
        {
            mTable.emptyRows();
        }
    }

    /**
     * Adds the fields contained within the data bag to the internally
     * managed table.  Refer to <code>schemaMailBag</code> and
     * <code>schemaServiceMailBag</code> methods.
     *
     * @param aBag Data bag instance.
     *
     * @return <i>true</i> if the message is valid and added.  <i>false</i>
     * otherwise.
     */
    public boolean addMessage(DataBag aBag)
    {
        boolean isValid;
        Logger appLogger = mAppMgr.getLogger(this, "addMessage");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if ((aBag == null) || (! aBag.isValid()))
            isValid = false;
        else
        {
            synchronized(this)
            {
                mTable.addRow(aBag);
            }
            isValid = true;
        }

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return isValid;
    }

    /**
     * Adds the fields contained within the data bag to the internally
     * managed table.  Refer to <code>schemaServiceMailBag</code>
     * method.
     *
     * @param anOperation Application defined operation string.
     * @param aStatus Status message of operation.
     * @param aDescription Description of the operation.
     * @param aDetail Details around the operation.
     *
     * @return <i>true</i> if the message is valid and added.  <i>false</i>
     * otherwise.
     */
    public boolean addMessage(String anOperation, String aStatus,
                              String aDescription, String aDetail)
    {
        Logger appLogger = mAppMgr.getLogger(this, "addMessage");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        DataBag dataBag = schemaServiceMailBag();
        dataBag.setValueByName("msg_operation", anOperation);
        dataBag.setValueByName("msg_status", aStatus);
        dataBag.setValueByName("msg_description", aDescription);
        dataBag.setValueByName("msg_detail", aDetail);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);

        return addMessage(dataBag);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this
     * method will deliver the subject and message via an email
     * transport (e.g. SMTP).
     *
     * @param aSubject Message subject.
     * @param aMessage Message content.
     *
     * @throws IOException I/O related error condition.
     * @throws NSException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessage(String aSubject, String aMessage)
        throws IOException, NSException, MessagingException
    {
        InternetAddress internetAddressFrom, internetAddressTo;
        Logger appLogger = mAppMgr.getLogger(this, "sendMessage");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isCfgStringTrue("delivery_enabled"))
        {
            if ((StringUtils.isNotEmpty(aSubject)) && (StringUtils.isNotEmpty(aMessage)))
            {
                initialize();

                String propertyName = "address_to";
                Message mimeMessage = new MimeMessage(mMailSession);
                if (mAppMgr.isPropertyMultiValue(getCfgString(propertyName)))
                {
                    String[] addressToArray = mAppMgr.getStringArray(getCfgString(propertyName));
                    for (String mailAddressTo : addressToArray)
                    {
                        internetAddressTo = new InternetAddress(mailAddressTo);
                        mimeMessage.addRecipient(MimeMessage.RecipientType.TO, internetAddressTo);
                    }
                }
                else
                {
                    String mailAddressTo = getCfgString(propertyName);
                    if (StringUtils.isEmpty(mailAddressTo))
                    {
                        String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                        appLogger.error(msgStr);
                        throw new NSException(msgStr);
                    }
                    internetAddressTo = new InternetAddress(mailAddressTo);
                    mimeMessage.addRecipient(MimeMessage.RecipientType.TO, internetAddressTo);
                }
                propertyName = "address_from";
                String mailAddressFrom = getCfgString(propertyName);
                if (StringUtils.isEmpty(mailAddressFrom))
                {
                    String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                    appLogger.error(msgStr);
                    throw new NSException(msgStr);
                }
                internetAddressFrom = new InternetAddress(mailAddressFrom);
                mimeMessage.addFrom(new InternetAddress[]{internetAddressFrom});
                mimeMessage.setSubject(aSubject);
                mimeMessage.setContent(aMessage, "text/plain");

                appLogger.debug(String.format("Mail Message (%s): %s", aSubject, aMessage));

                Transport.send(mimeMessage);
            }
            else
                throw new NSException("Subject and message are required parameters.");
        }
        else
            appLogger.warn("Email delivery is not enabled - no message will be sent.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this
     * method will deliver the subject and messages stored in the
     * internal table via an email transport (e.g. SMTP).
     *
     * @param aSubject Message subject.
     *
     * @throws IOException I/O related error condition.
     * @throws NSException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessageTable(String aSubject)
        throws IOException, NSException, MessagingException
    {
        Logger appLogger = mAppMgr.getLogger(this, "sendMessageTable");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        int rowCount = mTable.rowCount();
        if (rowCount > 0)
        {
            String propertyName = "template_service_file";
            String mailTemplatePathFileName = getCfgString(propertyName);
            if (StringUtils.isEmpty(mailTemplatePathFileName))
            {
                String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
                appLogger.error(msgStr);
                throw new NSException(msgStr);
            }

            DataBag dataBag = new DataBag("Mail Message Bag");
            dataBag.add(new DataTextField("msg_table", "Message Table"));

            synchronized(this)
            {
                StringWriter stringWriter = new StringWriter();
                PrintWriter printWriter = new PrintWriter(stringWriter);
                DataTableConsole dataTableConsole = new DataTableConsole(mTable);
                dataTableConsole.write(printWriter, StringUtils.EMPTY);
                printWriter.close();
                dataBag.setValueByName("msg_table", stringWriter.toString());
            }

            String messageBody = createMessage(dataBag, mailTemplatePathFileName);
            sendMessage(aSubject, messageBody);
        }
        else
            throw new NSException("The message table is empty - nothing to send.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this
     * method will deliver the subject and data bag via an email
     * transport (e.g. SMTP).
     *
     * @param aSubject Message subject.
     * @param aBag Data bag instance of fields.
     *
     * @throws IOException I/O related error condition.
     * @throws NSException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessageBag(String aSubject, DataBag aBag)
        throws IOException, NSException, MessagingException
    {
        Logger appLogger = mAppMgr.getLogger(this, "sendMessageBag");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        String propertyName = "template_service_file";
        String mailTemplatePathFileName = getCfgString(propertyName);
        if (StringUtils.isEmpty(mailTemplatePathFileName))
        {
            String msgStr = String.format("Mail Manager property '%s' is undefined.", mCfgPropertyPrefix + "." + propertyName);
            appLogger.error(msgStr);
            throw new NSException(msgStr);
        }

        String messageBody = createMessage(aBag, mailTemplatePathFileName);
        sendMessage(aSubject, messageBody);

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }

    /**
     * If the property "delivery_enabled" is <i>true</i>, then this method
     * will generate an email message that includes subject, message and
     * attachments to the recipient list.  You can use the convenience
     * methods <i>lookupFromAddress()</i>, <i>createRecipientList()</i>
     * and <i>createAttachmentList()</i> for parameter building assistance.
     *
     * @param aFromAddress Source email address.
     * @param aRecipientList List of recipient email addresses.
     * @param aSubject Subject of the email message.
     * @param aMessage Messsage.
     * @param anAttachmentFiles List of file attachments or <i>null</i> for none.
     *
     * @see <a href="https://www.tutorialspoint.com/javamail_api/javamail_api_send_email_with_attachment.htm">JavaMail API Attachments</a>
     * @see <a href="https://stackoverflow.com/questions/6756162/how-do-i-send-mail-with-both-plain-text-as-well-as-html-text-so-that-each-mail-r">JavaMail API MIME Types</a>
     *
     * @throws IOException I/O related error condition.
     * @throws NSException Missing configuration properties.
     * @throws MessagingException Message subsystem error condition.
     */
    public void sendMessage(String aFromAddress, ArrayList<String> aRecipientList,
                            String aSubject, String aMessage,
                            ArrayList<String>  anAttachmentFiles)

        throws IOException, NSException, MessagingException
    {
        InternetAddress internetAddressFrom, internetAddressTo;
        Logger appLogger = mAppMgr.getLogger(this, "sendMessage");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_ENTER);

        if (isCfgStringTrue("delivery_enabled"))
        {
            if ((StringUtils.isNotEmpty(aFromAddress)) && (aRecipientList.size() > 0) &&
                (StringUtils.isNotEmpty(aSubject)) && (StringUtils.isNotEmpty(aMessage)))
            {
                initialize();

                Message mimeMessage = new MimeMessage(mMailSession);
                internetAddressFrom = new InternetAddress(aFromAddress);
                mimeMessage.addFrom(new InternetAddress[]{internetAddressFrom});
                for (String mailAddressTo : aRecipientList)
                {
                    internetAddressTo = new InternetAddress(mailAddressTo);
                    mimeMessage.addRecipient(MimeMessage.RecipientType.TO, internetAddressTo);
                }
                mimeMessage.setSubject(aSubject);

// The following logic create a multi-part message and adds the attachment to it.

                BodyPart messageBodyPart = new MimeBodyPart();
                messageBodyPart.setText(aMessage);
//                messageBodyPart.setContent(aMessage, "text/html");
                Multipart multipart = new MimeMultipart();
                multipart.addBodyPart(messageBodyPart);

                if ((anAttachmentFiles != null) && (anAttachmentFiles.size() > 0))
                {
                    for (String pathFileName : anAttachmentFiles)
                    {
                        File attachmentFile = new File(pathFileName);
                        if (attachmentFile.exists())
                        {
                            messageBodyPart = new MimeBodyPart();
                            DataSource fileDataSource = new FileDataSource(pathFileName);
                            messageBodyPart.setDataHandler(new DataHandler(fileDataSource));
                            messageBodyPart.setFileName(attachmentFile.getName());
                            multipart.addBodyPart(messageBodyPart);
                        }
                    }
                    appLogger.debug(String.format("Mail Message (%s): %s - with attachments", aSubject, aMessage));
                }
                else
                    appLogger.debug(String.format("Mail Message (%s): %s", aSubject, aMessage));

                mimeMessage.setContent(multipart);
                Transport.send(mimeMessage);
            }
            else
                throw new NSException("Valid from, recipient, subject and message are required parameters.");
        }
        else
            appLogger.warn("Email delivery is not enabled - no message will be sent.");

        appLogger.trace(mAppMgr.LOGMSG_TRACE_DEPART);
    }
}
