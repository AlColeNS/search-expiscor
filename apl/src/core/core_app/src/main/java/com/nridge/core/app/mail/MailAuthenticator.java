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

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;

/**
 * The MailAuthenticator is a helper class that manages the
 * authentication of an email message.
 *
 * @see <a href="http://www.tutorialspoint.com/javamail_api/javamail_api_authentication.htm">JavaMail API - Authentication</a>
 */
public class MailAuthenticator extends Authenticator
{
    private PasswordAuthentication mAuthentication;

    public MailAuthenticator(String anAccountName, String anAccountPassword)
    {
        mAuthentication = new PasswordAuthentication(anAccountName, anAccountPassword);
    }

    protected PasswordAuthentication getPasswordAuthentication()
    {
        return mAuthentication;
    }
}
