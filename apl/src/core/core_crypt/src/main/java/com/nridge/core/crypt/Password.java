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

package com.nridge.core.crypt;

import org.apache.commons.lang3.StringUtils;
import org.jasypt.util.numeric.BasicDecimalNumberEncryptor;
import org.jasypt.util.numeric.BasicIntegerNumberEncryptor;
import org.jasypt.util.text.BasicTextEncryptor;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * The Password provides a collection of basic methods for encrypting
 * and decrypting messages using a password.  The process is bi-directional
 * so the approach is considered less secure.
 * <p>
 * This class utilizes the
 * <a href="http://www.jasypt.org/">jasypt</a>
 * framework to manage the transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class Password
{
    private String mSecret;

    /**
     * Default constructor.
     */
    public Password()
    {
        mSecret = StringUtils.EMPTY;
    }

    /**
     * Constructor that accepts a secret that will be used
     * for encryption.
     *
     * @param aSecret Secret string.
     */
    public Password(String aSecret)
    {
        setSecret(aSecret);
    }

    /**
     * Assigns a secret string for password encryption.
     *
     * @param aSecret Secret string.
     */
    public void setSecret(String aSecret)
    {
        mSecret = aSecret;
    }

    /**
     * Returns the secret string used for password
     * encryption.
     *
     * @return Secret string.
     */
    public String getSecret()
    {
        return mSecret;
    }

    /**
     * Encrypts the parameter using the secret string as the
     * password.
     *
     * @param aMessage Message to encrypt.
     *
     * @return An encrypted message.
     */
    public String encrypt(String aMessage)
    {
        BasicTextEncryptor basicTextEncryptor = new BasicTextEncryptor();
        basicTextEncryptor.setPassword(mSecret);

        return basicTextEncryptor.encrypt(aMessage);
    }

    /**
     * Decrypts a previously encrypted message using the secret
     * password string.
     *
     * @param anEncryptedMessage Encrypted message.
     *
     * @return Unencrypted message.
     */
    public String decrypt(String anEncryptedMessage)
    {
        BasicTextEncryptor basicTextEncryptor = new BasicTextEncryptor();
        basicTextEncryptor.setPassword(mSecret);

        return basicTextEncryptor.decrypt(anEncryptedMessage);
    }

    /**
     * Encrypts the parameter using the secret string as the
     * password.
     *
     * @param aNumber Number to encrypt.
     *
     * @return An encrypted number.
     */
    public BigInteger encrypt(BigInteger aNumber)
    {
        BasicIntegerNumberEncryptor basicIntegerNumberEncryptor = new BasicIntegerNumberEncryptor();
        basicIntegerNumberEncryptor.setPassword(mSecret);

        return basicIntegerNumberEncryptor.encrypt(aNumber);
    }

    /**
     * Decrypts a previously encrypted number using the secret
     * password string.
     *
     * @param aNumber Encrypted message.
     *
     * @return Unencrypted number.
     */
    public BigInteger decrypt(BigInteger aNumber)
    {
        BasicIntegerNumberEncryptor basicIntegerNumberEncryptor = new BasicIntegerNumberEncryptor();
        basicIntegerNumberEncryptor.setPassword(mSecret);

        return basicIntegerNumberEncryptor.decrypt(aNumber);
    }

    /**
     * Encrypts the parameter using the secret string as the
     * password.
     *
     * @param aNumber Number to encrypt.
     *
     * @return An encrypted number.
     */
    public BigDecimal encrypt(BigDecimal aNumber)
    {
        BasicDecimalNumberEncryptor basicDecimalNumberEncryptor = new BasicDecimalNumberEncryptor();
        basicDecimalNumberEncryptor.setPassword(mSecret);

        return basicDecimalNumberEncryptor.encrypt(aNumber);
    }

    /**
     * Decrypts a previously encrypted number using the secret
     * password string.
     *
     * @param aNumber Encrypted message.
     *
     * @return Unencrypted number.
     */
    public BigDecimal decrypt(BigDecimal aNumber)
    {
        BasicDecimalNumberEncryptor basicDecimalNumberEncryptor = new BasicDecimalNumberEncryptor();
        basicDecimalNumberEncryptor.setPassword(mSecret);

        return basicDecimalNumberEncryptor.decrypt(aNumber);
    }
}
