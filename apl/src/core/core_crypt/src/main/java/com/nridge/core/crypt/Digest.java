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

import org.jasypt.digest.StandardByteDigester;
import org.jasypt.digest.StandardStringDigester;
import org.jasypt.util.password.StrongPasswordEncryptor;

/**
 * The Digest provides a collection of basic methods for
 * hashing/digesting messages using a random salt algorithm.
 * The process is one directional - you cannot retrieve the
 * original message - only compare that two hash values are
 * identical.
 * <p>
 * This class utilizes the
 * <a href="http://www.jasypt.org/">jasypt</a>
 * framework to manage the transformations.
 * </p>
 *
 * @author Al Cole
 * @since 1.0
 */
public class Digest
{
    /**
     * Default constructor.
     */
    public Digest()
    {
    }

    /**
     * Process the parameter message using a hashing algorithm.
     *
     * @param aMessage Message to digest.
     *
     * @return Hashed value.
     */
    public String process(String aMessage)
    {
        StandardStringDigester standardStringDigester = new StandardStringDigester();
        standardStringDigester.setAlgorithm("SHA-1");
        standardStringDigester.setIterations(50000);
        standardStringDigester.setSaltSizeBytes(16);
        standardStringDigester.initialize();

        return standardStringDigester.digest(aMessage);
    }

    /**
     * Process the parameter message using a hashing algorithm.
     *
     * @param aMessage Message to digest.
     *
     * @return Hashed value.
     */
    public byte[] process(byte[] aMessage)
    {
        StandardByteDigester standardByteDigester = new StandardByteDigester();
        standardByteDigester.setAlgorithm("SHA-1");
        standardByteDigester.setIterations(50000);
        standardByteDigester.setSaltSizeBytes(16);
        standardByteDigester.initialize();

        return standardByteDigester.digest(aMessage);
    }

    /**
     * This is a one-way hashing algorithm for passwords.
     *
     * @param aPassword Password to hash.
     *
     * @return Encrypted hash value.
     */
    public String encryptPassword(String aPassword)
    {
        StrongPasswordEncryptor strongPasswordEncryptor = new StrongPasswordEncryptor();

        return strongPasswordEncryptor.encryptPassword(aPassword);
    }

    /**
     * Identifies if the plain password matches a previously hashed password
     * value.
     *
     * @param aPassword Password (plain text).
     *
     * @param aHashPassword Previously hashed password value.
     *
     * @return <i>true</i> if they match or <i>false</i> otherwise.
     */
    public boolean isPasswordValid(String aPassword, String aHashPassword)
    {
        StrongPasswordEncryptor strongPasswordEncryptor = new StrongPasswordEncryptor();

        return strongPasswordEncryptor.checkPassword(aPassword, aHashPassword);
    }
}
