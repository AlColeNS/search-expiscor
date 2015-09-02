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

package com.nridge.core.base.ds;

import com.nridge.core.base.std.NSException;

/**
 * A DSException and its subclasses are a form of Throwable that
 * indicates conditions that an application developer might want
 * to catch.
 */
public class DSException extends NSException
{
    /**
     * Default constructor.
     */
    public DSException()
    {
        super();
    }

    /**
     * Constructor accepts a default message for the exception.
     *
     * @param aMessage Message describing the exception.
     */
    public DSException(String aMessage)
    {
        super(aMessage);
    }
}