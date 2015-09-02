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

package com.nridge.core.base.std;

/**
 * A NSException and its subclasses are a form of Throwable that
 * indicates conditions that an application developer might want
 * to catch.
 */
public class NSException extends Exception
{
    // http://today.java.net/article/2006/04/04/exception-handling-antipatterns
    /**
     * Default constructor.
     */
    public NSException()
    {
        super();
    }

    /**
     * Constructor accepts a default message for the exception.
     *
     * @param aMessage Message describing the exception.
     */
    public NSException(String aMessage)
    {
        super(aMessage);
    }

    /**
     * Constructor accepts a default message and a related
     * throwable object (e.g. context stack trace) for the
     * exception.
     *
     * @param aMessage Message describing the exception.
     * @param aCause An object capturing the context of
     *               the exception.
     */
    public NSException(String aMessage, Throwable aCause)
    {
        super(aMessage, aCause);
    }

    /**
     * Constructor accepts a throwable object (e.g. context
     * stack trace) for the exception.
     *
     * @param aCause An object capturing the context of
     *               the exception.
     */
    public NSException(Throwable aCause)
    {
        super(aCause);
    }

    /**
     * Constructor accepts a default message and a related
     * throwable object (e.g. context stack trace) for the
     * exception.  This form of the exception accepts Java
     * 7 parameters which are not fully supported at this
     * time.
     *
     * @param aMessage Message describing the exception.
     * @param aCause An object capturing the context of
     *               the exception.
     * @param aEnableSuppression Whether or not suppression
     *                           is enabled or disabled.
     * @param aWritableStackTrace whether or not the stack
     *                            trace should be writable.
     */
    public NSException(String aMessage, Throwable aCause,
                       boolean aEnableSuppression,  boolean aWritableStackTrace)
    {
        super(aMessage, aCause, aEnableSuppression, aWritableStackTrace);
    }
}
