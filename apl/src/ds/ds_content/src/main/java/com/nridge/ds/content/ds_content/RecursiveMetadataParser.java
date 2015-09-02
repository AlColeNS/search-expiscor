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

import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.language.ProfilingHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ParserDecorator;
import org.apache.tika.sax.TeeContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;

/**
 * The RecursiveMetadataParser is responsible for extending the
 * standard Tika parser with meta data extraction support.
 *
 * @see <a href="http://tika.apache.org/">Apache Tika</a>
 * @see <a href="http://wiki.apache.org/tika/RecursiveMetadata">Recursive Metadata Discussion</a>
 *
 * @since 1.0
 * @author Al Cole
 */
public class RecursiveMetadataParser extends ParserDecorator
{
    /**
     * Default constructor creates a decorator for the given parser.
     *
     * @param aParser The parser instance to be decorated.
     */
    public RecursiveMetadataParser(Parser aParser)
    {
        super(aParser);
    }

    /**
     * Delegates the method call to the decorated parser. Subclasses should
     * override this method (and use <code>super.parse()</code> to invoke
     * the decorated parser) to implement extra decoration.
     *
     * @param aStream The document stream (input).
     * @param aContentHandler Handler for the XHTML SAX events (output).
     * @param aMetaData Document metadata (input and output).
     * @param aContext Parse context.
     *
     * @throws java.io.IOException If the document stream could not be read,
     * @throws org.xml.sax.SAXException If the SAX events could not be processed.
     * @throws org.apache.tika.exception.TikaException if the document could not be parsed.
     */
    @Override
    public void parse(InputStream aStream, ContentHandler aContentHandler,
                      Metadata aMetaData, ParseContext aContext)
        throws IOException, SAXException, TikaException
    {
        ProfilingHandler profilingHandler = new ProfilingHandler();
        ContentHandler teeContentHandler = new TeeContentHandler(aContentHandler, profilingHandler);

        super.parse(aStream, teeContentHandler, aMetaData, aContext);

        LanguageIdentifier languageIdentifier = profilingHandler.getLanguage();
        if (languageIdentifier.isReasonablyCertain())
            aMetaData.set(Metadata.CONTENT_LANGUAGE, languageIdentifier.getLanguage());
    }
}
