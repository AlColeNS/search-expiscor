expiscor - Latin for "Discover"
========

The _expiscor_ framework provides a rich set of classes for content modeling 
and data source access.  It was developed over several years to support enterprise search consulting engagements for many clients that NorthRidge Software served.  It has been open sourced with the goal of teaching fellow software developers techniques for information modeling and accessing structured/unstructured content sources such as web sites, file shares, RDBMS, graph databases and ECMs.  In addition, the framework has robust support for the Apache Solr search platform and its many search component features (e.g. queries, facets, highlighting, statistics, grouping, parent/child documents, etc.).

## Data Modeling ##

_Core Field Classes_

__DataField__ Can be used to describe a piece of meta data.  It consists of a type, name, title and one or more values.  In addition, a field can be further defined by one or more features (e.g. isRequired, isPrimaryKey, isIndexed).

__DataBag__ Can be used to describe a simple domain object such as a user or part.  It consists of a collection of fields and is identified by its name, title and one or more features.

__DataTable__ Can be used to capture a matrix of information (e.g. spreadsheet document).  It consists of a collection of cell fields organized by row & column and is identified by its name, title and one or more features.

__Document__ Describes a more complex domain object like a part assembly hierarchy.  It consists of a fields, relationships and links to other documents and is identified by its name, title and one or more features.

All core field classes support serialization/deserialization to/from XML, JSON and CSV formats.

## Data Sources ##
A data source is an abstract type designed to offer standard methods for accessing content sources. It is modeled after methods that support Create/Read/Update/Delete (CRUD) operations.

__MemoryTable__  The MemoryTable is specialized data source that manages a row x column matrix of data fields in memory.  It implements the four CRUD methods and supports advanced query and paging navigation using a criteria object.  In addition, this data source offers save and load methods for data values.  This can be a useful data source if your table size is small in nature and there is sufficient heap space available.

__RDBMS__  The RDBMS data sources are specialized data sources that implement drivers for Oracle, MySQL, PostgreSQL and HyperSQL database engines. They implement the four CRUD methods, table creation, table dropping, index creation, index dropping, sequence creation and support advanced query and paging navigation using a criteria object.

__SolrDS__ The Apache Lucene/Solr data source is a specialized data source that implements the four CRUD methods for the search platform and support advanced query and paging navigation using a criteria object.  In addition, the data source manages response payloads that include documents, highlights, facet fields, facet queries, facet pivots, facet ranges, statistics, spelling suggestions and grouped results.

__Neo4jDS__ The Neo4j data source is a specialized data source that that implements the four CRUD methods for a Neo4j graph database repository and support advanced query, relationship resolution and paging navigation using a criteria object.

### _Criteria_ ###

A criteria can be used to express a search criteria for a data source.  In its simplest form, it can capture one or more field names, their features, logical operators (e.g. equal, less-than, greater-than, contains, in, not in) and one or more field values.  In addition, it also supports grouping these expressions with boolean operators such as AND and OR.


## Services ##

One of the advantages to modeling domain objects as documents is that you can apply business logic to them in a more generic fashion.  For example, if you wanted to support version control over a __Document__, then you could create a _Version_ service class that implements a _Versionable_ interface.  The following interface definition illustrates how the method signatures would look with this approach.

```
public interface Versionable
{
    public void add(Document aDocument) throws ServiceException;

    public void checkout(Document aDocument)  throws ServiceException;

    public void checkin(Document aDocument)  throws ServiceException;

    public void alter(Document aDocument)  throws ServiceException;

    public void rollback(Document aDocument, int aVersion)  throws ServiceException;

    public void purge(Document aDocument)  throws ServiceException;

    public void delete(Document aDocument)  throws ServiceException;
}
```

The key to adopting this approach for services is to ensure that there are a collection of reserved fields (e.g. id, name, version, is_latest, etc.) and relationships (e.g. access control list) that are always present in the document.

## Content Source Types ##

### _Structured Content_ ###

Structured content is information or content that is organized in a predictable way and is usually classified with metadata.  Examples of structured content would include rows from relational database tables and documents stored in an enterprise content management system or a product lifecycle application.  

_Expiscor_ offers a number of classes that can be used to model information stored in these content sources.  The following table illustrates how domain objects related to an engineering design repository can be represented using the framework.

| Domain Object | _Expiscor_ Class | Notes                                                    |  
|  :------------------:| :--------------------:| ------------------------------------------------ |  
| Part                    | DataBag             | Collection of fields.                                |  
| Bill of Material | DataTable          | Collection of row x column cell fields.|  
| Assembly          | Document          | Hierarchical collection of fields, relationships and documents. |

### _Unstructured Content_ ###

Unstructured content refers to information that either does not have a pre-defined data model or is not organized in a pre-defined manner.  Examples of unstructured content would include Microsoft Office documents, PDF files, web pages and email messages.  The following table illustrates how domain objects related to file shares and web sites can be represented using the framework.

| Domain Object | _Expiscor_ Class | Notes                                                    |  
|  :------------------:| :--------------------:| ------------------------------------------------ |  
| MS Word Doc   | DataBag             | Collection of fields.                               |  
| MS Excel Doc   | DataTable          | Collection of row x column cell fields.|  
| Web Page          | Document          | Hierarchical collection of fields, relationships and documents. |

## Content Connectors ##

While there are a number of sophisticated open source and commercial options for content connectors, there are times when your requirements are simple enough that a basic connector implementation can get the job done.  The _Expiscor_ framework offers two types of connectors as a reference implementation for processing file share and web site content.  These connectors were designed to support a built-in and configurable extract/transform/publish (ETL) pipeline for each document processed.

### File Share ###

The file share connector is responsible for crawling document content within a file system.  The number of threads assigned to extract, transform and publish the documents is configurable. Other configurable features include:

* Full and incremental (via last modified date) document crawling
* Crawl folder start, folder follow and regular expression ignore lists
* Document typing and meta data property extraction
* CSV row expansion to individual documents
* Field mapping and deletion
* Pipeline metric processing statistics summaries
* Document viewing via a RESTful web service
* Email notification to administrators when errors are detected

### Web Site ###

The web site connector is responsible for crawling document content hosted on a web server.  The number of threads assigned to extract, transform and publish the documents is configurable. Other configurable features include:

* Full and incremental (via last modified date) document crawling
* Crawl URI start, URI follow and regular expression ignore lists
* Document typing and meta data property extraction
* Maximum crawl depth of a web site hierarchy
* Processing of JavaScript embedded within a web page
* Following of web page redirects
* Politeness delay time to avoid burdening a web server
* Field mapping and deletion
* Pipeline metric processing statistics summaries
* Email notification to administrators when errors are detected

## Build Environment ##

The following section outlines the steps a developer should follow to download, build and execute the _Expiscor_ framework components.

### Requirements ###

[JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or later

[Maven 3.x](http://maven.apache.org/download.cgi)

### Git Setup ###

```
$ git clone https://github.com/AlColeNS/NS.git
```

### Core and Data Source Build ###

```
$ cd apl/src
$ mvn compile source:jar javadoc:jar install
[INFO] ------------------------------------------------------------------------
[INFO] Reactor Summary:
[INFO] 
[INFO] Expiscor Super POM ................................ SUCCESS [  5.538 s]
[INFO] core_base ......................................... SUCCESS [  5.349 s]
[INFO] core_io ........................................... SUCCESS [  1.893 s]
[INFO] core_crypt ........................................ SUCCESS [  0.980 s]
[INFO] core_app .......................................... SUCCESS [  1.711 s]
[INFO] core_ds ........................................... SUCCESS [  2.377 s]
[INFO] NS Expiscor core packages ......................... SUCCESS [  4.850 s]
[INFO] ds_common ......................................... SUCCESS [  1.105 s]
[INFO] ds_content ........................................ SUCCESS [  1.742 s]
[INFO] ds_solr ........................................... SUCCESS [  1.821 s]
[INFO] ds_neo4j .......................................... SUCCESS [  1.842 s]
[INFO] NS Expiscor data source  packages ................. SUCCESS [  0.011 s]
[INFO] NS Expiscor packages .............................. SUCCESS [  0.011 s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 31.222 s
[INFO] Finished at: 2018-09-02T15:08:46-05:00
[INFO] Final Memory: 51M/523M
[INFO] ------------------------------------------------------------------------
```

## Examples ##

### DataBag Example ###

The DataBag example exercises some core features that the class has to offer.

```
$ cd apl/src/examples/databag
$ mvn test
Data Bag Console Output

     Long Field: 1
     Text Field: This is a test sentence.
    Float Field: 1.0
   Double Field: 1.0
  Integer Field: 1
  Boolean Field: true
Date/Time Field: Sep-02-2018 15:17:16

Data Bag XML Output
<DataBag type="DataBag" name="Data Bag" count="7" version="1.0">
 <Field type="Long" name="long_field" title="Long Field">1</Field>
 <Field type="Text" name="text_field" title="Text Field">This is a test sentence.</Field>
 <Field type="Float" name="float_field" title="Float Field">1.0</Field>
 <Field type="Double" name="double_field" title="Double Field">1.0</Field>
 <Field type="Integer" name="integer_field" title="Integer Field">1</Field>
 <Field type="Boolean" name="boolean_field" title="Boolean Field">true</Field>
 <Field type="DateTime" name="datetime_field" title="Date/Time Field">Sep-02-2018 15:17:16</Field>
</DataBag>

Data Bag JSON Output
{
 "name": "Data Bag",
 "version": "1.0",
 "fields": [
  {
   "name": "long_field",
   "type": "Long",
   "title": "Long Field",
   "value": "1"
  },
  {
   "name": "text_field",
   "type": "Text",
   "title": "Text Field",
   "value": "This is a test sentence."
  },
  {
   "name": "float_field",
   "type": "Float",
   "title": "Float Field",
   "value": "1.0"
  },
  {
   "name": "double_field",
   "type": "Double",
   "title": "Double Field",
   "value": "1.0"
  },
  {
   "name": "integer_field",
   "type": "Integer",
   "title": "Integer Field",
   "value": "1"
  },
  {
   "name": "boolean_field",
   "type": "Boolean",
   "title": "Boolean Field",
   "value": "true"
  },
  {
   "name": "datetime_field",
   "type": "DateTime",
   "title": "Date/Time Field",
   "value": "Sep-02-2018 15:17:16"
  }
 ]
}
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 3.625 s
[INFO] Finished at: 2018-09-02T15:17:17-05:00
[INFO] Final Memory: 18M/217M
[INFO] ------------------------------------------------------------------------
```

### DataTable Example ###

The DataTable example exercises some core features that the class has to offer.

```
$ cd apl/src/examples/datatable
$ mvn test
                                                    Data Table Console Output

Long Field  Text Field                              Float Field  Double Field  Integer Field  Boolean Field  Date/Time Field       
----------  ----------                              -----------  ------------  -------------  -------------  ---------------       
1           This is a test sentence 1 of 10 rows.   1.0          1.0           1              true           Sep-02-2018 15:22:57  
2           This is a test sentence 2 of 10 rows.   2.0          2.0           2              false          Sep-02-2018 15:23:57  
3           This is a test sentence 3 of 10 rows.   3.0          3.0           3              true           Sep-02-2018 15:24:57  
4           This is a test sentence 4 of 10 rows.   4.0          4.0           4              false          Sep-02-2018 15:25:57  
5           This is a test sentence 5 of 10 rows.   5.0          5.0           5              true           Sep-02-2018 15:26:57  
6           This is a test sentence 6 of 10 rows.   6.0          6.0           6              false          Sep-02-2018 15:27:57  
7           This is a test sentence 7 of 10 rows.   7.0          7.0           7              true           Sep-02-2018 15:28:57  
8           This is a test sentence 8 of 10 rows.   8.0          8.0           8              false          Sep-02-2018 15:29:57  
9           This is a test sentence 9 of 10 rows.   9.0          9.0           9              true           Sep-02-2018 15:30:57  
10          This is a test sentence 10 of 10 rows.  10.0         10.0          10             false          Sep-02-2018 15:31:57  

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 3.693 s
[INFO] Finished at: 2018-09-02T15:21:58-05:00
[INFO] Final Memory: 18M/217M
[INFO] ------------------------------------------------------------------------
```

### Document Example ###

The Document example exercises some core features that the class has to offer.

```
$ cd apl/src/examples/document
$ mvn test

... too much output to capture here ...

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 4.172 s
[INFO] Finished at: 2018-09-02T15:25:03-05:00
[INFO] Final Memory: 15M/212M
[INFO] ------------------------------------------------------------------------
```

## Memory Table Data Source Example ###

The Memory Table Data Source example exercises some core features that the data source has to offer.

```
$ cd apl/src/examples/memtbl
$ mvn test

                                  Memory Table Data Source Criteria Fetch

Id   First Name   Last Name  Email Address                 Country                IP Address       
--   ----------   ---------  -------------                 -------                ----------       
14   Cheryl       Ramirez    cramirezd@ca.gov              Peru                   211.85.44.44     
20   Christina    Ortiz      cortizj@histats.com           Indonesia              188.255.112.31   
35   Christine    Hunt       chunty@people.com.cn          Japan                  118.35.232.93    
50   Christine    Smith      csmith1d@cpanel.net           Philippines            55.18.195.73     
51   Christina    Chapman    cchapman1e@histats.com        Bolivia                123.31.73.58     
69   Christopher  Austin     caustin1w@cisco.com           Afghanistan            157.90.247.124   
71   Charles      Anderson   canderson1y@si.edu            Indonesia              24.115.117.130   
116  Christine    Jones      cjones37@businessweek.com     Malawi                 152.159.43.171   
117  Christine    Martin     cmartin38@nasa.gov            Russia                 82.9.99.51       
219  Cheryl       Hawkins    chawkins62@newsvine.com       Uruguay                114.60.137.51 

                                  Memory Table Data Source Sort

Id   First Name   Last Name  Email Address                 Country                IP Address       
--   ----------   ---------  -------------                 -------                ----------       
290  Chris        Alvarez    calvarez81@howstuffworks.com  Taiwan                 21.94.204.130    
71   Charles      Anderson   canderson1y@si.edu            Indonesia              24.115.117.130   
69   Christopher  Austin     caustin1w@cisco.com           Afghanistan            157.90.247.124   
682  Chris        Carr       ccarrix@nymag.com             Philippines            12.204.29.162    
51   Christina    Chapman    cchapman1e@histats.com        Bolivia                123.31.73.58     
771  Charles      Dean       cdeanle@mayoclinic.com        Panama                 80.173.151.107   
475  Cheryl       Diaz       cdiazd6@networksolutions.com  Argentina              253.145.129.53   
737  Christopher  Gordon     cgordonkg@privacy.gov.au      Sweden                 158.245.107.254  
518  Charles      Gray       cgrayed@java.com              Indonesia              15.224.97.44     
934  Chris        Greene     cgreenepx@google.com.br       Brazil                 158.0.240.254    
357  Cheryl       Hanson     chanson9w@boston.com          Niger                  179.20.113.93    
219  Cheryl       Hawkins    chawkins62@newsvine.com       Uruguay                114.60.137.51  

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 4.172 s
[INFO] Finished at: 2018-09-02T15:25:03-05:00
[INFO] Final Memory: 15M/212M
[INFO] ------------------------------------------------------------------------
```

## RDBMS Data Source Example ###

The RDBMS Data Source example exercises some core features that the data source has to offer.

```
$ cd apl/src/examples/ds_rdbms
$ mvn test
                                                        RDBMS Table Console Output

Id    Long Field  Float Field  Double Field  Integer Field  Boolean Field  Date/Time Field       Text Field                                 
--    ----------  -----------  ------------  -------------  -------------  ---------------       ----------                                 
1990  991         991.0        991.0         991            true           Sep-03-2018 08:03:41  This is a test sentence 991 of 1000 rows.  
1992  993         993.0        993.0         993            true           Sep-03-2018 08:05:41  This is a test sentence 993 of 1000 rows.  
1994  995         995.0        995.0         995            true           Sep-03-2018 08:07:41  This is a test sentence 995 of 1000 rows.  
1996  997         997.0        997.0         997            true           Sep-03-2018 08:09:41  This is a test sentence 997 of 1000 rows.  
1998  999         999.0        999.0         999            true           Sep-03-2018 08:11:41  This is a test sentence 999 of 1000 rows.  

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 3.403 s
[INFO] Finished at: 2018-09-02T15:32:43-05:00
[INFO] Final Memory: 22M/230M
[INFO] ------------------------------------------------------------------------
```

## Neo4j Data Source Example ###

The Neo4j Data Source example exercises some core features that the data source has to offer.

```
$ cd apl/src/examples/ds_neo4j
$ mvn test
                                                                                                  Data Table Console Output

NSD Id                            NSD Name                   NSD Title                             NSD Document Type  Configuration Id                  Current State                NSD Version Number  NSD Crawled On        
------                            --------                   ---------                             -----------------  ----------------                  -------------                ------------------  --------------        
8532D567C70A4029921437DA48B8447D  ASSEMBLY-TROLLEY           P000000014 ASSEMBLY-TROLLEY           Part               0C73DF74791C44788DFC07E6FF4E0BD9  Preliminary                  3                   Aug-02-2018 12:31:19  
35FFFEFC6EA246479DA05CBE00C3F34F  HANDLE                     P000000039 HANDLE                     Part               57341A2D6BEC416B85E2C4165205F0D1  Preliminary                  2                   Aug-02-2018 12:31:19  
683F3A7DA3044908942C0BFA0BEBB594  SWITCH                     P000000021 SWITCH                     Part               4FA3DF1DAEEF4D1FBD552946A1BA23CE  Preliminary                  2                   Aug-02-2018 12:31:19  
E73FE3D0B2DA4ED38E9815347D4EE21B  WHEEL_SMALL-ASSEMBLY       P000000035 WHEEL_SMALL-ASSEMBLY       Part               B6B36CF99A544CE39B2C2CCEA9EB40BC  Preliminary                  2                   Aug-02-2018 12:31:19  
8F4F3C0F53744310A65034E47D12D80E  COUNTERSINK-SCREW_M4X10    P000000015 COUNTERSINK-SCREW_M4X10    Part               7A676126708C4A0E954CB1EABFBE3329  Preliminary                  2                   Aug-02-2018 12:31:19  
89F0C4E15FFF4AA79DBDCEBBFC822CB0  SPRING_WASHER              P000000033 SPRING_WASHER              Part               63954B8F796D4DDCBFAC027E249C96B6  Preliminary                  2                   Aug-02-2018 12:31:19  
ECD92A97818E44298ACA361D7E3A2B89  WASHER                     P000000034 WASHER                     Part               7ED01E4439E24A2BB501079DB0A6D40F  Preliminary                  2                   Aug-02-2018 12:31:19  
9574DE06C7414B15A48B6D6C016FF7B1  SOCKET                     P000000025 SOCKET                     Part               AD784F2B0B204F4E8490DEB8808FD890  Preliminary                  2                   Aug-02-2018 12:31:19  

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 7.174 s
[INFO] Finished at: 2018-09-02T15:34:35-05:00
[INFO] Final Memory: 118M/509M
[INFO] ------------------------------------------------------------------------
```

## Solr Data Source Example ###

The Solr Data Source example exercises some core features that the data source has to offer.  Please note that this is a much more involved example because it is dependent on an Apache Solr index being available and configured for the fields and response handlers utilized by the example logic.  Refer to the "solr" sub folder for more information regarding these configuration files.

```
$ cd apl/src/examples/ds_solr
$ mvn test

... too much output to capture here ...

[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 4.851 s
[INFO] Finished at: 2018-09-02T15:39:07-05:00
[INFO] Final Memory: 23M/308M
[INFO] ------------------------------------------------------------------------
```


