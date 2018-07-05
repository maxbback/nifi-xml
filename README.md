# nifi-xml
XML processing with NiFi and Groovy

This NiFi processor written in Groovy is converting a XML tree to tables by flatening it out.

All root branches are converted to tables, if this contains new branches they are coverted to separate tables if they are of the type one to many if not it is flattened out into the parent to minimize number of tables.

For more information and description read the articles behind this code on;

http://max.bback.se/index.php/2018/06/30/xml-to-tables-csv-with-nifi-and-groovy-part-2-of-2/
