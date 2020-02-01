# nifi-xml xml2csv xml2avro
XML to csv processing with NiFi and Groovy
This processor can convert XML to CSV and XML to AVRO

This NiFi processor written in Groovy is converting a XML tree to tables by flatening it out.

All root branches are converted to tables, if this contains new branches they are coverted to separate tables if they are of the type one to many if not it is flattened out into the parent to minimize number of tables.
A uniqie key and foreign key is defined for each table to ease the table joins, if a natural key exist as an attribute on the branch it is used as the primary key.

For more information and description read the articles behind this code on;

http://max.bback.se/index.php/2018/06/30/xml-to-tables-csv-with-nifi-and-groovy-part-2-of-2/


Xml to table (CSV) transformation with Groovy and NiFi

XMl is a common format used by many applications, so it will be one of the formats you need to support in your Big Data platform.


XML is a structured format that is good for application but not so convenient if you are used to work with SQL and tables so I decided to design and write code for transforming XML to CSV.

You might thing that converting from XML to CSV is a very simple thing, but it is not XML is a hierarchical tree structured data format while CSV is a flat column based format.

A branch in XML can key value pairs and branches, branches is of type one2one or ont2many

When we convert an XML the best is if we have a schema so we know  how the XML file can look like as we then has the description for how the DB table shall be defined.

My design is based on that we have a XSD schema as a base to secure that we know which table columns we need.

In below schema we have example of many of the patterns we often see in a schema, lets discuss them.
Element person has an attribute name and can exist in any number of instances and needs to be its own table.
Element person has direct element childs;
-	Full_name is a simple element that is of type string and is a column in person table
-	Child_name is a simple element of type string that can exist in 0-5 times and can either be its own table och be 5 columns in person table “child_name1-5”
-	Child_name_complex is unbound and has to be its own table
Element child_name_complex has direct element childs;
-	Length is a simple element of type decimal and is a column in child_name_complex table
-	Age is also a simple element and will also be a column in child_name_complex
-	Mother is a complex type but can only exist 1 time and can then be flattened out to be columns in child_name_complex

<xs:element name="persons">
  <xs:complexType>
    <xs:sequence>
      <xs:element name="person" maxOccurs="unbounded">
        <xs:complexType>
          <xs:sequence>
            <xs:element name="full_name" type="xs:string"/>
            <xs:element name="child_name" type="xs:string" minOccurs="0" maxOccurs="5"/>
            <xs:element name="child_name_complex" minOccurs="0" maxOccurs="unbounded">
              <xs:complexType>
                <xs:sequence>
                  <xs:element name="length" type="xs:decimal" minOccurs="0" nillable="true"/>
                  <xs:element name="age" type="xs:decimal" minOccurs="0" nillable="true"/>
                  <xs:element name="mother" minOccurs="0" maxOccurs="1">
                    <xs:complexType>
                      <xs:sequence>
                        <xs:element name="name" type="xs:string" minOccurs="0" nillable="true"/>
                      </xs:sequence>
                    </xs:complexType>
                  </xs:element>
                </xs:sequence>
              </xs:complexType>
            </xs:element>
          </xs:sequence>
        </xs:complexType>
      </xs:element>
    </xs:sequence>
  </xs:complexType>
</xs:element>

Rules for XML to tables
We need simple rules to follow when we are flattening out the XML tree to tables and columns.

1.	Element with maxOccurs=”unbound” must be its own table
2.	Element with maxOccurs=”1” can be flattened out to be column in its parent but child elements must be renamed to include the parent name
a.	Example mother_name for mother element name
3.	Attributes is column in its parent
4.	Element of type simple that has a limited maxOccurs can be flattened out to columns, but if the value is high it’s better to generate a new table
5.	Tables of child’s need to be unique this can be accomplished be including parent table name in the table name or make each table unique by adding a suffix to them.


Sometimes XSD schema is very open which will result in many tables, if you know your data and the alternatives of its look, it is better to make a more strict schema which will dramatically lower the number of tables, if you use many  1t1 branches remove them and make them simple elements instead as you then will get shorter column names.

Read the full story in the word document.


A new file has been added xml2csv_xml2avro.groovy this new script convert XML to ether csv or avro and process the incoming flow file.
Also new in this is that the header line is removed and replaced by an avro schema as an attribute to each flow file, this is better as it will be easier for downstream processors to use the data and also to store the output in right format in hive or any other database

A NiFi template is availbe containing an example implementation
