import java.nio.charset.StandardCharsets
@Grab('commons-io:commons-io:2.4')
import org.apache.commons.io.IOUtils
@Grab('org.apache.avro:avro:1.8.1')
import org.apache.avro.*
import org.apache.avro.file.*
import org.apache.avro.generic.*
import groovy.json.JsonBuilder
import org.apache.avro.util.*

/*
	Column defintion class
*/
class Column {
	def path = []
	def colName
	def name
	def fieldName
	def type = "xs:string"
	def canBeNull
	def attribute
}

/*
	Table definition class
*/
class Table {
	def path = []
	def name
	def schema
	def columns = []
	def primaryKey = null
	def primaryKeyType = "xs:string"
	def foreignKey = null
	def foreignKeyType = "xs:string"
	def foreignKeyPath = []
}


/*
	Used to iterate down the xml tree based on path and return that node
*/
def getNodes (doc,path) {
	def node = doc
	path.each
	{
		node=node."${it}"
	}
	return node
}

// translate XML types to avro types
def getAvroField(fieldName,dataType, canBeNull) {
	if (dataType == "xs:integer") {
		dataType = "int"
	} else if (dataType == "xs:decimal") {
		dataType = "double"
	} else if (dataType == "xs:double") {
			dataType = "double"
	} else if (dataType == "xs:float") {
				dataType = "float"
	} else if (dataType == "xs:long") {
		dataType = "long"
	} else if (dataType == "xs:dateTime") {
		dataType = "string"
	}	else if (dataType == "xs:token") {
		dataType = "string"
	}
	else {
		dataType = "string"
	}
	def field = [
    name:fieldName,
    type:[
      "null",dataType
    ]
	]
	return field
}

// trim strings and convert xml text to correct avro data type
def trimValue(dataType, val) {
	if (dataType == "xs:decimal") {
		val = val.toDouble()
	} else if (dataType == "xs:double") {
			val = val.toDouble()
	} else if (dataType == "xs:float") {
				val = val.toFloat()
	} else if (dataType == "xs:integer") {
		val = val.toInteger()
	} else if (dataType == "xs:dateTime") {
		// convert it to a timeStamp
		val = val.toString()
	}	else if (dataType == "xs:string" || dataType == "xs:token") {
		val = val.toString()
		// for token columns we do some trimming
		// we remove all line breaks, just to simplify
		// we also escape quate character and escape character
		if (dataType == "xs:token") {
			val = val.replaceAll('\n',' ')
			val = val.replaceAll('\r',' ')
			val = val.replaceAll('\t',' ')
			val = val.replaceAll('\\s+',' ')
		}

	}

	return val
}
// Global variable holding all tables
def myTables = []

/*
	This function is processing XSD file and is flattning out the fieldStructure
	into tables
*/
def parseXsdElement(myTables,doc_xsd,element,table,path,colNamePrefix)
{
	def nrAttributes = 0
	def keyAttribute = ""
	def keyAttributeType = ""
	// All attributes are added to the list of columns
	element.complexType.attribute.each
	{
		nrAttributes=nrAttributes+1
		col = new Column ()
		// To secure that an attribute is unique in the table
		// column path is added if it is a branch attribute
		col.colName = "${colNamePrefix}${it.@name}"
		col.name = it.@name
		col.type = it.@type
		if (path.size() > 0) {
			col.path.addAll(path)
		}
		col.attribute = 1
		table.columns.add(col)
		keyAttribute = it.@name
		keyAttributeType = col.type
	}
	// Check if we have a primary key in this table
	// Only do this if we are on the toplevel of a table
	if (colNamePrefix.size() == 0 && nrAttributes == 1 && table.primaryKey == null)
	{
		// if only one attribute we treat it as primarykey
		table.primaryKey=keyAttribute
		table.primaryKeyType = keyAttributeType
	}

	// We process all elements
	element.complexType.sequence.element.each
	{
		// If type is defined we use it as a column
		// if not we will skip it
		if (!it.@type.isEmpty()) {
			col = new Column ()
			col.colName = "${colNamePrefix}${it.@name}"
			col.name = it.@name
			col.type = it.@type
			if (path.size() > 0) {
				col.path.addAll(path)
			}
			col.attribute = 0
			table.columns.add(col)
		} else {
			// if branch is a single child we will flatten it out
			if (it.@maxOccurs.isEmpty() || it.@maxOccurs == "1") {
				def name = it.@name
				// Flatten out this branch
				def newPath = []
				if (path.size() > 0) {
					newPath.addAll(path)
				}
				newPath.add(it.@name)
				// recursive iteration to find branches in this branch
				// we pass current table as the base so it can be extended with columns
				parseXsdElement(myTables,doc_xsd,it,table,newPath,"${colNamePrefix}${it.@name}_")
				//parseXsdElement(myTables,doc_xsd,element,table,path,colNamePrefix)
			} else {
				// We have a multi branch so we create a new table for it
				newtable = new Table()
				newtable.columns = []
				newtable.name = "${table.name}_${it.@name}"
				if (table.path.size() > 0) {
					newtable.path.addAll(table.path)
				}
				newtable.path.add(it.@name)
				if (path.size() > 0) {
					newtable.path.addAll(path)
				}
				if (table.primaryKey != null) {
					newtable.foreignKey = table.primaryKey
					newtable.foreignKeyType = table.primaryKeyType
					if (table.path.size() > 0) {
						newtable.foreignKeyPath.addAll(table.path)
					}
				}
				else if (table.foreignKey) {
					newtable.foreignKey = table.foreignKey
					newtable.foreignKeyType = table.foreignKeyType
					if (table.foreignKeyPath.size() > 0) {
						newtable.foreignKeyPath.addAll(table.foreignKeyPath)
					}
				}

				myTables.add(newtable)

				parseXsdElement(myTables,doc_xsd,it,newtable,[],"")
				//parseXsdElement(myTables,doc_xsd,element,table,path,colNamePrefix)
			}
		}
	}
}

// ###############################################
// Main code

def inflowFile = session.get()
if (inflowFile == null) {
    return;
}


// first lets pick up some attributes from this processor
def xsdFileName = xsdFile.value
//def xmlFileName = xmlFile.value
def fileFormat = fileOutputFormat.value

if (fileFormat == 'csv')
{
	csvOutput = true
} else {
	csvOutput = false
}



def xmlText = ""

try {

session.read(inflowFile, {inputStream ->
  xmlText = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
  // Do something with text here
} as InputStreamCallback)

} catch(e) {
    //log.error('Scripting error', e)
		String str= e.getStackTrace().toString()

		def pattern = ( str =~ /groovy.(\d+)./   )

		// Log line number in groovy code

		log.error " Error at line number = " + pattern[0][1]

    session.transfer(inflowFile, REL_FAILURE)
}

// Get the XSD and XML file
doc_xsd = new XmlSlurper().parse(xsdFileName)
//doc_xml = new XmlSlurper().parse(xmlFileName)
doc_xml = new XmlSlurper().parseText(xmlText)



try {
	// Define flowfile list that will hold each table§
	def flowFiles = [] as List<FlowFile>

	// Define the starting point in the XSD file
	// And collect table definitions
	doc_xsd.element.complexType.sequence.element.each
	{
		table = new Table()
		table.name = it.@name
		table.columns = []
		table.path = [it.@name]
		myTables.add(table)
		parseXsdElement(myTables,doc_xsd,it,table,[],"")
		//parseXsdElement(myTables,doc_xsd,element,table,path,colNamePrefix)
	}

	// Lets process the XML file
	def date = new Date()
	// timeStamp is used as primary key if no key is found
	// this makes the whole XML file share the same primary key
	// new timestamps has to be generated further down for child branches
	// that do not have a primary key
	def timeStamp = date.getTime()
	def countRows = 0
	def totalTableCount = myTables.size()
	def tablesWithContent = 0

	myTables.each
	{
		// path should be atleast one, this code can be removed
		if (it.path.size() == 0) {
			return
		}

		// table names is path joined by _, we could change this standard
		def tableName = it.path.join('_')

		// create new flowfile for the table
		flowFile = session.create()
		// define flow file as of type outbound
		flowFile = session.write(flowFile, {outputStream ->

			def table = it
			def columns = it.columns
			// Add our generated primary and foreign keys
			def pk,fk,keyNode

			// Define field array for the table
			def fields = []

			// get the foreign key for this table
			if (it.foreignKey) {
				keyNode = getNodes(doc_xml,it.foreignKeyPath)
				fk = keyNode.@"${it.foreignKey}"
			}
			else {
				fk = timeStamp
				table.foreignKeyType = "xs:long"
			}

			// add primary and foreign key fields to fields list for the table
			fields.add(getAvroField("apk",table.primaryKeyType, "null"))
			fields.add(getAvroField("afk",table.foreignKeyType, "null"))

			// We collect column name and types
			// column names are made unique by including the path if it is a branch
			it.columns.each {
				def p = []
				if (it.path.size() > 0) {
					p.addAll(it.path)
				}
				p.add(it.name)
				it.fieldName = p.join('_')
				// Add the field to avro fields list
				fields.add(getAvroField(it.fieldName,it.type, "null"))
			}

			// Define avro schema for the table
			// add all field definitions to the table
			def tableSchemaObj = [
				type:"record",
				name:tableName,
				namespace:"any.data",
				fields:fields
			]
			// create a json blob for the schema
			avroJsonSchema = new JsonBuilder(tableSchemaObj)

			// Generate a Avro schema definition from avro json blob
			avroSchema = new Schema.Parser().parse(avroJsonSchema.toString())

			DataFileWriter<GenericRecord> writer = null
			if (!csvOutput) {
				// Create avro writer
        writer = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>())
				// Attache the avro schema to output stream
				writer.create(avroSchema, outputStream)

				// We now have prepare our output stream with our avro writer and avro Schema
			}

			// Its time to push out avro records
			// counter of number of rowns found in this table
			countRows = 0
			// We loop over all elements in this branch to collect all rows
			getNodes(doc_xml,table.path).each
			{
				// do we have a primary key element or shall we use a timestamp
				if (table.primaryKey) {
					pk = it.@"${table.primaryKey}"
				}
				else {
					// to make each row unique we combine the timestamp with row counter
					// an alternative would be to have combined primary key of rown and timestamp as we then can use integers instead of strings
					pk = "${timeStamp}_${countRows}"
				}

				// Create a new record
				GenericRecord newAvroRecord = new GenericData.Record(avroSchema)
				// create csv array
				colArray = []

				// populate the avro record with data
				pk = trimValue(table.primaryKeyType,pk)
				fk = trimValue(table.foreignKeyType,fk)

				if (csvOutput) {
					// populate the csv col array
					colArray.add(pk.toString())
					colArray.add(fk.toString())
				}
				else {
					// populate record with keys
					newAvroRecord.put("apk", pk)
					newAvroRecord.put("afk", fk)
					// Consider forsing UTF8 character settings
					//newAvroRecord.put("afk", util.Utf8(fk))
				}


				//Lets populate the record with columns
				countRows++

				def record = it
				columns.each
				{
					def node = getNodes(record,it.path)
					def p = it.path.join('.')
					def val = ""
					if (it.attribute == 1)
					{
						def v = node.@"${it.name}"
						val = v.text()
					} else {
						def v = node."${it.name}"
						val = v.text()
					}

					// trim value and make it the corect data type
					val = trimValue(it.type,val)

					if (csvOutput) {
						// populate csv array with column value
						if (it.type == "xs:string") {
							val = val.replaceAll('\n',' ')
							val = val.replaceAll('\r',' ')

							if (val.contains('\\')) {
								val = val.replaceAll('\\','\\\\')
							}
							if (val.contains('"')) {
								val = val.replaceAll('"','\\"')
							}
							val = '"' + val + '"'
						}

						colArray.add(val.toString())
					}
					else {
						// populate record with column value
						newAvroRecord.put(it.fieldName, val)
					}


				}

				if (csvOutput) {
					// write csv line
					outputStream.write((colArray.join(',')+ "\n").getBytes(StandardCharsets.UTF_8))
				}
				else {
					// Append a new record to avro file
					writer.append(newAvroRecord)
				}

			}

			if (!csvOutput) { // avro
				//writer.appendAllFrom(reader, false)
				// do not forget to close the avro writer
				writer.close()
			}

		} as OutputStreamCallback)
		// If table is empty we dropp the flowfile
		// to prevent unneded tables, as XSD can contain tables not used by XML
		if (countRows == 0) {
			session.remove(flowFile)
			return
		}
	  // Count tables with content
		tablesWithContent++

		// Now we add some attributes to flowfile
		// table name and table definition
		if (csvOutput) {
			flowFile = session.putAttribute(flowFile, 'filename', tableName + '.csv')
		} else {
			flowFile = session.putAttribute(flowFile, 'filename', tableName + '.avro')
		}
		flowFile = session.putAttribute(flowFile, 'totalTableCount', totalTableCount.toString())
		flowFile = session.putAttribute(flowFile, 'recordCount', countRows.toString())
		flowFile = session.putAttribute(flowFile, 'avro.schema', avroSchema.toString())

		flowFiles << flowFile
	}
	session.transfer(flowFiles, REL_SUCCESS)
	//session.transfer(inflowFile, REL_SUCCESS)
	session.remove(inflowFile)

} catch(e) {
    //log.error('Scripting error', e)
		String str= e.getStackTrace().toString()

		def pattern = ( str =~ /groovy.(\d+)./   )

		// Log line number in groovy code

		log.error " Error at line number = " + pattern[0][1]

    session.transfer(flowFiles, REL_FAILURE)
}
