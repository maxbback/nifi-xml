import java.nio.charset.StandardCharsets

/*
	Column defintion class
*/
class Column {
	def path = []
	def colName
	def name
	def type = "xs:string"
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

// Global variable holding all tables
def myTables = []

/*
	This function is processing XSD file and is flattning out the fieldStructure
	into tables
*/
def showElement(myTables,doc_xml,doc_xsd,element,table,path,colNamePrefix)
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
				showElement(myTables,doc_xml,doc_xsd,it,table,newPath,"${colNamePrefix}${it.@name}_")

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

				showElement(myTables,doc_xml,doc_xsd,it,newtable,[],"")
			}
		}
	}
}

// ###############################################
// Main code

// first lets pick up some attributes from this processor
def xsdFileName = xsdFile.value
def xmlFileName = xmlFile.value

// Get the XSD and XML file
doc_xsd = new XmlSlurper().parse(xsdFileName)
doc_xml = new XmlSlurper().parse(xmlFileName)


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
		showElement(myTables,doc_xml,doc_xsd,it,table,[],"")
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

	myTables.each
	{
		if (it.path.size() == 0) {
			return
		}


		def tableName = it.path.join('.')
		def tableDefinition  = []
		flowFile = session.create()
		flowFile = session.write(flowFile, {outputStream ->

		def table = it
		def columns = it.columns
		// Add our generated primary and foreign keys
		def colArray = ["apk","afk"]
		def pk,fk,keyNode
		// Define table definition that can be used for creating Hive tables
		tableDefinition  = []
		tableDefinition.add("apk" + ":" + table.primaryKeyType)
		tableDefinition.add("afk" + ":" + table.foreignKeyType)

		// get the foreign key for this table
		if (it.foreignKey) {
			keyNode = getNodes(doc_xml,it.foreignKeyPath)
			fk = keyNode.@"${it.foreignKey}"
		}
		else {
			fk = timeStamp
		}
		// We collect column name and types
		// column names are made unique by including the path if it is a branch
		it.columns.each {
			def p = []
			if (it.path.size() > 0) {
				p.addAll(it.path)
			}
			p.add(it.name)
			colArray.add(p.join('.'))
			tableDefinition.add(p.join('.') + ":" + it.type)
		}

		def headerLine = colArray.join(',')+ "\n"
		countRows = 0

		getNodes(doc_xml,table.path).each
		{
			if (table.primaryKey) {
				pk = it.@"${table.primaryKey}"
			}
			else {
				pk = "${timeStamp}_${countRows}"
			}

			countRows++
			colArray = []
			colArray.add(pk)
			colArray.add(fk)

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
				// for string columns we do some trimming
				// we remove all line breaks, just to simplify
				// we also escape quate character and escape character
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

				colArray.add(val)
			}
			// Write out the
			if (countRows == 1) {
				outputStream.write(headerLine.getBytes(StandardCharsets.UTF_8))
			}
			outputStream.write((colArray.join(',')+ "\n").getBytes(StandardCharsets.UTF_8))
		}

	} as OutputStreamCallback)
	// If table is empty we dropp the flowfile
	// to prevent unneded tables, as XSD can contain tables not used by XML
	if (countRows == 0) {
		session.remove(flowFile)
		return
	}
	// Now we add some attributes to flowfile
	// table name and table definition
	flowFile = session.putAttribute(flowFile, 'filename', tableName + '.csv')
	flowFile = session.putAttribute(flowFile, 'tableDefinition', tableDefinition.join('\n'))
	flowFile = session.putAttribute(flowFile, 'totalTableCount', totalTableCount.toString())

		flowFiles << flowFile
	}
	session.transfer(flowFiles, REL_SUCCESS)
} catch(e) {
    log.error('Scripting error', e)
    session.transfer(flowFiles, REL_FAILURE)
}
