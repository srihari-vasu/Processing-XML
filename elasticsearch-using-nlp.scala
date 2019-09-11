import scalaj.http._
import scala.sys.process._
import java.io.File
import scala.io.Source
import scala.xml._


object CaseIndex {
	
	// The function finds the word substring and gets its value
	def get_word(word: String):String = {
		val i = word.indexOf("word")
		//println(i)
		//println(word.substring(i+7,word.length()))
		val j = word.substring(i+7,word.length()).indexOf("\"")
		//println(j+7+i)
		//println(word.substring(i+7,j+7+i))
		return word.substring(i+7,j+7+i)
	}

	def main(args: Array[String]) = {
		// Delete an existing Index if any
		val delete_idx = Http("http://localhost:9200/legal_idx?pretty").method("DELETE").asString
		//println(delete_idx)

		// Create new Index based on given requirement
		val create_idx = Http("http://localhost:9200/legal_idx?pretty").method("PUT").asString
		//println(create_idx)

		// Create new Mapping on given requirement with Person, Location, Organization, and also File data for generic search
		val mapping_string = "{ \"cases\" : { \"properties\" : { \"person\" : { \"type\" : \"text\" }, \"location\" : { \"type\" : \"text\" }, \"organization\" : { \"type\" : \"text\" }, \"file\" : { \"type\" : \"text\" } } } }"
		val create_mapping = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty").method("POST").header("content-type", "application/json").postData(mapping_string).asString
		//println(create_mapping)
		
		// Open all files in given location
		//val inputDir = new File(Array(0))
		val inputDir = new File("/tmp_amd/glass/export/glass/3/z5200336/bigdata/assignment3/cases_test/")
		val allFilesinDir = inputDir.listFiles.filter(_.isFile).filter(_.getName.endsWith("xml")).map(_.getPath).toList
		//println(allFilesinDir)

		// Run a for loop to open all the files in the directory and get its data
		for ( i <- 0 to allFilesinDir.size - 1) {

			// Extract the Data from the current XML File
			//val fileText = "Max lives in Sydney. He like Apple. I live."
			val fileText = Source.fromFile(allFilesinDir(i)).getLines.mkString
			
			// Pass the file's text to the NLP server and get its output
			val nlpResponse = Http("http://localhost:9000/").params(("annotators","ner"),("outputFormat","json")).postData(fileText).header("content-type","application/json").timeout(connTimeoutMs = 2000000, readTimeoutMs = 5000000).asString
			val return_val = nlpResponse.body
			
			// Extract the 3 values we need, ie., Person, Location and Organization
			val ner_splt = return_val.split("ner")
			
			// Initialize empty String for each type
			var locat = ""
			var pers = ""
			var orgs = ""
			
			// Run a for loop across the split list to get the words and add it to the empty Texts
			// We also remove the duplicates by checking if the word is already in the Text
			for (i <- 1 to ner_splt.size-1) {
			
				// Extracting location and checking for duplciates
				//println(ner_splt(i-1))
				if (ner_splt(i).startsWith("\":\"LOCATION")) {
					val wrdsl:String = get_word(ner_splt(i-1))
					if(locat.indexOf(wrdsl) == -1) {
						locat = locat.concat(wrdsl + " ")
					}
				}
				
				// Extracting person and checking for duplciates
				else if (ner_splt(i).startsWith("\":\"PERSON")) {
					val wrdsp:String = get_word(ner_splt(i-1))
					if(pers.indexOf(wrdsp) == -1) {
						pers = pers.concat(wrdsp + " ")
					}
				}
				
				// Extracting organization and checking for duplciates
				else if (ner_splt(i).startsWith("\":\"ORGANIZATION")) {
					val wrdso:String = get_word(ner_splt(i-1))
					if(orgs.indexOf(wrdso) == -1) {
						orgs = orgs.concat(wrdso + " ")
					}
				}
			}
			
			// Extracted data is then put into new variables after cleaning escape sequences as they are not accepted by JSON
			val person = pers
			val location = locat
			val organization = orgs
			var file = fileText.replace("\""," ").replace("\n"," ").replace("\r"," ").replace("\t"," ")
			
			// Push the data into the index created
			val pushing_string = "{ \"person\" : \"" + person + "\" , \"location\" : \"" + location + "\" , \"organization\" : \"" + organization + "\" , \"file\" : \"" + file + "\" }"
			val push_data = Http("http://localhost:9200/legal_idx/cases?pretty").method("POST").header("content-type", "application/json").postData(pushing_string).asString
			//println(push_data)
		}
		//val response = Http("http://localhost:9200/legal_idx/cases/_search?pretty").method("GET").asString
		//println(response)
	}
}
