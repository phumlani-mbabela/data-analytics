package co.za.zwideheights.spark;

import scala.io.Source
import java.io.{ FileReader, FileNotFoundException, IOException }
import scala.StringBuilder
import java.io.InputStream

import java.io.{ ObjectOutputStream, ObjectInputStream }
import java.io.{ FileOutputStream, FileInputStream }

/*  Reset stream throws an error. If i dont reset the stream the for loops do not automatically restart. Its probably a scala issue, or im just misusing the technology.
 *  val nwords = nwordsStream.reset.getLines
 *  val pwords = pwordsStream.reset.getLines
 *  
 * 
 * */

@SerialVersionUID(100L)
object SparkUtil extends Serializable {

  val regexUrl = "^((http[s]?|ftp):\\/)?\\/?([^:\\/\\s]+)((\\/\\w+)*\\/)([\\w\\-\\.]+[^#?\\s]+)(.*)?(#[\\w\\-]+)?$".r
  val regexHashtag = "#([^#]+)[\\s,;]*".r
  val regexAt = "@([^@]+)[\\s,;]*".r

  val nWordsFilename = "/negative-words.txt"
  val pWordsFilename = "/positive-words.txt"

  def processTuple( text:String ) : (String,String,Int,Int,Int,Boolean) = {
    
    //TODO: Inefficient code.
    //TODO: Reset stream throws an error. If i dont reset the stream the for loops do not automatically restart. 
    //      Its probably a scala issue, or im just misusing the technology.
    //      The best way of dealing with this situation is to read once, convert it to RDD, cache data and ship the data to the cluster.
    val nstream:InputStream = getClass.getResourceAsStream(nWordsFilename)
    val nwords = scala.io.Source.fromInputStream(nstream,"ISO-8859-1").getLines
    val pstream:InputStream = getClass.getResourceAsStream(pWordsFilename)
    val pwords = scala.io.Source.fromInputStream(pstream,"ISO-8859-1").getLines
    
    var p: Int = 0
    var n: Int = 0

    //Processing hashtag, @word and url logic.
    val noHashTagUrlsText = cleanTweet( text )

    //Processing positve lexisons.
    for (pword <- pwords) {
      if (noHashTagUrlsText.toLowerCase contains pword.toLowerCase) {
        p += 1
      }
    }
    
    //Processing negative lexisons.
    for (nword <- nwords) {
      if (noHashTagUrlsText.toLowerCase contains nword.toLowerCase) {
        n += 1;
      }
    }
    
    //We can also do language detection using Apache Tika 
    //(identifyLanguage(noHashTagUrlsText), noHashTagUrlsText, p, n, n-p, p > n)
    ("-", noHashTagUrlsText, p, n, n-p, p > n)
  }

   /* The function removes @Word, #Word and http*  */
  def cleanTweet(text: String): String = {
    val noUrlsText = regexUrl.replaceAllIn(text, "")
    val noHashTagUrlsText = regexHashtag.replaceAllIn(noUrlsText, "")
    val finalText = regexAt.replaceAllIn(noHashTagUrlsText, "")
    finalText
  }

  def identifyLanguage(text: String): String = {
    val identifier = new org.apache.tika.language.LanguageIdentifier(text)
    val language = identifier.getLanguage()
    language.toString
  }

}