import scala.util.Random
import org.apache.spark.sql.functions.udf

// Random chance for introducing mistakes
val rand = new Random()

def generateKeyboardConfusion(query: String, rand: Random): String = {
  val keyboardLayout = Map(
    'q' -> "was", 'w' -> "qesad", 'e' -> "wrsdf", 'r' -> "etdfg", 't' -> "ryfgh",
    'y' -> "tughj", 'u' -> "yihjk", 'i' -> "uojkl", 'o' -> "ipkl", 'p' -> "ol",
    'a' -> "qwsz", 's' -> "wedxza", 'd' -> "erfcxs", 'f' -> "rtgvcd", 'g' -> "tyhbvf",
    'h' -> "yujnbg", 'j' -> "uikmnh", 'k' -> "iol,mj", 'l' -> "opk.",
    'z' -> "asx", 'x' -> "zsdc", 'c' -> "xvdf", 'v' -> "cfgb", 'b' -> "vghn",
    'n' -> "bhjm", 'm' -> "njk", ',' -> "mkl", '.' -> "lk", '/' -> ".",
    '1' -> "2q", '2' -> "13qw", '3' -> "24we", '4' -> "35er", '5' -> "46rt",
    '6' -> "57ty", '7' -> "68yu", '8' -> "79ui", '9' -> "80io", '0' -> "9p",
    '-' -> "0p", '=' -> "-", '`' -> "1", '~' -> "!", '!' -> "@", '@' -> "#", '#' -> "$",
    '$' -> "%", '%' -> "^", '^' -> "&", '&' -> "*", '*' -> "(", '(' -> ")", ')' -> "_",
    '_' -> "+", '+' -> "=", '{' -> "[", '[' -> "{", '}' -> "]", ']' -> "}", '\\' -> "|",
    '|' -> "\\", ':' -> ";", ';' -> ":", '"' -> "'", '\'' -> "\"", '<' -> ",",
    '>' -> ".", '?' -> "/"
  )

  val queryChars = query.toCharArray
  val numChanges = rand.nextInt(3) // 0, 1, or 2 changes

  for (_ <- 0 until numChanges) {
    if (queryChars.nonEmpty) {
      val index = rand.nextInt(queryChars.length)
      val char = queryChars(index)

      if (keyboardLayout.contains(char)) {
        val neighbors = keyboardLayout(char)
        if (neighbors.nonEmpty) {
          val newChar = neighbors(rand.nextInt(neighbors.length))
          queryChars(index) = newChar
        }
      }
    }
  }

  queryChars.mkString
}

def introduceMistakes(query: String): (String, String) = {
  if (query == null || query.isEmpty) return (query, "no-mistake")

  val mistakeTypes = Seq(
    "double-letter",
    "missing-char",
    "extra-char",
    "reversed-letters",
    "space-mistake",
    "keyboard-confusion",
    "auto-correct",
    "phonetic-error",
    "homophone",
    "abbreviations",
    "skipping-letters",
    "predictive-text"
  )
  val mistakeType = mistakeTypes(rand.nextInt(mistakeTypes.length))

  def compareDistinctWords(query: String, mistakeQuery: String): Boolean = {
    val originalWords = query.split("\\s+").distinct
    val mistakeWords = mistakeQuery.split("\\s+").distinct
    originalWords.sameElements(mistakeWords)
  }

  val mistakeQuery: String = mistakeType match {
    case "keyboard-confusion" =>
      generateKeyboardConfusion(query, rand)

    case "double-letter" =>
      if (rand.nextDouble() < 0.3) {
        val index = rand.nextInt(query.length)
        query.take(index) + query(index) + query.drop(index)
      } else {
        query
      }

    case "missing-char" =>
      if (query.length > 3 && rand.nextDouble() < 0.3) {
        val index = rand.nextInt(query.length)
        query.take(index) + query.drop(index + 1)
      } else {
        query
      }

    case "extra-char" =>
      if (rand.nextDouble() < 0.2) {
        val index = rand.nextInt(query.length)
        query.take(index) + query(index) + query.drop(index)
      } else {
        query
      }

    case "reversed-letters" =>
      if (query.length > 1 && rand.nextDouble() < 0.2) {
        val index = rand.nextInt(query.length - 1)
        query.take(index) + query(index + 1) + query(index) + query.drop(index + 2)
      } else {
        query
      }

    case "space-mistake" =>
      if (query.contains(" ") && rand.nextDouble() < 0.2) {
        if (rand.nextDouble() < 0.5) {
          val index = rand.nextInt(query.length)
          query.take(index) + " " + query.drop(index)
        } else {
          query.replaceFirst(" ", "")
        }
      } else {
        query
      }

    case "auto-correct" =>
      val corrections = Map("teh" -> "the", "recieve" -> "receive", "wierd" -> "weird")
      corrections.foldLeft(query) { (acc, correction) =>
        if (rand.nextDouble() < 0.1 && acc.contains(correction._1)) {
          acc.replace(correction._1, correction._2)
        } else {
          acc
        }
      }

    case "phonetic-error" =>
      val phoneticMap = Map("ph" -> "f", "k" -> "c", "s" -> "z")
      phoneticMap.foldLeft(query) { (acc, phonetic) =>
        if (rand.nextDouble() < 0.1 && acc.contains(phonetic._1)) {
          acc.replace(phonetic._1, phonetic._2)
        } else {
          acc
        }
      }

    case "homophone" =>
      val homophones = Map("to" -> "too", "their" -> "there", "hear" -> "here")
      homophones.foldLeft(query) { (acc, homophone) =>
        if (rand.nextDouble() < 0.1 && acc.contains(homophone._1)) {
          acc.replace(homophone._1, homophone._2)
        } else {
          acc
        }
      }

    case "abbreviations" =>
      val abbreviations = Map("because" -> "bc", "information" -> "info", "with out" -> "wo")
      abbreviations.foldLeft(query) { (acc, abbreviation) =>
        if (rand.nextDouble() < 0.1 && acc.contains(abbreviation._1)) {
          acc.replace(abbreviation._1, abbreviation._2)
        } else {
          acc
        }
      }

    case "skipping-letters" =>
      if (query.length > 3 && rand.nextDouble() < 0.1) {
        val numSkips = rand.nextInt(2) + 1
        (0 until numSkips).foldLeft(query) { (acc, _) =>
          val index = rand.nextInt(acc.length - 1)
          acc.take(index) + acc.drop(index + 2)
        }
      } else {
        query
      }

    case "predictive-text" =>
      val predictiveWords = Seq("the", "a", "is", "of", "and")
      if (rand.nextDouble() < 0.1 && query.nonEmpty) {
        val index = rand.nextInt(query.split(" ").length)
        val words = query.split(" ").toBuffer
        if (words.length > index) {
          words(index) = predictiveWords(rand.nextInt(predictiveWords.length))
          words.mkString(" ")
        } else {
          query
        }
      } else {
        query
      }

    case _ =>
      query
  }

  if (compareDistinctWords(query, mistakeQuery)) {
    (query, "no-mistake")
  } else {
    (mistakeQuery, mistakeType)
  }
}

val introduceMistakesUDF = udf((query: String) => introduceMistakes(query))
