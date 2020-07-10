import java.io.FileNotFoundException

import org.apache.avro.ipc.specific.Person

import scala.tools.nsc.doc.model.Public
import scala.collection.mutable._
import org.apache.log4j._

import scala.io._


object HelloWorldBigData {
  /* premier programme Scala  */
  val ma_var_imm : String = "Juvenal"   // variable immutable

  class Person (var nom : String, var prenom : String, var age : Int)

  private val une_var_imm : String = "Formation Big Data "  // variable à portée privée
  BasicConfigurator.configure()
  private var trace_appli : Logger = LogManager.getLogger("Logger_Console")

  def main(args: Array[String]): Unit = {


      val diviseur : Double = try {
        division(12, 0)
      } catch {
        case ex : ArithmeticException => 0
        case ex2 : IllegalArgumentException => 0
      }

      trace_appli.info(s"la valeur de votre division est de ${diviseur}")

      lecture_fichier("c:/programmes/mesdonnees.txt")

      val nombre  = convert_entier("10")
      trace_appli.info(s"la valeur de votre nombre converti est : $nombre")

    println("Hello World : mon premier programme en Scala")

    var test_mu : Int  = 15    // variable mutable
    test_mu = test_mu + 10

    println(test_mu)

    val  test_imm : Int = 15

    println ("Votre texte contient : " + Comptage_caracteres("qu'avez-vous mangé ce matin ?   ") + " caractères")

    getResultat(10)

    testWhile(10)

    testFor()

    collectionScala ()

    collectionTuples()

  }

  //ma première fonction
  def Comptage_caracteres (texte : String) : Int = {

    trace_appli.info("démarrage du traçage de la classe")
    trace_appli.info(s"le paramètre tracé par Log4J pour cette fonction est : $texte")
    trace_appli.warn(s"Message d'avertissement Log4J interpolation de chaînes : ${10 + 15}")

     if (texte.isEmpty) {
       0
     } else {
       texte.trim.length()
     }
  }
  //syntaxe 2
  def Comptage_caracteres2 (texte : String) : Int = {
    return texte.trim.length()
  }
  //syntaxe 3
  def Comptage_caracteres3 (texte : String) : Int =  texte.trim.length()

  //ma première méthode/procédure
  def getResultat (parametre : Any) : Unit = {
    if (parametre == 10 ) {
      println ("votre valeur est un entier")
    } else {
      println ("votre valeur n'est pas un entier")
    }
  }

  // structures conditionnelles
  def testWhile (valeur_cond : Int) : Unit = {
    var i : Int = 0
    while (i < valeur_cond) {
      println("itération while N° " + i)
      i = i + 1
    }
  }

  def testFor () : Unit = {
    var i : Int = 0
    for (i <- 5 to 15 ) {
      println("itération For N° " + i)
    }
  }

  // Ma première fonction
  def Comptage_caracteres4(texte: String): Int = {
    texte.trim.length()
  }

  //les collections en scala
  def collectionScala () : Unit = {

    val maliste: List[Int] = List(1, 2, 3, 5, 10, 45, 15)
    val liste_s: List[String] = List("julien", "Paul", "jean", "rac", "trec", "joel", "ed", "chris", "maurice")
    val plage_v: List[Int] = List.range(1, 15, 2)

    println(maliste(0))

    for (i <- liste_s) {
      println(i)
    }

    //manipulation des collections à l'aide des fonctions anonymes
    val resultats: List[String] = liste_s.filter(e => e.endsWith("n"))

    for (r <- resultats) {
      println(r)
    }

    val res: Int = liste_s.count(i => i.endsWith("n"))
    println("nombre d'éléments respectant la condition : " + res)

    val maliste2: List[Int] = maliste.map(e => e * 2)

    for (r <- maliste2) {
      println(r)
    }

    val maliste3: List[Int] = maliste.map((e: Int) => e * 2)
    val maliste4: List[Int] = maliste.map(_ * 2)

    val nouvelle_liste: List[Int] = plage_v.filter(p => p > 5)

    val new_list: List[String] = liste_s.map(s => s.capitalize)

    new_list.foreach(e => println("nouvelle liste : " + e))
    nouvelle_liste.foreach(e => println("nouvelle liste : " + e))
    plage_v.foreach(println(_))
  }

  def collectionTuples () : Unit = {
   val tuple_test = (45, "JVC", "False")
   println(tuple_test._3)

   val nouv_personne : Person = new Person ("CHOKOGOUE", "Juvenal", 40)

   val tuple_2 = ("test", nouv_personne, 67 )

    tuple_2.toString().toList
  }


  //table de hachage
  val states = Map(
    "AK" -> "Alaska",
    "IL" -> "Illinois",
    "KY" -> "Kentucky"
  )

  val personne = Map(
    "nom" -> "CHOKOGOUE",
    "prénom" -> "Juvénal",
    "age" -> 45
  )

  // les tableaux ou Array
  val montableau : Array[String] = Array("juv", "jvc", "test")
  montableau.foreach(e => println(e))

  // utilisation d'un gestionnaire d'erreur
  def convert_entier (nombre_chaine : String) : Int = {
    try {
      val nombre : Int = nombre_chaine.toInt
      return nombre
    } catch {
        case ex : Exception => 0
    }

  }

  def lecture_fichier(chemin_fichier : String) : Unit = {
    try {
      val fichier = Source.fromFile(chemin_fichier)
      fichier.getLines()
      fichier.close()
    } catch {
      case ex : FileNotFoundException => trace_appli.error("votre fichier est introuvable. Vérifiez le chemin d'accès"+ ex.printStackTrace())
    }

  }

  def division(numerateur : Int, denominateur : Int) : Double = {
    val resultat = numerateur/denominateur
    return resultat
  }
}