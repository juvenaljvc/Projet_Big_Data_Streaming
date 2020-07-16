// utilisation de la suite FlatSpecs
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._

class FlatSpecTests extends AnyFlatSpec with Matchers {

  "la division" should("renvoyer 10") in {
    assert(HelloWorldBigData.division(20,2) === 10)
  }

  "an arithmetical error" should( "be thrown") in {
    an [ArithmeticException] should be thrownBy(HelloWorldBigData.division(20,0))
  }

  it should("send an OutofBound Error") in {
    var liste_fruits : List[String] = List("banane", "pamplemousse", "goyave")
    assertThrows[IndexOutOfBoundsException] (liste_fruits(4))
  }

  it should ("returns the starting letters of the string") in {
    var chaine : String = "chaîne de caractères"
    chaine should startWith("c")
  }

}
