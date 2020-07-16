// utilisation du modèle de test FunSuite

import org.scalatest
import org.scalatest.funsuite.AnyFunSuite

class UnitTestBigDataScalaTest extends AnyFunSuite {

    test("la division doit renvoyer 10") {
      assert(HelloWorldBigData.division(20,2) === 10 )
    }

  test("la division doit renvoyer une erreur de type ArithmeticException") {
    assertThrows[ArithmeticException](
      HelloWorldBigData.division(20,0)
    )
  }
}
