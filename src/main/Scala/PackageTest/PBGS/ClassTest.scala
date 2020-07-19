package PackageTest.PBGS

class ClassTest {

  def comptage_package (texte : String) : Int = {
    if (texte.isEmpty) {
      0
    } else {
      texte.trim.length()
    }

  }
}
