import mill._, scalalib._, scalafmt._

trait root extends ScalaModule with ScalafmtModule {
  def scalaVersion = "2.13.15"
  def forkArgs =
    Seq("-Xmx4g", "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED")

  object test extends ScalaTests with TestModule.ScalaTest {
    override def ivyDeps = Agg(ivy"org.scalatest::scalatest:3.2.19")
  }

}

object sprk extends root {
  def scalaVersion = "2.12.17"
  def ivyDeps = Agg(
    ivy"org.apache.spark:spark-core_2.12:3.4.1",
    ivy"org.apache.spark:spark-sql_2.12:3.4.1",
    ivy"org.scala-lang:scala-library:2.12.17"
  )
}