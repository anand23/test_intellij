name := "test"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "com.databricks" % "spark-xml_2.11" % "0.4.1",
  "org.apache.spark" %% "spark-streaming" % "2.4.4"
)