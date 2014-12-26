import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

val akkaVersion = "2.3.8"

val project = Project(
  id = "things",
  base = file("."),
  settings = SbtMultiJvm.multiJvmSettings ++ Seq(
    name := "Things",
    version := "1.0",
    scalaVersion := "2.11.4",
    scalacOptions ++= Seq("-feature"),
    libraryDependencies ++= Seq(
      // Akka dependencies
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-experimental" % "0.9",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion % "test",
      "org.scala-lang.modules" %% "scala-xml" % "1.0.2" % "test",
      "org.scalatest" %% "scalatest" % "2.1.3" % "test",
      "commons-io" % "commons-io" % "2.4" % "test",
      "com.github.krasserm" %% "akka-persistence-cassandra" % "0.3.5",
      // Runtime
      "ch.qos.logback" % "logback-classic" % "1.0.13",
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
    ),
    resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
    // make sure that MultiJvm test are compiled by the default test compilation
    compile in MultiJvm <<= (compile in MultiJvm) triggeredBy (compile in Test),
    // disable parallel tests
    parallelExecution in Test := false,
    // make sure that MultiJvm tests are executed by the default test target,
    // and combine the results from ordinary test and multi-jvm tests
    executeTests in Test <<= (executeTests in Test, executeTests in MultiJvm) map {
      case (testResults, multiNodeResults) =>
        val overall =
          if (testResults.overall.id < multiNodeResults.overall.id)
            multiNodeResults.overall
          else
            testResults.overall
        Tests.Output(overall,
          testResults.events ++ multiNodeResults.events,
          testResults.summaries ++ multiNodeResults.summaries)
    }
  )
) configs (MultiJvm)