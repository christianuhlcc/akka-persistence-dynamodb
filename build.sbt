name := "akka-persistence-dynamodb"

scalaVersion       := "2.11.8"
crossScalaVersions := Seq("2.11.8", "2.12.1")
crossVersion       := CrossVersion.binary

val akkaVersion = "2.4.14"
val amzVersion = "1.11.66"

libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-dynamodb"  % "0.6",
  "com.typesafe.akka"   %% "akka-persistence"             % akkaVersion,
  "com.typesafe.akka"   %% "akka-stream"                  % akkaVersion,
  "com.typesafe.akka"   %% "akka-persistence-tck"         % akkaVersion   % "test",
  "com.typesafe.akka"   %% "akka-testkit"                 % akkaVersion   % "test",
  "org.scalatest"       %% "scalatest"                    % "3.0.1"       % "test",
  "commons-io"          % "commons-io"                    % "2.4"         % "test",
  "org.hdrhistogram"    % "HdrHistogram"                  % "2.1.8"       % "test",
  "org.mockito"         % "mockito-core"                  % "2.7.19"      % "test"
)

parallelExecution in Test := false
logBuffered := false
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings
ScalariformKeys.preferences in Compile  := formattingPreferences
ScalariformKeys.preferences in Test     := formattingPreferences

def formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(SpacesAroundMultiImports, true)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(AlignArguments, true)
}
