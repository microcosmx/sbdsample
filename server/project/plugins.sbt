
resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"


addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.1")
