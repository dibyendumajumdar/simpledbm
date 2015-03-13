# Introduction #

SimpleDBM releases are built using maven [release-plugin](http://maven.apache.org/plugins/maven-release-plugin/). Releases are deployed to the Maven Central Repository.

# Pre-requisites #
  * Maven
  * PGP
  * Java 1.6 or above

# Details #

The steps for creating a release are:
  * Checkout a SimpleDBM project into a new folder. Do not use your existing Eclipse project as the release-plugin does not like local files. To checkout you need to execute a command similar to this:
```
hg clone https://simpledbm.googlecode.com/hg/simpledbm/ simpledbm
```
  * Make sure that you have added following to your maven settings file (located under `$HOME/.m2/settings.xml`)
```
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                      http://maven.apache.org/xsd/settings-1.0.0.xsd">
 <servers>
  <server>
    <id>simpledbm.googlecode.com</id>
    <username>Maintainer username</username>
    <password>Maintainer password</password>
  </server> 
    <server>
      <id>sonatype-nexus-snapshots</id>
      <username>Maintainer username</username>
      <password>Maintainer password</password> 
    </server>
    <server>
      <id>sonatype-nexus-staging</id>
      <username>Maintainer username</username>
      <password>Maintainer password</password>
    </server>
 </servers>
</settings>
```

  * Ensure also that the Mercurial user id and password are stored in `$HOME/.hg/hgrc`.
```
[auth]
simpledbm.prefix = https://code.google.com/p/simpledbm/
simpledbm.username = Maintainer username
simpledbm.password = Maintainer password
```

  * Cd into the build folder and execute following. Note that we run tests first to ensure everything is okay. The `find` command below is for Linux / Mac OSX environments; it finds and deletes the folders named `testdata` that are created during the test run.
```
mvn test
mvn clean
find .. -name 'testdata' -exec rm -rf {} \;
mvn release:clean
mvn release:prepare
mvn release:perform
```

> Note that you will be prompted for the PGP passphrase.


  * Final step is to promote the release - assuming there were no errors above. To do this, login to [SonaType Nexus](https://oss.sonatype.org/) using your maintainer id. Under **Build Promotion** select _Staging Repositories_ and scroll to the bottom of the list. You should see a repository named starting `orgsimpledbm` and ending `-1000`. Select this repository, and hit on button **Close**. If this completes successfully then hit **Release**. More detailed instructions on this process can be found at [Releasing the Deployment](http://central.sonatype.org/pages/releasing-the-deployment.html) page.



