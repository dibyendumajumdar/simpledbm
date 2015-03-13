# Introduction #

The following instructions are for setting up Netbeans to
develop SimpleDBM RSS.

# Notice #
This hasn't been tested recently so can't guarantee that the process will work.

# Steps #

Follow these steps:
  * If you don't have Maven 2 - download it and extract it somewhere. Update you PATH settings. Note that Maven requires JAVA\_HOME to be set.
  * Checkout SimpleDBM projects
  * Create netbeans project files.
```
mvn netbeans-freeform:generate-netbeans-project
```
  * Open the project in Netbeans. Netbeans sets the default source level to 1.4. Change this in the Project properties to 1.5.