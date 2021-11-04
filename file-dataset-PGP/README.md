## Building from source
In order to build the sample from source, you need to have access to **coreBuildDistributionImage** and have **Gradle 4.8** or higher.
1. Extract **coreBuildDistributionImage.zip/archives/pegadbinstall-classes.zip** to a temporary folder
2. Copy jar file to the **libs** folder:
 - pegadbinstall-classes/lib/pega/prpublic.jar → libs/prpublic.jar
 - pegadbinstall-classes/lib/pega/bigdata-utils.jar → libs/bigdata-utils.jar
3. Run `./gradlew clean build -x test` to generate **<nobr>build/libs/file-dataset-PGP-1.0-SNAPSHOT.jar<nobr>** component jar, 
   which can be imported into Pega platform

## Running tests
1. Copy jar files to the **libs** folder:
 - pegadbinstall-classes/lib/pega/prpublic.jar → libs/prpublic.jar
 - pegadbinstall-classes/lib/pega/bigdata-utils.jar → libs/bigdata-utils.jar
2. Run `./gradlew clean test`

## Running integration tests

Integration tests are checking how that implementation is working in conjunction with custom processing layer without 
installing the jar into Pega platform.

_To run such test you will need to have access to particular jars which are not publicly available_

1. Copy jar files from **Building from source** section
2. Copy jar files to the **libs** folder:
- pegadbinstall-classes/lib/pega/prlog4jcustomappender.jar → libs/prlog4jcustomappender.jar
- pegadbinstall-classes/lib/pega/prprivcommon.jar → libs/prprivcommon.jar
- pegadbinstall-classes/lib/pega/pricu2jdk.jar → libs/pricu2jdk.jar
- pegadbinstall-classes/lib/pega/prenginext.jar → libs/prenginext.jar
- pegadbinstall-classes/lib/pega/prcommons-lang-2.5.1.jar → libs/prcommons-lang-2.5.1.jar
- libs/prbootstrap.jar - **not part of pega distribution, ask Pega developers for support**
- libs/prbootstrap-api.jar - **not part of pega distribution, ask Pega developers for support**
1. Run `./gradlew clean integrationTest`

## Usage

### PGPKeys page

The keys used for encryption are referenced from data page "D_PGPKeys". The page must have the following properties:
- PrivateKey
- PublicKey
- Passphrase

### Using encryption

Steps required to use PGP encryption in file data set:

1. In "File configuration" section of file data set configuration select "Custom stream processing"
2. In "Java class with reader implementation" put "com.pega.bigdata.dataset.file.processor.PgpDecryptor"
3. In "Java class with writer implementation" put "com.pega.bigdata.dataset.file.processor.PgpEncryptor"


