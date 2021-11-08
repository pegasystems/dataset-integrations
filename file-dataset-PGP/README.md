## Building from source
In order to build the sample from source, you need to have access to **coreBuildDistributionImage** and have **Gradle 4.8** or higher.
1. Extract **coreBuildDistributionImage.zip/archives/pegadbinstall-classes.zip** to a temporary folder
2. Copy jar file to the **libs** folder:
 - pegadbinstall-classes/lib/pega/prpublic.jar → libs/prpublic.jar
3. Run `./gradlew clean build -x test` to generate **<nobr>build/libs/file-dataset-PGP-1.0-SNAPSHOT.jar<nobr>** component jar, 
   which can be imported into Pega platform

## Running tests
1. Copy jar files to the **libs** folder:
 - pegadbinstall-classes/lib/pega/prpublic.jar → libs/prpublic.jar
2. Run `./gradlew clean test`

## Supported Pega versions
- 8.5
- 8.6
- 8.7

## GnuPGP compatibility
This implementation is compatible with GnuPGP version 2.2 

## Usage

### Required jars

Following jars need to be uploaded to the Pega Platform versions 8.6 and earlier for custom processing to work:
 - org.bouncycastle:bcpg-jdk15on:1.66 
 - org.bouncycastle:bcprov-jdk15on:1.66
 - name.neuhalfen.projects.crypto.bouncycastle.openpgp:bouncy-gpg:2.2.0
 
### Key requirements

Keys need to be generated using AES256 algorithm and encoded in base 64.

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
