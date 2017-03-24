# SparkCredit

# Build
To build this app:

```
   git clone https://github.com/hydrator/topn.git
   mvn clean package
```    

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

# Deployment

You can deploy your plugins using the CDAP CLI:

    > load artifact <target/topn-<version>.jar> config-file <target/topn-<version>.json>

For example, if your artifact is named 'topn-<version>':

    > load artifact target/topn-<version>.jar config-file target/topn-<version>.json