package azure

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object AzureUtilities {

  // Function for mounting a blob store in databricks
  def mountBlobDatabricks(container: String, mountPoint: String, storageAccount: String, secretScope: String, secretKey: String): Boolean = {
    val SASKey = dbutils.secrets.get(scope = secretScope, key = secretKey)

    val SourceStr = s"wasbs://$container@$storageAccount.blob.core.windows.net/"
    val ConfKey = s"fs.azure.sas.$container.$storageAccount.blob.core.windows.net"

    try {
      dbutils.fs.unmount(s"$mountPoint") // Use this to unmount as needed
    } catch {
      case ioe: java.rmi.RemoteException => println(s"$mountPoint already unmounted")
    }

    try {
      dbutils.fs.mount(
        source = SourceStr,
        mountPoint = mountPoint,
        extraConfigs = Map(ConfKey -> SASKey)
      )
    }
    catch {
      case e: Exception =>
        println(s"Unable to mount $mountPoint. Exception: ${
          e.getMessage
        }")
        throw e
    }
  }
}
