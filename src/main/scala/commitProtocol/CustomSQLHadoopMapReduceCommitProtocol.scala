package commitProtocol

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.{OutputCommitter, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

class CustomSQLHadoopMapReduceCommitProtocol(jobId: String,
                                             path: String,
                                             dynamicPartitionOverwrite: Boolean = false)
  extends CustomHadoopMapReduceCommitProtocol(jobId, path, dynamicPartitionOverwrite)
    with Serializable with Logging {

  override protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    var committer = super.setupCommitter(context)

    val configuration = context.getConfiguration
    val clazz =
      configuration.getClass(SQLConf.OUTPUT_COMMITTER_CLASS.key, null, classOf[OutputCommitter])

    if (clazz != null) {
      logInfo(s"Using user defined output committer class ${clazz.getCanonicalName}")

      // Every output format based on org.apache.hadoop.mapreduce.lib.output.OutputFormat
      // has an associated output committer. To override this output committer,
      // we will first try to use the output committer set in SQLConf.OUTPUT_COMMITTER_CLASS.
      // If a data source needs to override the output committer, it needs to set the
      // output committer in prepareForWrite method.
      if (classOf[FileOutputCommitter].isAssignableFrom(clazz)) {
        // The specified output committer is a FileOutputCommitter.
        // So, we will use the FileOutputCommitter-specified constructor.
        val ctor = clazz.getDeclaredConstructor(classOf[Path], classOf[TaskAttemptContext])
        committer = ctor.newInstance(new Path(path), context)
      } else {
        // The specified output committer is just an OutputCommitter.
        // So, we will use the no-argument constructor.
        val ctor = clazz.getDeclaredConstructor()
        committer = ctor.newInstance()
      }
    }
    logInfo(s"Using output committer class ${committer.getClass.getCanonicalName}")
    committer
  }
}
