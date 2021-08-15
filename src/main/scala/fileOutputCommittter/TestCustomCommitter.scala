package fileOutputCommittter

import org.apache.hadoop.mapreduce.{JobContext, OutputCommitter, TaskAttemptContext}

class TestCustomCommitter extends OutputCommitter {
  override def setupJob(jobContext: JobContext): Unit = ???

  override def setupTask(taskContext: TaskAttemptContext): Unit = ???

  override def needsTaskCommit(taskContext: TaskAttemptContext): Boolean = ???

  override def commitTask(taskContext: TaskAttemptContext): Unit = ???

  override def abortTask(taskContext: TaskAttemptContext): Unit = ???
}
