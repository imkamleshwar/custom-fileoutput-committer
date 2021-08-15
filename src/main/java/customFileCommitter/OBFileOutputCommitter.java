package customFileCommitter;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

/*
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
*/

/**
 * An {@link OutputCommitter} that commits files specified
 * in job output directory i.e. ${mapreduce.output.fileoutputformat.outputdir}.
 **/
@SuppressWarnings("CheckStyle")
@InterfaceAudience.Public
@InterfaceStability.Stable
public class OBFileOutputCommitter extends OutputCommitter {

    public static final Log LOG = LogFactory.getLog(
            "org.apache.hadoop.mapred.FileOutputCommitter");

    /**
     * Temporary directory name
     */
//  public static String TEMP_DIR_NAME = FileOutputCommitter.pendingDirName;
//  public static String SUCCEEDED_FILE_NAME = FileOutputCommitter.SUCCEEDED_FILE_NAME;
//  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER;

    private static Path getOutputPath(JobContext context) {
        JobConf conf = context.getJobConf();
        return FileOutputFormat.getOutputPath(conf);
    }

    private static Path getOutputPath(TaskAttemptContext context) {
        JobConf conf = context.getJobConf();
        return FileOutputFormat.getOutputPath(conf);
    }

    private FileOutputCommitter wrapped = null;

    private FileOutputCommitter
    getWrapped(JobContext context) throws IOException {
        if (wrapped == null) {
            wrapped = new FileOutputCommitter(
                    getOutputPath(context), context);
            /*LOG.info(String.format("The pending suffix is %s", FileOutputCommitter.pendingDirName));
            System.out.println(String.format("The pending suffix is %s", FileOutputCommitter.pendingDirName));*/
        }
        return wrapped;
    }

    private FileOutputCommitter
    getWrapped(TaskAttemptContext context) throws IOException {
        if (wrapped == null) {
            wrapped = new FileOutputCommitter(
                    getOutputPath(context), context);
        }
        return wrapped;
    }

    /**
     * Compute the path where the output of a given job attempt will be placed.
     *
     * @param context the context of the job.  This is used to get the
     *                application attempt id.
     * @return the path to store job attempt data.
     */
    @Private
    Path getJobAttemptPath(JobContext context) {
        Path out = getOutputPath(context);
        return out == null ? null :
                org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
                        .getJobAttemptPath(context, out);
    }

    @Private
    public Path getTaskAttemptPath(TaskAttemptContext context) throws IOException {
        Path out = getOutputPath(context);
        return out == null ? null : getTaskAttemptPath(context, out);
    }

    private Path getTaskAttemptPath(TaskAttemptContext context, Path out) throws IOException {
        Path workPath = FileOutputFormat.getWorkOutputPath(context.getJobConf());
        if (workPath == null && out != null) {
            return org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
                    .getTaskAttemptPath(context, out);
        }
        return workPath;
    }

    /**
     * Compute the path where the output of a committed task is stored until
     * the entire job is committed.
     *
     * @param context the context of the task attempt
     * @return the path where the output of a committed task is stored until
     * the entire job is committed.
     */
    @Private
    Path getCommittedTaskPath(TaskAttemptContext context) {
        Path out = getOutputPath(context);
        return out == null ? null :
                org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
                        .getCommittedTaskPath(context, out);
    }

    public Path getWorkPath(TaskAttemptContext context, Path outputPath)
            throws IOException {
        return outputPath == null ? null : getTaskAttemptPath(context, outputPath);
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        getWrapped(context).setupJob(context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
        getWrapped(context).commitJob(context);
    }

    @Override
    @Deprecated
    public void cleanupJob(JobContext context) throws IOException {
        getWrapped(context).cleanupJob(context);
    }

    @Override
    public void abortJob(JobContext context, int runState)
            throws IOException {
        JobStatus.State state;
        if (runState == JobStatus.State.RUNNING.getValue()) {
            state = JobStatus.State.RUNNING;
        } else if (runState == JobStatus.State.SUCCEEDED.getValue()) {
            state = JobStatus.State.SUCCEEDED;
        } else if (runState == JobStatus.State.FAILED.getValue()) {
            state = JobStatus.State.FAILED;
        } else if (runState == JobStatus.State.PREP.getValue()) {
            state = JobStatus.State.PREP;
        } else if (runState == JobStatus.State.KILLED.getValue()) {
            state = JobStatus.State.KILLED;
        } else {
            throw new IllegalArgumentException(runState + " is not a valid runState.");
        }
        getWrapped(context).abortJob(context, state);
    }

    @Override
    public void setupTask(TaskAttemptContext context) throws IOException {
        getWrapped(context).setupTask(context);
    }

    @Override
    public void commitTask(TaskAttemptContext context) throws IOException {
        getWrapped(context).commitTask(context, getTaskAttemptPath(context));
    }

    @Override
    public void abortTask(TaskAttemptContext context) throws IOException {
        getWrapped(context).abortTask(context, getTaskAttemptPath(context));
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext context)
            throws IOException {
        return getWrapped(context).needsTaskCommit(context, getTaskAttemptPath(context));
    }

    @Override
    @Deprecated
    public boolean isRecoverySupported() {
        return true;
    }

    @Override
    public boolean isRecoverySupported(JobContext context) throws IOException {
        return getWrapped(context).isRecoverySupported(context);
    }

    @Override
    public void recoverTask(TaskAttemptContext context)
            throws IOException {
        getWrapped(context).recoverTask(context);
    }
}