using Microsoft.Hadoop.MapReduce;

namespace ReduceSideJoin
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/samples/users",
                OutputFolder = "output/UserCommentJoin",
            };
            config.AdditionalInputPath.Add("/samples/comments");
            hadoop.MapReduceJob.Execute<UserCommentJoinMapper, UserCommentJoinReducer>(config);
        }
    }
}
