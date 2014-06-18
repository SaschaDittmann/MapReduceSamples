using System;
using Microsoft.Hadoop.MapReduce;

namespace StructuredToHierarchical
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/samples/posts",
                OutputFolder = "tmp/StructuredToHierarchical",
            };
            config.AdditionalInputPath.Add("/samples/comments");
            var step1 = hadoop.MapReduceJob.Execute<PostCommentHierarchyMapper, PostCommentHierarchyReducer>(config);
            if (step1.Info.ExitCode != 0)
                return;

            config = new HadoopJobConfiguration
            {
                InputPath = "tmp/StructuredToHierarchical",
                OutputFolder = "output/StructuredToHierarchical",
            };
            hadoop.MapReduceJob.Execute<QuestionAnswerMapper, QuestionAnswerReducer>(config);

            hadoop.StorageSystem.Delete("tmp/StructuredToHierarchical");
            
            Console.ReadKey();
        }
    }
}
