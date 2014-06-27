using Microsoft.Hadoop.MapReduce;

namespace WordCount
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            hadoop.MapReduceJob.Execute<WordCountMapper, WordCountReducerCombiner, WordCountReducerCombiner>(
                new HadoopJobConfiguration
                {
                    InputPath = "/samples/texte",
                    OutputFolder = "output/WordCount",
                });
        }
    }
}
