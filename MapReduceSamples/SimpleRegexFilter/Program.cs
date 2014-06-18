using System;
using Microsoft.Hadoop.MapReduce;

namespace SimpleRegexFilter
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/data/posts",
                OutputFolder = "output/SimpleRegexFilter",
            };
            hadoop.MapReduceJob.Execute<RegexMapper>(config);

            Console.ReadKey();
        }
    }
}
