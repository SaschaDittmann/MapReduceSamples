using System;
using Microsoft.Hadoop.MapReduce;

namespace BloomFiltering
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/data/posts",
                OutputFolder = "output/BloomFiltering",
            };
            config.FilesToInclude.Add("filter.bloom");
            hadoop.MapReduceJob.Execute<BloomFilteringMapper>(config);

            Console.ReadKey();
        }
    }
}
