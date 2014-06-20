using System;
using Microsoft.Hadoop.MapReduce;

namespace SimpleRandomSampling
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/data/posts",
                OutputFolder = "output/SimpleRandomSampling",
            };
            hadoop.MapReduceJob.Execute<SimpleRandomSamplingMapper>(config);

            Console.ReadKey();
        }
    }
}
