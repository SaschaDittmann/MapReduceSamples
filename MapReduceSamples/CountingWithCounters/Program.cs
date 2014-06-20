using System;
using Microsoft.Hadoop.MapReduce;

namespace CountingWithCounters
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            hadoop.MapReduceJob.Execute<CountNumUsersByStateMapper>(new HadoopJobConfiguration
            {
                InputPath = "/samples/user",
                OutputFolder = "output/CountNumUsersByState",
            });

            Console.ReadKey();
        }
    }
}
