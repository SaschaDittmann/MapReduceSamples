using System;
using Microsoft.Hadoop.MapReduce;

namespace Distinct
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/samples/comments",
                OutputFolder = "output/DistictUser",
            };
            hadoop.MapReduceJob.Execute<DistinctUserMapper, DistinctUserReducer, DistinctUserReducer>(config);

            Console.ReadKey();
        }
    }
}
