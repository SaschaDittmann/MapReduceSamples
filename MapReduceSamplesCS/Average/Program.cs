using System;
using System.IO;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.Json;

namespace Average
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            hadoop.MapReduceJob.ExecuteJob<AverageJob>();

            Console.ReadKey();
        }
    }
}
