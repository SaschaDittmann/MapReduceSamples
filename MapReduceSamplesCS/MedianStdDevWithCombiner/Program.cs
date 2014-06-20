using System;
using Microsoft.Hadoop.MapReduce;

namespace MedianStdDevWithCombiner
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            hadoop.MapReduceJob.ExecuteJob<MedianStdDevJob>();

            Console.ReadKey();
        }
    }
}
