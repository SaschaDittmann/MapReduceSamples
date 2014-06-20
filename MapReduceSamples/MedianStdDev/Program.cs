using System;
using Microsoft.Hadoop.MapReduce;

namespace MedianStdDev
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
