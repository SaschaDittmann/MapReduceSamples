using System;
using Microsoft.Hadoop.MapReduce;

namespace MinMaxCount
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            hadoop.MapReduceJob.ExecuteJob<MinMaxCountJob>();

            Console.ReadKey();
        }
    }
}
