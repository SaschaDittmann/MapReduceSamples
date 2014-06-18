using System;
using Microsoft.Hadoop.MapReduce;

namespace InvertedIndex
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            hadoop.MapReduceJob.ExecuteJob<InvertedIndexJob>();

            Console.ReadKey();
        }
    }
}
