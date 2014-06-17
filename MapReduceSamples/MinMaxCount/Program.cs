using System;
using System.IO;
using System.Linq;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.Json;

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

        static void Test()
        {
            var context = new JsonMapperContext<MinMaxCountData>(
                new TestContext()
            );

            var mapper = new MinMaxCountMapper();

            foreach (var line in File.ReadAllLines(@"C:\Users\SaschaDi\Downloads\stackexchange\stackoverflow\Comments.Sample.txt"))
            {
                mapper.Map(line, context);
            }
        }
    }
}
