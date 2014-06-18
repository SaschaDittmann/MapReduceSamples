using System;
using System.Collections.Generic;
using Microsoft.Hadoop.MapReduce;

namespace TopTen
{
    class Program
    {
        static void Main(string[] args)
        {
            var hadoop = Hadoop.Connect();
            var config = new HadoopJobConfiguration
            {
                InputPath = "/samples/users",
                OutputFolder = "output/TopTen",
            };
            config.Defines.Add(new KeyValuePair<string, string>("mapred.reduce.tasks", "1"));
            hadoop.MapReduceJob.Execute<TopTenMapper, TopTenReducer>(config);

            Console.ReadKey();
        }
    }
}
