using System;
using System.IO;
using System.Linq;
using MapReduceSamples.Utils;

namespace BloomFilterDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                var errorRate = Convert.ToSingle(args[1])/100.0f;

                var inputLines = File.ReadAllLines(args[0]);
                var filter = new BloomFilter<string>(inputLines.Length, errorRate);
                Console.WriteLine("Training Bloom Filter with an error rate of {0}% ...", args[1]);
                foreach (var line in inputLines)
                {
                    filter.Add(line);
                }
                Console.WriteLine("Bloom Filter trained with {0} entries.", inputLines.Count());

                filter.Save(args[2]);

                Console.ReadKey();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.ReadKey();
        }
    }
}
