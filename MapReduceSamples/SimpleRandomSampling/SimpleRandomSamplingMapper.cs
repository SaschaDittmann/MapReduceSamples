using System;
using Microsoft.Hadoop.MapReduce;

namespace SimpleRandomSampling
{
    public class SimpleRandomSamplingMapper : MapperBase
    {
        private readonly Random _rands = new Random();
        private const double SamplingRate = 0.1;

        public override void Map(string inputLine, MapperContext context)
        {
            if (_rands.NextDouble() < SamplingRate)
                context.EmitLine(inputLine);
        }
    }
}
