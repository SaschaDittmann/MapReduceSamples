using System;
using Microsoft.Hadoop.MapReduce;

namespace MapReduceSamples.Utils
{
    public class TestReducerCombinerContext : ReducerCombinerContext
    {
        public TestReducerCombinerContext(bool isCombiner) : base(isCombiner)
        {
        }

        public override void EmitKeyValue(string key, string value)
        {
            Console.WriteLine("EmitKeyValue: Key \"{0}\", \"{1}\"", key, value);
        }

        public override void EmitLine(string line)
        {
            Console.WriteLine("EmitLine: {0}", line);
        }

        public override void IncrementCounter(string category, string counterName, int increment)
        {
            Console.WriteLine(
                "IncrementCounter: Category \"{0}\", Counter Name \"{1}\", Increment \"{2}\"",
                category,
                counterName,
                increment);
        }

        public override void IncrementCounter(string counterName, int increment)
        {
            Console.WriteLine(
                "IncrementCounter: Counter Name \"{0}\", Increment \"{1}\"",
                counterName,
                increment);
        }

        public override void IncrementCounter(string counterName)
        {
            Console.WriteLine(
                "IncrementCounter: Counter Name \"{0}\"",
                counterName);
        }

        public override void Log(string message)
        {
            Console.WriteLine(
                "Log: {0}",
                message);
        }
    }
}
