using System;
using System.Collections.Generic;
using Microsoft.Hadoop.MapReduce;

namespace InvertedIndex
{
    public class Concatenator : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            context.EmitKeyValue(key, String.Join(" ", values));
        }
    }
}
