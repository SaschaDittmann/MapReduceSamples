using System.Collections.Generic;
using Microsoft.Hadoop.MapReduce;

namespace Distinct
{
    public class DistinctUserReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            context.EmitKeyValue(key, null);
        }
    }
}
