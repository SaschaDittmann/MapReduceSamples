using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.Json;

namespace MinMaxCount
{
    public class MinMaxCountReducer : JsonInReducerCombinerBase<MinMaxCountData>
    {
        public override void Reduce(string key, IEnumerable<MinMaxCountData> values, ReducerCombinerContext context)
        {
            var data = values.ToList();

            context.EmitKeyValue(key, new MinMaxCountData
            {
                Min = data.Min(v => v.Min),
                Max = data.Max(v => v.Min),
                Count = data.Sum(v => v.Count),
            }.ToString());
        }
    }
}
