using Microsoft.Hadoop.MapReduce.Json;
using System.Collections.Generic;
using System.Linq;

namespace MinMaxCount
{
    public class MinMaxCountReducerCombiner : JsonInOutReducerCombinerBase<MinMaxCountData, MinMaxCountData>
    {
        public override void Reduce(string key, IEnumerable<MinMaxCountData> values, JsonReducerCombinerContext<MinMaxCountData> context)
        {
            var data = values.ToList();

            context.EmitKeyValue(key, new MinMaxCountData
            {
                Min = data.Min(v => v.Min),
                Max = data.Max(v => v.Min),
                Count = data.Sum(v => v.Count),
            });
        }
    }
}
