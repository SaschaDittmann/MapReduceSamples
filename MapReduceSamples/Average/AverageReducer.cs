using System.Collections.Generic;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.Json;

namespace Average
{
    public class AverageReducer : JsonInReducerCombinerBase<CountAverageData>
    {
        public override void Reduce(string key, IEnumerable<CountAverageData> values, ReducerCombinerContext context)
        {
            float sum = 0;
            float count = 0;

            foreach (var value in values)
            {
                sum += value.Count * value.Average;
                count += value.Count;
            }

            context.EmitKeyValue(key, new CountAverageData
            {
                Average = sum / count,
                Count = count,
            }.ToString());
        }
    }
}
