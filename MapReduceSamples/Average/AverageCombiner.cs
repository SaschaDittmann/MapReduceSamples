using System.Collections.Generic;
using Microsoft.Hadoop.MapReduce.Json;

namespace Average
{
    public class AverageCombiner : JsonInOutReducerCombinerBase<CountAverageData, CountAverageData>
    {
        public override void Reduce(string key, IEnumerable<CountAverageData> values, JsonReducerCombinerContext<CountAverageData> context)
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
            });
        }
    }
}
