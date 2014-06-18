using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce.Json;

namespace MedianStdDevWithCombiner
{
    public class MedianStdDevCombiner : JsonOutReducerCombinerBase<MedianStdDevData>
    {
        public override void Reduce(string key, IEnumerable<string> values, JsonReducerCombinerContext<MedianStdDevData> context)
        {
            var query = values
                .Select(int.Parse)
                .GroupBy(v => v)
                .Select(grp => new MedianStdDevData
                {
                    Value = grp.Key,
                    Count = grp.Count(),
                });

            foreach (var value in query)
            {
                context.EmitKeyValue(key, value);
            }
        }
    }
}
