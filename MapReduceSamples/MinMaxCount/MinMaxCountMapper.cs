using System;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce.Json;

namespace MinMaxCount
{
    public class MinMaxCountMapper : JsonOutMapperBase<MinMaxCountData>
    {
        public override void Map(string inputLine, JsonMapperContext<MinMaxCountData> context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("CreationDate") || !parsed.ContainsKey("UserId"))
            {
                context.CoreContext.IncrementCounter("Min Max Count Mapper", "Invalid Rows", 1);
                return;
            }

            DateTime creationDate;
            if (!DateTime.TryParse(parsed["CreationDate"], out creationDate))
            {
                context.CoreContext.IncrementCounter("Min Max Count Mapper", "Invalid Creation Dates", 1);
                return;
            }

            context.EmitKeyValue(parsed["UserId"], new MinMaxCountData
            {
                Min = creationDate,
                Max = creationDate,
                Count = 1,
            });
        }
    }
}
