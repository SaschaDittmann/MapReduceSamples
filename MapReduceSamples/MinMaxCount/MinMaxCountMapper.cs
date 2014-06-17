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
                context.CoreContext.IncrementCounter("MinMaxCountMapper", "InvalidRows", 1);
                return;
            }

            DateTime creationDate;
            if (!DateTime.TryParse(parsed["CreationDate"], out creationDate))
            {
                context.CoreContext.IncrementCounter("MinMaxCountMapper", "InvalidCreationDates", 1);
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
