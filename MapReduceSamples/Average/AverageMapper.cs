using System;
using System.Globalization;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce.Json;

namespace Average
{
    public class AverageMapper : JsonOutMapperBase<CountAverageData>
    {
        public override void Map(string inputLine, JsonMapperContext<CountAverageData> context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("CreationDate") || !parsed.ContainsKey("Text"))
            {
                context.CoreContext.IncrementCounter("Average Mapper", "Invalid Rows", 1);
                return;
            }

            DateTime creationDate;
            if (!DateTime.TryParse(parsed["CreationDate"], out creationDate))
            {
                context.CoreContext.IncrementCounter("Average Mapper", "Invalid Creation Dates", 1);
                return;
            }

            var text = parsed["Text"];

            context.EmitKeyValue(
                creationDate.Hour.ToString(CultureInfo.InvariantCulture),
                new CountAverageData
                {
                    Average = text.Length,
                    Count = 1,
                });
        }
    }
}
