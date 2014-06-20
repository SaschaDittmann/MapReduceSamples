using System;
using System.Globalization;
using Microsoft.Hadoop.MapReduce;
using MapReduceSamples.Utils;

namespace MedianStdDev
{
    public class MedianStdDevMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("CreationDate") || !parsed.ContainsKey("Text"))
            {
                context.IncrementCounter("MedianStdDevMapper", "InvalidRows", 1);
                return;
            }

            DateTime creationDate;
            if (!DateTime.TryParse(parsed["CreationDate"], out creationDate))
            {
                context.IncrementCounter("MedianStdDevMapper", "InvalidCreationDates", 1);
                return;
            }

            var text = parsed["Text"];

            context.EmitKeyValue(
                creationDate.Hour.ToString(CultureInfo.InvariantCulture),
                text.Length.ToString(CultureInfo.InvariantCulture));
        }
    }
}
