using System.Collections.Generic;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace TopTen
{
    public class TopTenReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            var records = new SortedList<int, string>();

            foreach (var value in values)
            {
                var parsed = XmlUtils.ParseXml(value);
                var reputation = int.Parse(parsed["Reputation"]);

                records.Add(reputation, value);

                if (records.Count > 10)
                    records.RemoveAt(0);
            }

            foreach (var record in records)
                context.EmitLine(record.Value);
        }
    }
}
