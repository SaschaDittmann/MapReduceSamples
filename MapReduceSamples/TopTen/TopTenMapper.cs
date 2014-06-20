using System.Collections.Generic;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace TopTen
{
    public class TopTenMapper : MapperBase
    {
        private readonly SortedList<int, string> _records = new SortedList<int, string>();

        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("Reputation"))
            {
                context.IncrementCounter("Top Ten Mapper", "Invalid Rows", 1);
                return;
            }

            int reputation;
            if (!int.TryParse(parsed["Reputation"], out reputation))
            {
                context.IncrementCounter("Top Ten Mapper", "Invalid Reputations", 1);
                return;
            }

            _records.Add(reputation, inputLine);

            if (_records.Count > 10)
                _records.RemoveAt(0);
        }

        public override void Cleanup(MapperContext context)
        {
            base.Cleanup(context);
            foreach (var record in _records)
                context.EmitKeyValue(null, record.Value);
        }
    }
}
