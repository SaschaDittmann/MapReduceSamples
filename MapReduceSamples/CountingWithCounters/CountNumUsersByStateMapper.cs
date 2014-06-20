using System;
using System.Collections.Generic;
using System.Linq;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace CountingWithCounters
{
    public class CountNumUsersByStateMapper : MapperBase
    {
        public const String StateCounterGroup = "States";
        public const String InvalidRows = "Invalid Rows";
        public const String UnknownCounter = "Unknown";
        public const String NullOrEmptyCounter = "Null or Empty";

        private static readonly List<string> States = new List<string> 
        {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
            "SF", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        };

        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null)
            {
                context.IncrementCounter(StateCounterGroup, InvalidRows, 1);
                return;
            }

            if (parsed.ContainsKey("Location") && !String.IsNullOrEmpty(parsed["Location"]))
            {
                var locations = parsed["Location"].ToUpper().Split(' ');
                var unknown = true;
                foreach (var state in locations.Where(state => States.Contains(state)))
                {
                    context.IncrementCounter(StateCounterGroup, state, 1);
                    unknown = false;
                    break;
                }
                if (unknown)
                    context.IncrementCounter(StateCounterGroup, UnknownCounter, 1);
            }
            else
                context.IncrementCounter(StateCounterGroup, UnknownCounter, 1);
        }
    }
}
