using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace Distinct
{
    public class DistinctUserMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("UserId"))
            {
                context.IncrementCounter("Distinct User Mapper", "Invalid Rows", 1);
                return;
            }

            context.EmitKeyValue(parsed["UserId"], null);
        }
    }
}
