using System;
using Microsoft.Hadoop.MapReduce;
using MapReduceSamples.Utils;

namespace ReduceSideJoin
{
    public class UserCommentJoinMapper : MapperBase
    {

        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("Id"))
            {
                context.IncrementCounter("Reduce Side Join", "Invalid Rows", 1);
                return;
            }

            if (parsed.ContainsKey("UserId"))
                context.EmitKeyValue(parsed["UserId"], String.Format("R{0}", inputLine));
            else
                context.EmitKeyValue(parsed["Id"], String.Format("L{0}", inputLine));
        }
    }
}
