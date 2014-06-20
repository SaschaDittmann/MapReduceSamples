using System;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace StructuredToHierarchical
{
    public class PostCommentHierarchyMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("Id"))
            {
                context.IncrementCounter("Structured To Hierarchical", "Invalid Rows", 1);
                return;
            }

            if (parsed.ContainsKey("PostId"))
                context.EmitKeyValue(parsed["PostId"], String.Format("C{0}", inputLine));
            else
                context.EmitKeyValue(parsed["Id"], String.Format("P{0}", inputLine));
        }
    }
}
