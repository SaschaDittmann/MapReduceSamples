using System;
using System.Xml.Linq;
using Microsoft.Hadoop.MapReduce;

namespace StructuredToHierarchical
{
    public class QuestionAnswerMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            try
            {
                var doc = XDocument.Parse(inputLine);
                if (doc.Root == null)
                    return;

                var postTypeId = Convert.ToInt32(doc.Root.Attribute("PostTypeId").Value);
                if (postTypeId == 1)
                    context.EmitKeyValue(doc.Root.Attribute("Id").Value, String.Format("Q{0}", inputLine));
                else
                    context.EmitKeyValue(doc.Root.Attribute("ParentId").Value, String.Format("A{0}", inputLine));
            }
            catch (Exception)
            {
                context.IncrementCounter("Structured To Hierarchical", "Invalid Rows", 1);
            }
        }
    }
}
