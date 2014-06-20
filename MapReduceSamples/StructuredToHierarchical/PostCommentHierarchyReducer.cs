using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml;
using System.Xml.Linq;
using Microsoft.Hadoop.MapReduce;

namespace StructuredToHierarchical
{
    public class PostCommentHierarchyReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            string post = null;
            var comments = new List<string>();
            foreach (var value in values)
            {
                if (value[0] == 'P')
                    post = value.Substring(1).Trim();
                else
                    comments.Add(value.Substring(1).Trim());
            }

            if (post != null)
            {
                var newPost = CreatePost(post, comments);
                if (newPost != null)
                    context.EmitLine(newPost);
            }
        }

        public static string CreatePost(string post, IEnumerable<string> comments)
        {
            var postXml = XDocument.Parse(post);

            if (postXml.Root == null)
                return null;

            postXml.Root.Name = "post";

            foreach (var comment in comments.Select(XElement.Parse))
            {
                comment.Name = "comment";
                postXml.Root.Add(comment);
            }
            
            var settings = new XmlWriterSettings { OmitXmlDeclaration = true };
            using(var stream = new MemoryStream())
            {
                using (var writer = XmlWriter.Create(stream, settings))
                {
                    postXml.Save(writer);
                    writer.Flush();
                }

                stream.Position = 0;
                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
