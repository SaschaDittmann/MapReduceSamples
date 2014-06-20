using System;
using System.Text.RegularExpressions;
using System.Web;
using System.Linq;
using MapReduceSamples.Utils;
using Microsoft.Hadoop.MapReduce;

namespace InvertedIndex
{
    public class WikipediaExtractor : MapperBase
    {
        private readonly Regex _urlRegex = new Regex(@"(http|https)\://[a-zA-Z0-9\-\.]+\.[a-zA-Z]{2,3}(:[a-zA-Z0-9]*)?/?([a-zA-Z0-9\-\._\?\,\'/\\\+&amp;%\$#\=~])*");
        public override void Map(string inputLine, MapperContext context)
        {
            var parsed = XmlUtils.ParseXml(inputLine);

            if (parsed == null || !parsed.ContainsKey("Body") || !parsed.ContainsKey("PostTypeId") || !parsed.ContainsKey("Id"))
            {
                context.IncrementCounter("WikipediaExtractor", "InvalidRows", 1);
                return;
            }

            var body = HttpUtility.HtmlDecode(parsed["Body"]).ToLower();
            var postTypeId = parsed["PostTypeId"];
            
            // if the body is null, or the post is a question (1), skip
            if (String.IsNullOrEmpty(body) || postTypeId == "1")
                return;

            foreach (var url in _urlRegex.Matches(body)
                .Cast<object>()
                .Select(url => url.ToString())
                .Where(url => url.IndexOf("wikipedia.org", StringComparison.Ordinal) >= 0))
            {
                context.EmitKeyValue(url, parsed["Id"]);
            }
        }
    }
}
