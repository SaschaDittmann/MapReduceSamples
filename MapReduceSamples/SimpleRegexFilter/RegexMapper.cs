using System.Text.RegularExpressions;
using Microsoft.Hadoop.MapReduce;

namespace SimpleRegexFilter
{
    public class RegexMapper : MapperBase
    {
        private readonly Regex _regex = new Regex("hdinsight|hadoop", RegexOptions.IgnoreCase);

        public override void Map(string inputLine, MapperContext context)
        {
            if (_regex.IsMatch(inputLine))
                context.EmitLine(inputLine);
        }
    }
}
