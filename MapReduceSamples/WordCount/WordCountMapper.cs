using System.Text.RegularExpressions;
using Microsoft.Hadoop.MapReduce;

namespace WordCount
{
    public class WordCountMapper : MapperBase
    {
        private static readonly Regex Regex = new Regex("[a-zA-Z]+");

        public override void Map(string inputLine, MapperContext context)
        {
            foreach (var match in Regex.Matches(inputLine))
                context.EmitKeyValue(match.ToString(), "1");
        }
    }
}
