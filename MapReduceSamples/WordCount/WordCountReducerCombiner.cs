using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace WordCount
{
    public class WordCountReducerCombiner : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            context.EmitKeyValue(
                key, 
                values
                    .Sum(v => int.Parse(v))
                    .ToString(CultureInfo.InvariantCulture)
                );
        }
    }
}
