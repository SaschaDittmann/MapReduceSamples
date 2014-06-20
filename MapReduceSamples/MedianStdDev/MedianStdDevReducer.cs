using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace MedianStdDev
{
    public class MedianStdDevReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            float sum = 0;
            int count = 0;

            var commentLengths = new List<float>();

            // Iterate through all input values for this key
            foreach (var value in values.Select(float.Parse))
            {
                commentLengths.Add(value);
                sum += value;
                count++;
            }

            commentLengths.Sort((x, y) => x.CompareTo(y));

            double median;
            if (count % 2 == 0)
            {
                // if commentLengths is an even value, average middle two elements
                median = (commentLengths[Convert.ToInt32(count / 2 - 1)]
                    + commentLengths[Convert.ToInt32(count / 2)]) / 2.0f;
            } else {
                // else, set median to middle value
                median = commentLengths[Convert.ToInt32(count / 2)];
            }

            // calculate standard deviation
            var mean = sum / count;
            var sumOfSquares = commentLengths
                .Sum(commentLength => (commentLength - mean)*(commentLength - mean));
            var stdDev = Math.Sqrt(sumOfSquares / (count - 1));

            context.EmitKeyValue(
                key,
                String.Format("{0}\t{1}", median, stdDev));
        }
    }
}
