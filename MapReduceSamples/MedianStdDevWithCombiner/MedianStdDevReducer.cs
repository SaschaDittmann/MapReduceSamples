using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.Json;

namespace MedianStdDevWithCombiner
{
    public class MedianStdDevReducer : JsonInReducerCombinerBase<MedianStdDevData>
    {
        public override void Reduce(string key, IEnumerable<MedianStdDevData> values, ReducerCombinerContext context)
        {
            float sum = 0;
            long totalComments = 0;
            
            var commentLengthCounts = new Dictionary<int, long>();
            foreach (var data in values)
            {
                totalComments += data.Count;
                sum += data.Value*data.Count;
                if (!commentLengthCounts.ContainsKey(data.Value))
                    commentLengthCounts.Add(data.Value, data.Count);
                else
                    commentLengthCounts[data.Value] += data.Count;
            }

            // calculate median
            double median = 0;
            var medianIndex = totalComments / 2;
            long previousComments = 0;
            var prevKey = 0;
            foreach (var entry in commentLengthCounts.OrderBy(e => e.Key))
            {
                if (previousComments <= medianIndex && medianIndex < previousComments + entry.Value)
                {
                    if (totalComments % 2 == 0 && previousComments == medianIndex)
                        median = (entry.Key + prevKey)/2.0f;
                    else
                        median = entry.Key;
                    break;
                }
                previousComments += entry.Value;
                prevKey = entry.Key;
            }

            // calculate standard deviation
            var avg = sum / totalComments;
            var sumOfSquares = commentLengthCounts
                .Sum(entry => Math.Pow(entry.Key - avg, 2) * entry.Value);
            var stdDev = Math.Sqrt(sumOfSquares / (totalComments - 1));

            context.EmitKeyValue(
                key,
                String.Format("{0}\t{1}", median, stdDev));
        }
    }
}
