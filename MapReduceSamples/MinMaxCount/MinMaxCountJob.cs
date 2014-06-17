using Microsoft.Hadoop.MapReduce;

namespace MinMaxCount
{
    public class MinMaxCountJob : HadoopJob<MinMaxCountMapper, MinMaxCountReducerCombiner, MinMaxCountReducerCombiner>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            return new HadoopJobConfiguration
            {
                InputPath = "/samples/comments",
                OutputFolder = "output/MinMaxCount"
            };
        }
    }
}
