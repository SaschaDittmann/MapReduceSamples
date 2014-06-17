using Microsoft.Hadoop.MapReduce;

namespace MedianStdDev
{
    public class MedianStdDevJob : HadoopJob<MedianStdDevMapper, MedianStdDevReducer>
    {
        public override HadoopJobConfiguration Configure(ExecutorContext context)
        {
            return new HadoopJobConfiguration
            {
                InputPath = "/samples/comments",
                OutputFolder = "output/MedianStdDev"
            };
        }
    }
}
