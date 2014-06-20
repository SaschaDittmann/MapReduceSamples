using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MinMaxCount
{
    public class MinMaxCountData
    {
        public DateTime Min { get; set; }
        public DateTime Max { get; set; }
        public long Count { get; set; }

        public override string ToString()
        {
            return String.Format("{0}\t{1}\t{2}", Min, Max, Count);
        }
    }
}
