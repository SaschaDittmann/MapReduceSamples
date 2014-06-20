using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Average
{
    public class CountAverageData
    {
        public float Average { get; set; }
        public float Count { get; set; }

        public override string ToString()
        {
            return String.Format("{0}\t{1}", Average, Count);
        }
    }
}
