using System;

namespace ReactivePratice
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var groupByUntilSample = new GroupByUntilSample();
            groupByUntilSample.Start();

            Console.ReadKey();
        }
    }
}