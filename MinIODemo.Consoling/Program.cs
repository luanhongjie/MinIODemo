using Minio;
using Minio.DataModel;
using MinIODemo.Consoling.Common;
using System;
using System.Threading.Tasks;

namespace MinIODemo.Consoling
{
    class Program
    {
        //private static MinioClient minio = new MinioClient("127.0.0.1:9000", "minioadmin", "minioadmin");//.WithSSL()   //不要带http://   
        static async Task Main(string[] args)
        {
            //    // Create an async task for listing buckets.
            //    var getListBucketsTask = minio.ListBucketsAsync();
            //    foreach (Bucket bucket in getListBucketsTask.Result.Buckets)
            //    {
            //        Console.WriteLine($"{bucket.Name}  {bucket.CreationDateDateTime}");
            //    }


            MinIOHelper minIOHelper = new MinIOHelper("127.0.0.1:9000", "minioadmin", "minioadmin");
           bool b=  await minIOHelper.BucketExistsAsync("picture");
            Console.WriteLine(b);
        }
    }
}
