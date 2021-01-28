using Microsoft.Extensions.Logging;
using Minio;
using Minio.DataModel;
using System;
using System.Collections.Generic;
using System.IO;
using System.Reactive.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace MinIODemo.Consoling.Common
{
    public class MinIOHelper: IDisposable
    {
        //private static readonly Logger Log = LogManager.GetLogger("MinioHelper");
        private readonly MinioClient _minioClient;


        /// <summary>
        /// 初始化客户端
        /// </summary>
        /// <param name="endpoint">端点</param>
        /// <param name="accessKey">账号</param>
        /// <param name="secretKey">私钥</param>
        public MinIOHelper(string endpoint, string accessKey, string secretKey)
        {
            _minioClient = new MinioClient(endpoint, accessKey, secretKey);
        }

        /// <summary>
        /// 判断Bucket桶是否存在
        /// </summary>
        /// <param name="bucketName">桶名称</param>
        /// <returns></returns>
        public async Task<bool> BucketExistsAsync(string bucketName)
        {
            try
            {
                return await _minioClient.BucketExistsAsync(bucketName);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 创建Bucket桶
        /// </summary>
        /// <param name="bucketName">桶名称</param>
        /// <returns></returns>
        public async Task<bool> MakeBucketAsync(string bucketName)
        {
            try
            {
                //如果存在则不创建，返回false
                if (await BucketExistsAsync(bucketName))
                {
                    return false;
                }
                await _minioClient.MakeBucketAsync(bucketName);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 获取所有Bucket桶
        /// </summary>
        /// <returns></returns>
        public async Task<ListAllMyBucketsResult> ListBucketsAsync()
        {
            try
            {
                return await _minioClient.ListBucketsAsync();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 删除Bucket桶
        /// </summary>
        /// <param name="bucketName">桶名称</param>
        /// <returns></returns>
        public async Task<bool> RemoveBucketAsync(string bucketName)
        {
            try
            {
                await _minioClient.RemoveBucketAsync(bucketName);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 返回指定bucketName桶中的对象集合
        /// </summary>
        /// <param name="bucketName">桶名</param>
        /// <param name="prefix">前缀字符串，列出名称以prefix为前缀的对象</param>
        /// <param name="recursive">如果为false，则模拟目录结构，其中返回的每个列表都是完整对象或对象键的一部分，直到第一个“ /”。所有具有相同前缀（直到第一个“ /”）的对象都将合并到一个条目中。默认为false</param>
        /// <returns></returns>
        public async Task<IObservable<Item>> ListObjectsAsync(string bucketName
            , string prefix
            , bool recursive = true)
        {
            try
            {
                return await Task.Run(() =>
                _minioClient.ListObjectsAsync(bucketName, prefix: prefix, recursive));
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 下载特定对象为流
        /// </summary>
        /// <param name="bucketName">桶名称</param>
        /// <param name="objectName">对象名称</param>
        /// <returns></returns>
        public async Task<Stream> GetObjectAsync(string bucketName, string objectName)
        {
            try
            {
                Stream outStream = null;
                await _minioClient.StatObjectAsync(bucketName, objectName);
                await _minioClient.GetObjectAsync(bucketName, objectName, (stream) =>
                 {
                     outStream = stream;
                 });
                return outStream;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 下载特定对象为文件
        /// </summary>
        /// <param name="bucketName">桶名称</param>
        /// <param name="objectName">对象名称</param>
        /// <param name="filePath">保存文件的路径</param>
        /// <returns></returns>
        public async Task GetObjectAsync(string bucketName, string objectName, string filePath)
        {
            try
            {
                await _minioClient.StatObjectAsync(bucketName, objectName);
                await _minioClient.GetObjectAsync(bucketName, objectName, filePath + "/" + objectName);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 将内容从流上传到objectName
        /// </summary>
        /// <param name="bucketName">桶名称</param>
        /// <param name="objectName">对象名称</param>
        /// <param name="filePath">文件路径+文件全名称</param>
        /// <param name="contentType">文件的内容类型。默认为“应用程序/八位字节流”</param>
        /// <returns></returns>
        public async Task<bool> PutObjectByStreamAsync(string bucketName
            , string objectName
            , string filePath
            , string contentType)
        {
            try
            {
                byte[] bytes = File.ReadAllBytes(filePath);
                MemoryStream stream = new MemoryStream(bytes);
                Aes aes = Aes.Create();
                aes.KeySize = 256;
                aes.GenerateKey();
                var ssec = new SSEC(aes.Key);
                await _minioClient.PutObjectAsync(bucketName, objectName, stream, stream.Length, contentType, null, ssec);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        /// <summary>
        /// 上传文件到特定的桶中作为对象
        /// </summary>
        /// <param name="bucketName"></param>
        /// <param name="objectName"></param>
        /// <param name="filepath"></param>
        /// <param name="contentType"></param>
        /// <returns></returns>
        public async Task<bool> PutObjectByFileAsync(string bucketName
            , string objectName
            , string filepath
            , string contentType)
        {
            try
            {
                await _minioClient.PutObjectAsync(bucketName, objectName, filepath, contentType);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 获取对象的元数据
        /// </summary>
        /// <param name="bucketName">桶名</param>
        /// <param name="objectName">存储桶中的对象名称</param>
        /// <returns></returns>
        public async Task<ObjectStat> StatObjectAsync(string bucketName, string objectName)
        {
            try
            {
                return await _minioClient.StatObjectAsync(bucketName, objectName);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        /// <summary>
        /// 为HTTP GET操作生成一个预签名的URL。即使存储桶是私有的，浏览器/移动客户端也可能指向该URL直接下载对象。该预签名URL可以具有关联的到期时间（以秒为单位），在此时间之后，它将不再起作用。默认有效期设置为7天
        /// </summary>
        /// <param name="bucketName">桶名</param>
        /// <param name="objectName">存储桶中的对象名称</param>
        /// <param name="expiresInt">到期秒数。默认有效期设置为7天</param>
        /// <returns></returns>
        public async Task<string> PresignedGetObjectAsync(string bucketName, string objectName, int expiresInt)
        {
            try
            {
                return await _minioClient.PresignedGetObjectAsync(bucketName, objectName, expiresInt);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Dispose()
        {
            
        }
    }
}