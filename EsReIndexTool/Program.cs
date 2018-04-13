using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EsReIndexTool {

    internal class Program {

        private static void Main(string[] args) {
            //url
            var fromeUri = new Uri("http://192.168.0.151:9224");
            var toUri = new Uri("http://localhost:9400");
            //index
            var fromIndex = "chl";
            var toIndex = "chllp";
            //type
            var fromType = "chl";
            var toType = "chllp";

            int size = 100;
            string scrollTime = "1h";
            //建立两个client
            var settings = new ConnectionSettings(
                fromeUri,
                defaultIndex: fromIndex
            );
            var toSettings = new ConnectionSettings(
              toUri,
              defaultIndex: toIndex
            );

            var fromClient = new ElasticClient(settings);
            var toClient = new ElasticClient(toSettings);

            List<string> FieldNameList = new List<string>();
            //获取字段列表
            try {
                List<TypeMapping> tpMaps = new List<TypeMapping>();
                var mapRequest = new GetMappingRequest(toIndex, toType);
                var Indexmap = toClient.GetMapping(mapRequest);
                var mapResult = Indexmap as GetMappingResponse;
                tpMaps = mapResult.Mappings[toType] as List<TypeMapping>;
                foreach (var mp in tpMaps) {
                    foreach (var field in mp.Mapping.Properties) {
                        FieldNameList.Add(field.Value.Name.Name);
                    }
                    break;
                }
            }
            catch (Exception e) {
                Console.Write(e.Message + ":" + e.ToString());
            }
            string[] FieldNames = FieldNameList.ToArray(); //包含CheckFullText
            Console.WriteLine(string.Format("字段列表获取完毕，共提取{0}个字段\r\n", FieldNames.Length));

            //scroll查询
            var scanResults = fromClient.Search<object>(s => s
             .Type(fromType)
             .From(0)
             .Take(size)
             .Scroll(scrollTime)
             .SearchType(Elasticsearch.Net.SearchType.Scan)
             .MatchAll()
            );
            int id = 1;
            string scrollId = scanResults.ScrollId;
            bool isScrollSetHasData = true;
            string gid = "";

            #region 迭代取数据，然后索引到新的索引

            while (isScrollSetHasData) {
                scanResults = fromClient.Scroll<object>(scrollTime, scrollId);
                isScrollSetHasData = false;// scanResults.Documents.Any();

                scrollId = scanResults.ScrollId;
                if (scanResults.IsValid) {
                    foreach (var item in scanResults.Hits) {
                        dynamic setValue = item.Source;
                        Dictionary<string, object> resDictionary = new Dictionary<string, object>();

                        try {
                            gid = setValue["Gid"].ToString();
                        }
                        catch (Exception ex) {
                            string logInfoAjax = "插入数据失败，错误码为" + ex;
                            Console.WriteLine(logInfoAjax);
                            continue;
                        }

                        for (int i = 0; i < FieldNames.Length; i++) {
                            var field = FieldNames[i];
                            var value = setValue[field];
                            resDictionary.Add(field, value ?? null);
                        }

                        string json = JsonConvert.SerializeObject(resDictionary);
                        try {
                            var indexRequest = new IndexRequest<object>(json) {
                                Index = toIndex,
                                Type = toType,
                                Refresh = true,
                                Id = gid
                            };
                            id++;
                            var n = toClient.Index<object>(indexRequest);
                            Console.WriteLine(n.Id + "+" + n.Index);
                        }
                        catch (Exception ex) {
                            string logInfoAjax = "插入数据失败，错误码原因为" + ex.ToString();
                            Console.WriteLine(logInfoAjax);
                            throw;
                        }
                    }
                }
            }

            #endregion 迭代取数据，然后索引到新的索引

            try {
                //清理scroll
                fromClient.ClearScroll(scrollId);
            }
            catch (Exception e) {
            }

            Console.WriteLine("这次添加了：" + id + "条数据");
        }
    }
}