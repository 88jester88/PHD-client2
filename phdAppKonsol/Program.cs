using Newtonsoft.Json;
using System.IO;
using LumenWorks.Framework.IO.Csv;
using System;
using System.Collections;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using Uniformance.PHD;
using Confluent.Kafka;
using Confluent.Kafka.Admin;



namespace phdAppKonsol
{




    //internal class Program
    static class Program
    {



        static async Task Main(string[] args)
        {


            string brokerList = "phd-kafka-kafka.apps.iris.local:9091";
        string topicName = "my-topic-phd";

        var config = new ProducerConfig { BootstrapServers = brokerList };





        /*string value1 = null;
        string value2 = null;
        string value3 = null;
        string value4 = null ;
        string value5 = null;*/
        //string brokerList = "";
        //string topicName = "";
        //string[] names = new string[44822];
        string[] names = new string[3];
            string json = null;
            //string json_add = null;
            /*
            if (args.Length == 0)
            {
                Console.WriteLine("Invalid args");
                return;
            }

            var command = args[0];

            switch (command)
            {
                case "push":
                    //int valuex = 0;
                    break;
                case "get" when args.Length == 11 && args[1] == "-sn" && args[3] == "-sl" && args[5] == "-sd" && args[7] == "-ed" && args[9] == "-what":
                    value1 = args[2];
                    value2 = args[4];
                    value3 = args[6];
                    value4 = args[8];
                    value5 = args[10];
                    break;
                default:
                    Console.WriteLine("Invalid command");
                    break;
            }
            */
            
            var csvTable = new DataTable();
            using (var csvReader = new CsvReader(new StreamReader(System.IO.File.OpenRead("src\t2\this.csv")), true))
            {
                csvTable.Load(csvReader);
                
            }

            string Column1 = csvTable.Columns[0].ToString();
            for (int j = 1; j <= 3; j++)
            {
                string Row1 = csvTable.Rows[j][0].ToString();
                //Console.WriteLine(Row1);
                names[j-1] = Row1;
            }
          


            long milliseconds1 = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            //Console.WriteLine(names[2]);
            //Console.WriteLine(Column1);
             Console.WriteLine("PHD Server Connection");
             PHDServer phdSrv = new PHDServer();
             PHDHistorian pdh = new PHDHistorian();
             phdSrv.HostName = "10.170.90.21";
             phdSrv.Port = 3100;
             phdSrv.APIVersion = SERVERVERSION.API200;
             pdh.DefaultServer = phdSrv;

             pdh.Sampletype = SAMPLETYPE.Raw;
             pdh.ReductionType = REDUCTIONTYPE.None;
             pdh.StartTime = "NOW-1D";
             pdh.EndTime = "NOW";
             Console.WriteLine("END");

             Console.WriteLine("PHD Browse Tags");
                
            //var allTags = pdh.BrowsingTags(100000, null);

            //System.IO.File.AppendAllText(@"C:\Users\DA00625\Desktop\devicesecond.txt", allTags.GetXml());

            //dynamic jsonObject = new JObject();
            //string json_tag_p = JsonConvert.SerializeObject(allTags, Formatting.Indented);
            //var jj = jObject.Parse(json_tag_p);
            //var id = jj["TagData"]["TagName"].ToString();
            //Console.WriteLine(id);






            // Tag tag1 = new Tag(".PV");
            //var allRdi = pdh.GetRDIs();
            //ArrayList parentTags = pdh.GetParentTagList();
            //TagFilter tagFilt = new TagFilter();
            //tagFilt.ParentTagname = ((ParentTaglistStruct)parentTags[5]).Tagname;
            // var allTags0 = pdh.BrowsingTags(5, tagFilt);
            //var tagdfn = pdh.TagDfn(".PV");
            //Console.WriteLine(allTags.GetXml());

            //var tagdfn = pdh.TagDfn("100TI0385.PV");
            
            int last = 0;
            for (int a = 0; a <= 2; a++)
            {
               // Console.WriteLine(a);
                var allValues = pdh.FetchRowData(names[a]);
               // var tagdfn = pdh.TagDfn(names[a]);
                allValues.Tables[0].Columns.RemoveAt(8);
                allValues.Tables[0].Columns.RemoveAt(7);
                allValues.Tables[0].Columns.RemoveAt(6);
                allValues.Tables[0].Columns.RemoveAt(5);
                allValues.Tables[0].Columns.RemoveAt(4);
                //string json = JsonConvert.SerializeObject(allValues, Formatting.Indented);
                //Console.WriteLine(json);
                json = JsonConvert.SerializeObject(allValues, Formatting.Indented);
                //System.IO.File.AppendAllText(@"C:\Users\DA00625\Desktop\thisis.txt", json);
                //json_add = json_add.Replace("]", ",") +json.Replace("TagData: [" , "\n");
                //System.IO.File.AppendAllText(@"C:\Users\DA00625\Desktop\devices.txt");
                //Console.WriteLine(names[a]);


                using (var p = new ProducerBuilder<Null, string>(config).Build())
                {
                    try
                    {
                        var dr = await p.ProduceAsync(topicName, new Message<Null, string> { Value = json });
                        Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                }
            
        


        last = a;
            }
            
            long milliseconds2 = DateTimeOffset.Now.ToUnixTimeMilliseconds();
            long time = milliseconds2 - milliseconds1;
            Console.Write("Finished, Time Elapsed :");
            System.IO.File.AppendAllText("src\t2\thisis.txt", time.ToString());
            Console.WriteLine(time);
            Console.Write("Last value number:");
            Console.WriteLine(last);
            

            /*
             var allValues = pdh.FetchRowData("445FI0002.PV");
             allValues.Tables[0].Columns.RemoveAt(8);
             allValues.Tables[0].Columns.RemoveAt(7);
             allValues.Tables[0].Columns.RemoveAt(6);
             allValues.Tables[0].Columns.RemoveAt(5);
             allValues.Tables[0].Columns.RemoveAt(4);
             Console.WriteLine(allValues);
             json = JsonConvert.SerializeObject(allValues, Formatting.Indented);
             

            */















            //var tagdfn = pdh.TagDfn("100TI0385.PV");
            /*allValues.Tables[0].Columns.RemoveAt(8);
            allValues.Tables[0].Columns.RemoveAt(7);
            allValues.Tables[0].Columns.RemoveAt(6);
            allValues.Tables[0].Columns.RemoveAt(5);
            allValues.Tables[0].Columns.RemoveAt(4);
           // Console.WriteLine("END");
            //Console.WriteLine("values:");
            /*if (value5 != "l")
            {
                Console.WriteLine(value1);
                Console.WriteLine(value2);
                Console.WriteLine(value3);
                Console.WriteLine(value4);
                string json = JsonConvert.SerializeObject(allValues, Formatting.Indented);
                //Console.WriteLine(json);
            }
            if(value5 == "l")
            {
                //Console.WriteLine(tagFilt.GetXml());
            }
            */
            pdh.Dispose();
             
            Console.ReadLine();
            
        }

    }
}


