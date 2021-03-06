namespace OracleReaderModule
{
    using System;
    using System.IO;
    using System.Runtime.InteropServices;
    using System.Runtime.Loader;
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Azure.Devices.Shared;
    using Newtonsoft.Json;
    using Helper;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;

    class Program
    {
        static int counter;

        #region Oracle Reader Properties
        static string ConnectionString { get; set; } = "";
        static string SqlQuery { get; set; } = "";
        static bool IsSqlQueryJson { get; set; } = false;
        static int PoolingIntervalMiliseconds { get; set; } = 6000;
        static int MaxBatchSize { get; set; }  = 200;
        static bool Verbose { get; set; } = true;
        
        static PoolingHelper sqlPooling;
        #endregion

        static ModuleClient ioTHubModuleClient;
        static void Main(string[] args)
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            MqttTransportSettings mqttSetting = new MqttTransportSettings(TransportType.Mqtt_Tcp_Only);
            ITransportSettings[] settings = { mqttSetting };

            // Open a connection to the Edge runtime
            ioTHubModuleClient = await ModuleClient.CreateFromEnvironmentAsync(settings);
            await ioTHubModuleClient.OpenAsync();
            Logger.Writer.LogInformation("IoT Hub module client initialized.");

            var moduleTwin = await ioTHubModuleClient.GetTwinAsync();
            await OnDesiredPropertiesUpdate(moduleTwin.Properties.Desired, ioTHubModuleClient);
            await ioTHubModuleClient.SetDesiredPropertyUpdateCallbackAsync(OnDesiredPropertiesUpdate, null);

            // Register callback to be called when a message is received by the module
            await ioTHubModuleClient.SetInputMessageHandlerAsync("input1", PipeMessage, ioTHubModuleClient);

            try
            {
                //starts the execution
                Logger.Writer.LogInformation($"Creating object to long polling...");

                if (sqlPooling != null)
                {
                    sqlPooling.Stop();
                    sqlPooling = null;
                }

                sqlPooling = new PoolingHelper(ioTHubModuleClient,
                Settings.Current.ConnectionString, Settings.Current.SqlQuery, Settings.Current.IsSqlQueryJson,
                Settings.Current.PoolingIntervalMiliseconds, Settings.Current.MaxBatchSize, Settings.Current.Verbose);
                sqlPooling.Run();
            }
            catch (Exception ex)
            {
                Logger.Writer.LogError(ex, $"Error ocurrer on Init: {ex.ToString()}");
            }


        }

        static Task OnDesiredPropertiesUpdate(TwinCollection desiredProperties, object userContext)
        {
            try
            {
                Logger.Writer.LogInformation("Desired property change detected:");
                Logger.Writer.LogInformation(JsonConvert.SerializeObject(desiredProperties));

                if (desiredProperties["ConnectionString"] != null)
                    ConnectionString = desiredProperties["ConnectionString"];

                if (desiredProperties["SqlQuery"] != null)
                    SqlQuery = desiredProperties["SqlQuery"];

                if (desiredProperties["IsSqlQueryJson"] != null)
                    IsSqlQueryJson = desiredProperties["IsSqlQueryJson"];

                if (desiredProperties["PoolingIntervalMiliseconds"] != null)
                    PoolingIntervalMiliseconds = desiredProperties["PoolingIntervalMiliseconds"];

                if (desiredProperties["MaxBatchSize"] != null)
                    MaxBatchSize = desiredProperties["MaxBatchSize"];

                if (desiredProperties["Verbose"] != null)
                    Verbose = desiredProperties["Verbose"];
            
            }
            catch (AggregateException ex)
            {
                foreach (Exception exception in ex.InnerExceptions)
                {
                    Logger.Writer.LogError(exception, "Error when receiving desired property: {0}");
                }
            }
            catch (Exception ex)
            {
                Logger.Writer.LogError(ex, "Error when receiving desired property: {0}", ex.Message);
            }
            return Task.CompletedTask;
        }

        

        /// <summary>
        /// This method is called whenever the module is sent a message from the EdgeHub. 
        /// It just pipe the messages without any change.
        /// It prints all the incoming messages.
        /// </summary>
        static async Task<MessageResponse> PipeMessage(Message message, object userContext)
        {
            int counterValue = Interlocked.Increment(ref counter);

            var moduleClient = userContext as ModuleClient;
            if (moduleClient == null)
            {
                throw new InvalidOperationException("UserContext doesn't contain " + "expected values");
            }

            byte[] messageBytes = message.GetBytes();
            string messageString = Encoding.UTF8.GetString(messageBytes);
            Console.WriteLine($"Received message: {counterValue}, Body: [{messageString}]");

            if (!string.IsNullOrEmpty(messageString))
            {
                using (var pipeMessage = new Message(messageBytes))
                {
                    foreach (var prop in message.Properties)
                    {
                        pipeMessage.Properties.Add(prop.Key, prop.Value);
                    }
                    await moduleClient.SendEventAsync("output1", pipeMessage);

                    Console.WriteLine("Received message sent");
                }
            }
            return MessageResponse.Completed;
        }
    }
}
