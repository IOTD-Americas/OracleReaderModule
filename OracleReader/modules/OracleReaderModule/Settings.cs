namespace OracleReaderModule
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.Contracts;
    using System.IO;
    using System.Linq;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Helper;
    using Microsoft.Azure.Devices.Shared;

    internal class Settings
    {
        public static Settings Current = Create();

        private Settings(string ConnectionString, string SqlQuery, bool IsSqlQueryJson, int PoolingIntervalMiliseconds, int MaxBatchSize, bool Verbose)
        {
            if (!string.IsNullOrEmpty(ConnectionString))
                this.ConnectionString = ConnectionString;
            else
                throw new ArgumentException("ConnectionString was not provided");


            if (!string.IsNullOrEmpty(SqlQuery))
                this.SqlQuery = SqlQuery;
            else
                throw new ArgumentException("SqlQuery was not provided");

            this.IsSqlQueryJson = IsSqlQueryJson;
            this.PoolingIntervalMiliseconds = PoolingIntervalMiliseconds > 0 ? PoolingIntervalMiliseconds : 60000;
            this.MaxBatchSize = MaxBatchSize > 0 ? MaxBatchSize : 60000;
            this.Verbose = Verbose;
        }

        public static bool RebuildFromTwinModule(TwinCollection desiredProperties)
        {
            Settings settings = null;
            try
            {
                string ConnectionString = string.Empty;
                string SqlQuery = string.Empty;
                bool IsSqlQueryJson = false;
                int PoolingIntervalMiliseconds = 6000;
                int MaxBatchSize = 200;
                bool Verbose = false;

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
            
                settings = new Settings(ConnectionString, SqlQuery, IsSqlQueryJson, PoolingIntervalMiliseconds, MaxBatchSize, Verbose);
                if (settings != null)
                {
                    Logger.Writer.LogInformation("Correctly reading settings from Twin Module");
                    Settings.Current = settings;
                }
                return true;
            }
            catch (ArgumentException e)
            {
                Logger.Writer.LogCritical("Error reading arguments from TwinCollection vaiables.");
                Logger.Writer.LogCritical(e.ToString());
                settings = null;
                return false;
            }
        }

        private static Settings Create()
        {
            try
            {
                IConfiguration configuration = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("settings.json", true)
                    .AddEnvironmentVariables()
                    .Build();

                return new Settings(
                    configuration.GetValue<string>("ConnectionString"),
                    configuration.GetValue<string>("SqlQuery"),
                    configuration.GetValue<bool>("IsSqlQueryJson", false),
                    configuration.GetValue<int>("PoolingIntervalMiliseconds", 60000),
                    configuration.GetValue<int>("MaxBatchSize", 200),
                    configuration.GetValue<bool>("Verbose", true)
                    );
            }
            catch (ArgumentException e)
            {
                Logger.Writer.LogCritical("Error reading arguments from environment variables. Make sure all required parameter are present");
                Logger.Writer.LogCritical(e.ToString());
                Environment.Exit(2);
                throw new Exception();  // to make code analyzers happy (this line will never run)
            }
        }


        public string ConnectionString { get; }
        public string SqlQuery { get; }
        public bool IsSqlQueryJson { get; }
        public int PoolingIntervalMiliseconds { get; }
        public int MaxBatchSize { get; }
        public bool Verbose { get; }



        // TODO: is this used anywhere important? Make sure to test it if so
        public override string ToString()
        {
            string HostName = Environment.GetEnvironmentVariable("IOTEDGE_GATEWAYHOSTNAME");
            Console.WriteLine($"IOTEDGE_GATEWAYHOSTNAME: {HostName}");

            var fields = new Dictionary<string, string>()
            {
                { nameof(this.ConnectionString), this.ConnectionString },
                { nameof(this.SqlQuery), this.SqlQuery },
                { nameof(this.PoolingIntervalMiliseconds), this.PoolingIntervalMiliseconds.ToString() },
                { nameof(this.MaxBatchSize), this.MaxBatchSize.ToString() },
                { nameof(this.IsSqlQueryJson), this.IsSqlQueryJson.ToString() },
                { nameof(this.Verbose), this.Verbose.ToString() }


            };

            return $"Settings:{Environment.NewLine}{string.Join(Environment.NewLine, fields.Select(f => $"{f.Key}={f.Value}"))}";
        }
    }
}
