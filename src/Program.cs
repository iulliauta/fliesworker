using System;
using System.Data.Common;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Newtonsoft.Json;
using Npgsql;
using StackExchange.Redis;

namespace Worker
{
    public class Program
    {
	static string[] arrayZboruri1 = new string[] 
		{"Londra, BritishAirlines - 2 ore, fara escala, 90 EURO",
		 "Paris, AirFrance - 3 ore, fara escala, 100 EURO",
                 "Instanbul, TurkishAirlines - 1:30, fara escala, 80 EURO",
                 "Madrid, BlueAir - 4 ore, escala Paris, 220 EURO",
                 "Barcelona, WizzAir - 3 ore, fara escala, 200 EURO"};

	 static string[] arrayZboruri2 = new string[] 
		{"Roma, BlueAir - 3 ore, fara escala, 125 EURO",
                 "Milano, RyanAir - 3 ore, escala Paris, 220 EURO",
                 "New Delhi, RyanAir - 4:30, fara escala, 490 EURO",
                 "Bologna, BlueAir - 2 ore, escala Paris, 300 EURO",
                 "Venetia, AlItalia - 3 ore, fara escala, 420 EURO"};

	static string[] arrayZboruri3 = new string[] 
		{"Berlin, BritishAirlines - 4 ore, fara escala, 490 EURO",
		 "Atena, AirFrance - 3 ore, fara escala, 400 EURO",
                 "Bruxelles, Tarom - 2:30, fara escala, 280 EURO",
                 "Sicilia, BlueAir - 4 ore, escala Madrid, 420 EURO",
                 "Budapesta, WizzAir - 2 ore, fara escala, 100 EURO"};

	static string[] arrayZboruri4 = new string[] 
		{"Dublin, RyanAir - 4 ore, fara escala, 490 EURO",
		 "Paris, AirFrance - 3 ore, fara escala, 250 EURO",
                 "Barcelona, RyanAir - 1:30, fara escala, 180 EURO",
                 "Atena, Tarom - 3 ore, escala Paris, 420 EURO",
                 "Creta, WizzAir - 2 ore, fara escala, 80 EURO"};

        public static int Main(string[] args)
        {
            try
            {
                var pgsql = OpenDbConnection("Server=db;Username=postgres;");
                var redisConn = OpenRedisConnection("redis");
                var redis = redisConn.GetDatabase();

                // Keep alive is not implemented in Npgsql yet. This workaround was recommended:
                // https://github.com/npgsql/npgsql/issues/1214#issuecomment-235828359
                var keepAliveCommand = pgsql.CreateCommand();
                keepAliveCommand.CommandText = "SELECT 1";

                var definition = new { plecare = "", intoarcere = "", voter_id = "" };
                while (true)
                {
                    // Slow down to prevent CPU spike, only query each 100ms
                    Thread.Sleep(100);

                    // Reconnect redis if down
                    if (redisConn == null || !redisConn.IsConnected) {
                        Console.WriteLine("Reconnecting Redis");
                        redisConn = OpenRedisConnection("redis");
                        redis = redisConn.GetDatabase();
                    }
                    string json = redis.ListLeftPopAsync("entries").Result;
                    if (json != null)
                    {
                        var zbor = JsonConvert.DeserializeAnonymousType(json, definition);
                        Console.WriteLine($"Processing zbor for '{zbor.plecare} - {zbor.intoarcere}' by '{zbor.voter_id}'");
                        // Reconnect DB if down
                        if (!pgsql.State.Equals(System.Data.ConnectionState.Open))
                        {
                            Console.WriteLine("Reconnecting DB");
                            pgsql = OpenDbConnection("Server=db;Username=postgres;");
                        }
                        else
                        { 
                            UpdateVote(pgsql, zbor.voter_id, zbor.plecare, zbor.intoarcere);
                        }
                    }
                    else
                    {
                        keepAliveCommand.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.ToString());
                return 1;
            }
        }

        private static NpgsqlConnection OpenDbConnection(string connectionString)
        {
            NpgsqlConnection connection;

            while (true)
            {
                try
                {
                    connection = new NpgsqlConnection(connectionString);
                    connection.Open();
                    break;
                }
                catch (SocketException)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
                catch (DbException)
                {
                    Console.Error.WriteLine("Waiting for db");
                    Thread.Sleep(1000);
                }
            }

            Console.Error.WriteLine("Connected to db");

            var command = connection.CreateCommand();
            command.CommandText = @"CREATE TABLE IF NOT EXISTS zboruri (
					id VARCHAR(255),
                                        plecare VARCHAR(255),
                                        intoarcere VARCHAR(255),
                                        zbor VARCHAR(255)
                                    )";
            command.ExecuteNonQuery();

            return connection;
        }

        private static ConnectionMultiplexer OpenRedisConnection(string hostname)
        {
            // Use IP address to workaround https://github.com/StackExchange/StackExchange.Redis/issues/410
            var ipAddress = GetIp(hostname);
            Console.WriteLine($"Found redis at {ipAddress}");

            while (true)
            {
                try
                {
                    Console.Error.WriteLine("Connecting to redis");
                    return ConnectionMultiplexer.Connect(ipAddress);
                }
                catch (RedisConnectionException)
                {
                    Console.Error.WriteLine("Waiting for redis");
                    Thread.Sleep(1000);
                }
            }
        }

        private static string GetIp(string hostname)
            => Dns.GetHostEntryAsync(hostname)
                .Result
                .AddressList
                .First(a => a.AddressFamily == AddressFamily.InterNetwork)
                .ToString();

        private static void UpdateVote(NpgsqlConnection connection, string id, string plecare, string intoarcere)
        {
            var command = connection.CreateCommand();
	        Random ran = new Random();
            string[] randomZbor = new string[4];
			string[] newArr;
            int index1 = ran.Next(0, arrayZboruri1.Length - 1);
            randomZbor[0] = arrayZboruri1[index1];

	    int index2 = ran.Next(0, arrayZboruri2.Length - 1);
	    randomZbor[1] = arrayZboruri2[index2];
                           
	    int index3 = ran.Next(0, arrayZboruri3.Length - 1);
        randomZbor[2] = arrayZboruri3[index3];
           

	    int index4 = ran.Next(0, arrayZboruri4.Length - 1);
        randomZbor[3] = arrayZboruri4[index4];
                         
            try
            {
               int i;
               for(i = 0; i < 4; i++) {
               		command.CommandText = "INSERT INTO zboruri (id, plecare, intoarcere, zbor) VALUES (@id, @plecare, @intoarcere, @zbor)";
			command.Parameters.AddWithValue("@id", id);
                	command.Parameters.AddWithValue("@plecare", plecare);
                	command.Parameters.AddWithValue("@intoarcere", intoarcere);
			command.Parameters.AddWithValue("@zbor", randomZbor[i]);
                	command.ExecuteNonQuery();
		}
            }
            catch (DbException)
            {
                command.CommandText = "UPDATE votes SET zbor = @zbor WHERE id = @id";
                command.ExecuteNonQuery();
            }
            finally
            {
                command.Dispose();
            }
        }
    }
}
