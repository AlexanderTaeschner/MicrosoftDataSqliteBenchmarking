namespace InsertTest
{
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Running;
    using Microsoft.Data.Sqlite;

    public class InsertTest
    {
        [Benchmark]
        public void InsertSpeed_Prepared_NoTransaction()
            => InsertSpeed(true, false);

        [Benchmark]
        public void InsertSpeed_Prepared_InTransaction()
            => InsertSpeed(true, true);

        [Benchmark]
        public void InsertSpeed_NotPrepared_NoTransaction()
            => InsertSpeed(false, false);

        [Benchmark]
        public void InsertSpeed_NotPrepared_InTransaction()
            => InsertSpeed(false, true);

        public void InsertSpeed(bool prepare, bool useTransaction)
        {
            var connectionString = "Data Source=:memory:";

            using (var connection = new SqliteConnection(connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "DROP TABLE IF EXISTS Numbers";
                command.ExecuteNonQuery();
                command.CommandText = "CREATE TABLE `Numbers` (Key INTEGER, Value REAL, PRIMARY KEY(Key));";
                command.ExecuteNonQuery();

                if (prepare)
                {
                    command.CommandText = "INSERT INTO Numbers VALUES (@Key, @Value);";
                    command.Prepare();
                    command.Parameters.AddWithValue("@Key", 0);
                    command.Parameters.AddWithValue("@Value", 0);
                }

                SqliteTransaction txn = null;
                if (useTransaction)
                {
                    txn = connection.BeginTransaction();
                    command.Transaction = txn;
                }

                for (var i = 0; i < 100000; i++)
                {
                    if (prepare)
                    {
                        command.Parameters["@Key"].Value = i;
                        command.Parameters["@Value"].Value = i;
                    }
                    else
                    {
                        command.CommandText = $"INSERT INTO Numbers VALUES ({i}, {i});";
                    }

                    command.ExecuteNonQuery();
                }

                if (useTransaction)
                {
                    txn.Commit();
                }
            }
        }

    }

    public class Program
    {
        static void Main(string[] args)
        {
            var summary = BenchmarkRunner.Run<InsertTest>();
        }
    }
}

