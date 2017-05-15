namespace InsertTest
{
    using BenchmarkDotNet.Attributes;
    using BenchmarkDotNet.Running;

    public class InsertTest
    {
        [Benchmark]
        public void InsertSpeed_MDS_Prepared_NoTransaction()
            => InsertSpeedMicrosoft(true, false);

        [Benchmark]
        public void InsertSpeed_MDS_Prepared_InTransaction()
            => InsertSpeedMicrosoft(true, true);

        [Benchmark]
        public void InsertSpeed_MDS_NotPrepared_NoTransaction()
            => InsertSpeedMicrosoft(false, false);

        [Benchmark]
        public void InsertSpeed_MDS_NotPrepared_InTransaction()
            => InsertSpeedMicrosoft(false, true);

        public void InsertSpeedMicrosoft(bool prepare, bool useTransaction)
        {
            var connectionString = "Data Source=:memory:";

            using (var connection = new Microsoft.Data.Sqlite.SqliteConnection(connectionString))
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

                Microsoft.Data.Sqlite.SqliteTransaction txn = null;
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

        [Benchmark]
        public void InsertSpeed_SDS_Prepared_NoTransaction()
            => InsertSpeedSQLite(true, false);

        [Benchmark]
        public void InsertSpeed_SDS_Prepared_InTransaction()
            => InsertSpeedSQLite(true, true);

        [Benchmark]
        public void InsertSpeed_SDS_NotPrepared_NoTransaction()
            => InsertSpeedSQLite(false, false);

        [Benchmark]
        public void InsertSpeed_SDS_NotPrepared_InTransaction()
            => InsertSpeedSQLite(false, true);

        public void InsertSpeedSQLite(bool prepare, bool useTransaction)
        {
            var connectionString = "Data Source=:memory:";

            using (var connection = new System.Data.SQLite.SQLiteConnection(connectionString))
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

                System.Data.SQLite.SQLiteTransaction txn = null;
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

