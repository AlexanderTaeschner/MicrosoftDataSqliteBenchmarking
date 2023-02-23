namespace InsertTest;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;

public class InsertTest
{
    private const int NumberOfInserts = 10000;

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

    public static void InsertSpeedMicrosoft(bool prepare, bool useTransaction)
    {
        var connectionString = "Data Source=:memory:";

        using var connection = new Microsoft.Data.Sqlite.SqliteConnection(connectionString);
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

        for (var i = 0; i < NumberOfInserts; i++)
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

    public static void InsertSpeedSQLite(bool prepare, bool useTransaction)
    {
        var connectionString = "Data Source=:memory:";

        using var connection = new System.Data.SQLite.SQLiteConnection(connectionString);
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

        for (var i = 0; i < NumberOfInserts; i++)
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

    [Benchmark]
    public void InsertSpeed_DNS_Prepared_NoTransaction()
        => InsertSpeedDotNetSQLite(true, false);

    [Benchmark]
    public void InsertSpeed_DNS_Prepared_InTransaction()
        => InsertSpeedDotNetSQLite(true, true);

    [Benchmark]
    public void InsertSpeed_DNS_NotPrepared_NoTransaction()
        => InsertSpeedDotNetSQLite(false, false);

    [Benchmark]
    public void InsertSpeed_DNS_NotPrepared_InTransaction()
        => InsertSpeedDotNetSQLite(false, true);

    public static void InsertSpeedDotNetSQLite(bool prepare, bool useTransaction)
    {
        using var connection = SQLiteLibrary.SQLiteConnection.CreateTemporaryInMemoryDb();
        connection.ExecuteNonQuery("DROP TABLE IF EXISTS Numbers"u8);
        connection.ExecuteNonQuery("CREATE TABLE `Numbers` (Key INTEGER, Value REAL, PRIMARY KEY(Key));"u8);

        SQLiteLibrary.SQLiteStatement command = null;
        if (prepare)
        {
            command = connection.PrepareStatement("INSERT INTO Numbers VALUES (@Key, @Value);"u8);
        }

        if (useTransaction)
        {
            connection.ExecuteNonQuery("BEGIN TRANSACTION"u8);
        }

        for (var i = 0; i < NumberOfInserts; i++)
        {
            if (prepare)
            {
                command.BindParameter("@Key"u8, i);
                command.BindParameter("@Value"u8, i);
                command.DoneStep();
                command.Reset();
            }
            else
            {
#pragma warning disable DNSQLL001 // Type or member is obsolete
                connection.ExecuteNonQuery($"INSERT INTO Numbers VALUES ({i}, {i});");
#pragma warning restore DNSQLL001 // Type or member is obsolete
            }
        }

        if (useTransaction)
        {
            connection.ExecuteNonQuery("COMMIT TRANSACTION"u8);
        }
    }
}

public class Program
{
    static void Main()
    {
        BenchmarkRunner.Run<InsertTest>();
    }
}

