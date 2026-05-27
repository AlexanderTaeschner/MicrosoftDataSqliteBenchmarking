namespace InsertTest;

using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using BenchmarkDotNet.Running;

[Orderer(SummaryOrderPolicy.Method, MethodOrderPolicy.Alphabetical)]
public class InsertTest
{
    private const int NumberOfInserts = 10000;

    [Benchmark]
    public void Insert_Prepared_NoTransaction_MDS()
        => InsertMicrosoft(true, false);

    [Benchmark]
    public void Insert_Prepared_InTransaction_MDS()
        => InsertMicrosoft(true, true);

    [Benchmark]
    public void Insert_NotPrepared_NoTransaction_MDS()
        => InsertMicrosoft(false, false);

    [Benchmark]
    public void Insert_NotPrepared_InTransaction_MDS()
        => InsertMicrosoft(false, true);

    public static void InsertMicrosoft(bool prepare, bool useTransaction)
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
    public void Insert_Prepared_NoTransaction_SDS()
        => InsertSQLite(true, false);

    [Benchmark]
    public void Insert_Prepared_InTransaction_SDS()
        => InsertSQLite(true, true);

    [Benchmark]
    public void Insert_NotPrepared_NoTransaction_SDS()
        => InsertSQLite(false, false);

    [Benchmark]
    public void Insert_NotPrepared_InTransaction_SDS()
        => InsertSQLite(false, true);

    public static void InsertSQLite(bool prepare, bool useTransaction)
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
    public void Insert_Prepared_NoTransaction_DNS()
        => InsertDotNetSQLite(true, false);

    [Benchmark]
    public void Insert_Prepared_InTransaction_DNS()
        => InsertDotNetSQLite(true, true);

    [Benchmark]
    public void Insert_NotPrepared_NoTransaction_DNS()
        => InsertDotNetSQLite(false, false);

    [Benchmark]
    public void Insert_NotPrepared_InTransaction_DNS()
        => InsertDotNetSQLite(false, true);

    public static void InsertDotNetSQLite(bool prepare, bool useTransaction)
    {
        using var connection = SQLiteLibrary.SQLiteConnection.CreateTemporaryInMemoryDb();
        connection.ExecuteNonQuery("DROP TABLE IF EXISTS Numbers\0"u8);
        connection.ExecuteNonQuery("CREATE TABLE `Numbers` (Key INTEGER, Value REAL, PRIMARY KEY(Key))\0"u8);

        SQLiteLibrary.SQLiteStatement command = null;
        if (prepare)
        {
            command = connection.PrepareStatement("INSERT INTO Numbers VALUES (@Key, @Value)\0"u8);
        }

        if (useTransaction)
        {
            connection.ExecuteNonQuery("BEGIN TRANSACTION\0"u8);
        }

        for (var i = 0; i < NumberOfInserts; i++)
        {
            if (prepare)
            {
                command.BindParameter("@Key\0"u8, i);
                command.BindParameter("@Value\0"u8, i);
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
            connection.ExecuteNonQuery("COMMIT TRANSACTION\0"u8);
        }
    }

    [Benchmark]
    public void Insert_Prepared_NoTransaction_DuckDB()
        => InsertDuckDB(true, false);

    [Benchmark]
    public void Insert_Prepared_InTransaction_DuckDB()
        => InsertDuckDB(true, true);

    [Benchmark]
    public void Insert_NotPrepared_NoTransaction_DuckDB()
        => InsertDuckDB(false, false);

    [Benchmark]
    public void Insert_NotPrepared_InTransaction_DuckDB()
        => InsertDuckDB(false, true);

    public static void InsertDuckDB(bool prepare, bool useTransaction)
    {
        var connectionString = "Data Source=:memory:";

        using var connection = new DuckDB.NET.Data.DuckDBConnection(connectionString);
        connection.Open();
        var command = connection.CreateCommand();
        command.CommandText = "DROP TABLE IF EXISTS Numbers";
        command.ExecuteNonQuery();
        command.CommandText = "CREATE TABLE Numbers (Key INTEGER, Value REAL, PRIMARY KEY(Key));";
        command.ExecuteNonQuery();

        if (prepare)
        {
            command.CommandText = "INSERT INTO Numbers VALUES ($Key, $Value);";
            command.Prepare();
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter("Key", 0));
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter("Value", 0));
        }

        DuckDB.NET.Data.DuckDBTransaction txn = null;
        if (useTransaction)
        {
            txn = connection.BeginTransaction();
            command.Transaction = txn;
        }

        for (var i = 0; i < NumberOfInserts; i++)
        {
            if (prepare)
            {
                command.Parameters["Key"].Value = i;
                command.Parameters["Value"].Value = i;
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


public class Program
{
    static void Main()
    {
        BenchmarkRunner.Run<InsertTest>();
    }
}

