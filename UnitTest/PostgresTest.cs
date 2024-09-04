using CodeM.Common.DbHelper;
using NUnit.Framework;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace UnitTest
{
    /// <summary>
    /// PostgreSQL数据库测试
    /// </summary>
    public class PostgresTest
    {
        public PostgresTest()
        {
            DbUtils.RegisterDbProvider("postgres", "Npgsql.NpgsqlFactory, Npgsql");
            DbUtils.AddDataSource("postgres_test", "postgres", "Host=111.198.29.34:5444;Database=mxpt_business_databases;User Id=postgres;Password=Mxpt@2024#1;");
        }

        [Test]
        public void Order1_CreateTable()
        {
            string sql = "Create Table test(id int not null primary key, name varchar(64), age int, address varchar(255))";
            int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
            Assert.IsTrue(result == 0);
            string comment = "COMMENT ON COLUMN test.name IS '名称'";
            result = DbUtils.ExecuteNonQuery("postgres_test", comment);
            Assert.IsTrue(result == 0);
        }

        [Test]
        public void Order2_InsertRecord()
        {
            

            //多线程并发调用sql查询
            int threadCount = 5;
            var tasks = new Task[threadCount];
            for (int index = 0; index < threadCount; index++)
            {
                int i = index;
                tasks[index] = Task.Run(() =>
                {

                    string sql = String.Format("Insert Into test (id, name, age, address) Values({0}, 'wangxm', 18, '河北保定')", i + 1);
                    int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
                    Assert.IsTrue(result == 1);

                });

            }
            // 等待所有任务完成
            Task.WhenAll(tasks).Wait();
        }

        [Test]
        public void Order3_InsertRecord2()
        {
            try
            {
                string sql = "Insert Into test (id, name, age, address) Values(1, 'wangxm', 18, '河北保定')";
                int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
                Assert.Fail("Insert Error.");
            }
            catch (Exception e)
            {
                Assert.IsTrue(e.Message.Contains("重复"));
            }
        }

        [Test]
        public void Order4_QueryRecord()
        {
            //多线程并发调用sql查询
            int threadCount = 5;
            var tasks = new Task[threadCount];
            for (int index = 0; index < threadCount; index++)
            {
                int i = index;
                tasks[index] = Task.Run(() =>
                {

                    string sql = "Select * From test Where id=1";
                    DbDataReader dr = DbUtils.ExecuteDataReader("postgres_test", sql);
                    Assert.IsTrue(dr.HasRows);
                    Assert.IsTrue(dr.Read());
                    string name = dr.GetString(1);
                    Assert.AreEqual(name, "wangxm");
                    dr.Close();

                });

            }
            // 等待所有任务完成
            Task.WhenAll(tasks).Wait();

            
        }

        [Test]
        public void Order5_DropTable()
        {
            string sql = "Drop Table test";
            int result = DbUtils.ExecuteNonQuery("postgres_test", sql);
            Assert.IsTrue(result == 0);
        }
    }
}
